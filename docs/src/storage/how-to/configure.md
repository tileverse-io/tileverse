# Performance Configuration

This guide covers how to tune the `RangeReader` for different workloads, such as random access (tiles), sequential processing (ETL), or high-latency environments.

## Caching Layers

Caching is critical when reading from remote sources (S3, HTTP) to minimize latency and cost.

### Memory Cache (`CachingRangeReader`)

Best for: **Random access**, **Tile servers**, **Metadata headers**.

```java
URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/key");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader s3Reader = storage.openRangeReader(leaf);
        RangeReader cachedReader = CachingRangeReader.of(s3Reader)) {
    // ...
}
```

The cache stores exactly the `(offset, length)` ranges requested from it. One in-memory cache is shared by every `CachingRangeReader` of the JVM (one per `CacheManager`), partitioned by the source identifier of the wrapped reader, and sized as a whole rather than per reader:

| Setting | Value |
| :--- | :--- |
| Capacity | 20% of the maximum heap, weighed by cached bytes; over capacity, Caffeine evicts by its frequency and recency policy |
| Expiry | 60 seconds after the last access to an entry; a system scheduler removes expired entries promptly |

`clearCache()` drops the entries of one reader's source, and closing the reader does the same. `getCacheStats()` reports hits, misses, evictions and the entry count. `CachingRangeReader.builder(reader).cacheManager(CacheManager.newInstance())` gives a reader a cache of its own with the same capacity: every extra manager adds another 20% of the heap to the budget.

To cache every reader opened by a `Storage` without composing the decorator by hand, set `storage.caching.enabled=true` on the configuration passed to `StorageFactory.open`. It is off by default.

## Read Optimization

### Block Alignment

Cloud storage APIs (S3, GCS) often perform better (and cost less) when reading aligned blocks rather than many tiny, fragmented ranges. Block alignment is a decorator you stack above a `CachingRangeReader`, declaring which byte regions should be aligned:

```java
// Aligner outermost, cache beneath it: aligned blocks are what gets cached
CachingRangeReader cache = CachingRangeReader.builder(reader).build();
RangeReader alignedReader = BlockAlignedRangeReader.builder(cache)
    .blockSize(64 * 1024)
    .alignRegion(0, headerAndIndexLength) // or alignWholeFile()
    .build();
```

The aligner expands a request inside a declared region into whole blocks before the call reaches the cache, which then stores block-grain data for that region. Reads outside every declared region pass straight through to the cache, which stores them under their own exact keys.

**Impact:** If you request bytes `100-200` inside a declared region, the reader fetches the whole `0-65536` block through the cache. A later request for `200-300` inside the same block is served from the memory cache immediately.

## Provider-Specific Tuning

### Amazon S3

For deep AWS SDK customization (custom HTTP client, retry policy, credentials provider), build the `S3Client` yourself and pass it through `S3StorageProvider.open(URI, S3Client)`. The returned `Storage` borrows the client; closing the `Storage` does NOT close the client.

```java
S3Client s3Client = S3Client.builder()
    .region(Region.US_WEST_2)
    .httpClient(ApacheHttpClient.builder()
        .maxConnections(50)
        .socketTimeout(Duration.ofSeconds(10))
        .build())
    .build();

URI bucket = URI.create("s3://maps/");
URI leaf = URI.create("s3://maps/planet.pmtiles");
try (Storage storage = S3StorageProvider.open(bucket, s3Client);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}
```

#### Custom endpoint (MinIO and other S3-compatible services)

To target a non-AWS S3-compatible service (MinIO, Ceph RGW, Cloudflare R2, DigitalOcean Spaces, Wasabi, LocalStack) you can either encode the service host into an `http(s)://host/bucket/key` URI, or keep a canonical `s3://bucket/key` URI and set the endpoint out of band with `storage.s3.endpoint`:

```java
Properties props = new Properties();
props.setProperty("storage.s3.endpoint", "http://localhost:9000"); // MinIO
props.setProperty("storage.s3.aws-access-key-id", "minioadmin");
props.setProperty("storage.s3.aws-secret-access-key", "minioadmin");
props.setProperty("storage.s3.region", "us-east-1"); // any valid region; the SDK requires one

URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/planet.pmtiles");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}
```

How `storage.s3.endpoint` interacts with the other S3 parameters:

- **Precedence**: an explicit `storage.s3.endpoint` wins over any endpoint inferred from an `http(s)://host/bucket/key` URI. Leave it unset to use the default AWS endpoints for the region.
- **`storage.s3.force-path-style`**: defaults to `true` whenever an endpoint override is in effect (from either the parameter or the URI), and `false` for canonical AWS URIs. Most S3-compatible services require path-style. Set `storage.s3.force-path-style` explicitly to override the default.
- **`storage.s3.region`**: still required by the SDK even for services that ignore it (e.g. MinIO). It falls back to `us-east-1` when neither the parameter nor the URI specifies one. For Cloudflare R2, use `storage.s3.region=auto`.
- **Credentials** (`storage.s3.anonymous`, `storage.s3.aws-access-key-id` / `storage.s3.aws-secret-access-key`, `storage.s3.default-credentials-profile`, the default chain): fully orthogonal to the endpoint. The endpoint selects the service; credentials select the identity. Static access-key + secret is the usual pairing for self-hosted services.

### HTTP / HTTPS

The connect timeout and trust-all-certificates flag are configurable via `Properties`:

```java
Properties props = new Properties();
props.setProperty("storage.http.timeout-millis", "5000");
// dev/test only:
// props.setProperty("storage.http.trust-all-certificates", "true");

URI parent = URI.create("https://server.example/data/");
URI leaf = URI.create("https://server.example/data/file.bin");
try (Storage storage = StorageFactory.open(parent, props);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}
```

For full `HttpClient` customization (custom proxy, executor, SSL context, request timeout per call), build the `HttpClient` yourself and pass it through `HttpStorageProvider.open(URI, HttpClient[, HttpAuthentication])`. The returned `Storage` **borrows** the client; closing the `Storage` does NOT close it. The Properties path (`StorageFactory.open(uri, props)`) instead acquires a refcounted lease from `HttpClientCache`, which lets identical configs across multiple `Storage` instances share one underlying client.

### Local files

A `file:` `Storage` is rooted at an existing directory. Its readers open a file channel on the first read and close it after `storage.file.idle-timeout` (ISO-8601 duration, default `PT60S`; `PT0S` keeps the channel open; a value above zero must be at least 1 millisecond) with no read in progress, which keeps long-lived servers from holding stale NFS handles:

```java
Properties props = new Properties();
props.setProperty("storage.file.idle-timeout", "PT30S");

try (Storage storage = StorageFactory.open(URI.create("file:///data/tiles/"), props);
        RangeReader reader = storage.openRangeReader("world.pmtiles")) {
    // ...
}
```

A single file with no use for a `Storage` opens directly, with the 60-second default or a timeout of your own:

```java
try (RangeReader reader = FileStorageProvider.openRangeReader(Path.of("/data/tiles/world.pmtiles"))) {
    // ...
}
```

### Batched reads

The object-store and HTTP providers accept three parameters that tune how the readers of one `Storage` serve `readRanges`:

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `storage.batch.max-gap` | Largest gap, in bytes, between two ranges of a batch fetched along with them as one request; negative disables merging | derived from the backend's connection profile: about 350 KB for object stores, 230 KB for HTTP |
| `storage.batch.max-fetch` | Upper bound, in bytes, on one merged fetch | 32 MiB |
| `storage.batch.max-in-flight-fetches` | Fetches of one batch in flight at once; 0 removes the bound | 8 |

The gap is a property of the deployment: measure it per latency and bandwidth profile with the `BatchReadResult` every `readRanges` call returns. Two stores on different endpoints can be tuned apart, since each `Storage` resolves the three values once when it is created.

### System Properties

The executor behind batched fetches is shared by the whole JVM and picked once, at first use:

| Property | Description | Default |
| :--- | :--- | :--- |
| `io.tileverse.storage.batch.executor` | Executor for batched fetches: `auto`, `virtual` or `pool` | `auto` |
| `io.tileverse.storage.batch.pool.size` | Size of the `pool` executor | the larger of 8 and the processor count |

The connection pools of the S3 clients take three settings, each read from its system property first and from its environment variable second:

| Property | Environment variable | Description | Default |
| :--- | :--- | :--- | :--- |
| `io.tileverse.storage.s3-http-client.max-concurrency` | `IO_TILEVERSE_STORAGE_S3HTTPCLIENT_MAXCONCURRENCY` | Connections pooled per host | 50 |
| `io.tileverse.storage.s3-http-client.connection-timeout` | `IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONTIMEOUT` | Longest wait for a connection to open | 2 seconds |
| `io.tileverse.storage.s3-http-client.connection-acquisition-timeout` | `IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONACQUISITIONTIMEOUT` | Longest wait of a request for a pooled connection | 30 seconds |

A duration reads as ISO-8601 (`PT30S`) or as a number and a unit (`30s`, `500ms`).

## Stack Recommendations

### For Tile Servers
A tile server needs low latency. Keep hot tiles in a memory cache.

```java
// 1. Base S3 Reader via Storage
URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/tiles.pmtiles");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader base = storage.openRangeReader(leaf);

        // 2. Memory Cache: hot tiles stay in the shared cache for 60 seconds after their last read
        RangeReader reader = CachingRangeReader.of(base)) {
    // serve tiles from `reader`
}
```

### For Data Pipelines (ETL)
ETL jobs often read large chunks sequentially. Memory caching is less useful; read large ranges directly from the base reader and focus on throughput.

```java
try (Storage storage = StorageFactory.open(bucket);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // issue large sequential readRange calls
}
```