# Configure for performance

This guide covers how to tune the `RangeReader` for different workloads, such as random access (tiles), sequential processing (ETL), or high-latency environments.

## Caching Layers

Caching helps random access to remote sources, such as a tile server that reads the same headers and tiles again. A job that reads a file once, front to back, gains nothing from it: use the reader returned by `Storage.openRangeReader` directly.

### Memory Cache (`CachingRangeReader`)

```java
URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/key");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader s3Reader = storage.openRangeReader(leaf);
        RangeReader cachedReader = CachingRangeReader.of(s3Reader)) {
    // ...
}
```

The cache stores exactly the `(offset, length)` ranges requested from it. The in-memory caches of a `CacheManager` live in two pools, each with its own budget:

- The data pool holds data: the byte ranges cached by `CachingRangeReader` and the tiles decoded by the vector tile stores.
- The metadata pool holds the indexes for finding that data: PMTiles directories and VersaTiles tile indexes. Other libraries can add their own caches to it.

The caches in a pool share its budget, counted in bytes. When a pool is full, Caffeine evicts entries from any of its caches, keeping the ones used often and recently. Two settings set the size of the pools, and a third sets the largest read kept by the byte-range cache. Each setting is read from its system property, then from its environment variable, and otherwise takes its default:

| Property | Environment variable | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `io.tileverse.cache.data.max-size` | `IO_TILEVERSE_CACHE_DATA_MAXSIZE` | `20%` | Budget of the data pool |
| `io.tileverse.cache.metadata.max-size` | `IO_TILEVERSE_CACHE_METADATA_MAXSIZE` | `10%` | Budget of the metadata pool |
| `io.tileverse.cache.data.max-read-size` | `IO_TILEVERSE_CACHE_DATA_MAXREADSIZE` | `16M` | Largest read kept by the byte-range cache, as a byte count |

A size is either a percentage of the maximum heap, from `0%` to `100%`, or a byte count. A byte count is an integer with an optional unit `K`, `M` or `G`, in powers of 1024, optionally followed by `B` or `iB`: `512M`, `1GiB` and `4096` are byte counts. Units are case-insensitive. A budget of `0` or `0%` disables the pool. The two pool settings are read when the first pool is created, and `max-read-size` when the first `CachingRangeReader` is built without its own limit. An invalid value makes that call fail with an `IllegalArgumentException` that names the setting and the value.

Each cache drops an entry after it goes unused for the time below. A background scheduler removes expired entries without waiting for the next access.

| Cache | Contents | Expires after unused for |
| :--- | :--- | :--- |
| `tileverse-storage-cache` | Byte ranges | 5 minutes |
| `tileverse-vectortile-cache` | Decoded vector tiles | 10 seconds |
| `tileverse-pmtiles-directory-cache` | PMTiles directories | 10 minutes |
| `tileverse-versatiles-tile-index-cache` | VersaTiles tile indexes | 10 minutes |

The caches key entries by source and, when it is known, by the version of the object. The version is the object's ETag, or its version id if it has no ETag, or else its size and last-modified time. A reader knows the version when it is opened from a listing or stat entry with `Storage.openRangeReader(StorageEntry.File)`, or opened with `Storage.openVersionedRangeReader(String)`. Archives opened with `PMTilesReader.open` or `VersatilesReader.open` know it too. These last three make one metadata request per open to get it. When an object is replaced, a reader opened with the new version does not see the data cached for the old one. Readers opened by key alone have no version and share one set of entries per source. If the storage cannot answer the metadata request, for example an HTTP server that rejects `HEAD`, the reader or the archive opens without a version.

If the uncached ranges of a read add up to more than `io.tileverse.cache.data.max-read-size`, the reader reads them directly into the caller's buffers and does not cache them. Ranges of that read already in the cache are still served from it. `CachingRangeReader.Builder.maxReadSize(long)` sets the limit for one reader, and `getBypassStats()` counts these reads and their bytes across the readers of a cache manager.

`CacheManager.stats()` and `getCacheNames()` list the two pools as `tileverse-cache-data` and `tileverse-cache-metadata`. The `CaffeineCacheManager` returned by `CacheManager.getDefault()` and `CacheManager.newInstance()` also lists each cache of the table above by its name. An application with its own `CacheManager` sees the two pools in its cache registry.

`clearCache()` and closing the reader both remove the entries of the reader's source and version. `getCacheStats()` reports hits, misses, evictions and the entry count of the whole `tileverse-storage-cache`, not of one reader. `CachingRangeReader.builder(reader).cacheManager(CacheManager.newInstance())` gives a reader its own manager, with its own two pools of the same sizes. Each extra manager adds another data budget and another metadata budget.

Concurrent reads of one range through one `CachingRangeReader` fetch it once: the other reads wait for that fetch, and a read that waited 10 seconds fetches the range itself. Readers built separately over the same source and version share the cache, not the fetches in progress.

To cache every reader opened by a `Storage` without composing the decorator by hand, set `storage.caching.enabled=true` on the configuration passed to `StorageFactory.open`. It is off by default. The file backend ignores it.

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

For example, a request for bytes `100-200` inside a declared region fetches the whole `0-65535` block through the cache. A later request for `200-300` is served from the cached block.

## Provider-Specific Tuning

### Amazon S3

For deep AWS SDK customization (retry policy, credentials provider, execution interceptors), build the `S3AsyncClient` yourself and pass it through `S3StorageProvider.open(URI, S3AsyncClient)`. The returned `Storage` borrows the client; closing the `Storage` does NOT close the client. Presigned URLs need a `Storage` opened from a URI or a `StorageConfig`. A client built with `S3AsyncClient.crtBuilder()` fails on S3-compatible endpoints answering without an `ETag` header; a `Storage` opened from a URI reads them.

```java
S3AsyncClient s3Client = S3AsyncClient.builder()
    .region(Region.US_WEST_2)
    .build();

URI bucket = URI.create("s3://maps/");
URI leaf = URI.create("s3://maps/planet.pmtiles");
try (Storage storage = S3StorageProvider.open(bucket, s3Client);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}
```

#### Region

A `Storage` on AWS takes its region from the first of:

1. `storage.s3.region`;
2. the URI, when its host or bucket name holds a region;
3. the `aws.region` system property, then the `AWS_REGION` environment variable;
4. the AWS profile files: the profile of `storage.s3.default-credentials-profile`, else the profile named by `AWS_PROFILE` or `default`;
5. EC2 metadata, for a `Storage` on the default credentials chain;
6. `us-east-1`.

A request reaching the wrong region is sent again to the region of the bucket, at the cost of one failed request. Presigned URLs are not corrected: they need the right region.

#### Custom endpoint (MinIO and other S3-compatible services)

To target a non-AWS S3-compatible service (MinIO, Ceph RGW, Cloudflare R2, DigitalOcean Spaces, Wasabi, LocalStack) you can either keep the service URL (`http(s)://host/bucket/key`) and name the provider with `storage.provider=s3`, or keep a canonical `s3://bucket/key` URI and set the endpoint out of band with `storage.s3.endpoint`:

```java
Properties props = new Properties();
props.setProperty("storage.s3.endpoint", "http://localhost:9000"); // MinIO
props.setProperty("storage.s3.aws-access-key-id", "minioadmin");
props.setProperty("storage.s3.aws-secret-access-key", "minioadmin");
props.setProperty("storage.s3.region", "us-east-1"); // the region of the service; us-east-1 when left out

URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/planet.pmtiles");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}
```

The endpoint comes from the first of:

1. `storage.s3.endpoint`;
2. the URL of an `http(s)://host/bucket/key` URI opened with `storage.provider=s3`;
3. the `aws.endpointUrlS3` or `aws.endpointUrl` system property;
4. the `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` environment variable;
5. `endpoint_url` in the AWS profile files, for the profile of `storage.s3.default-credentials-profile`, else the profile named by `AWS_PROFILE` or `default`.

With none of them, the `Storage` uses the AWS endpoints of the region. How the endpoint interacts with the other S3 parameters:

- **`storage.s3.force-path-style`**: defaults to `true` whenever an endpoint override is in effect (from any of these sources), and `false` for canonical AWS URIs. Most S3-compatible services require path-style. Set `storage.s3.force-path-style` explicitly to override the default.
- **`storage.s3.region`**: with an endpoint override the region only signs the requests, and the service may reject a region other than its own (MinIO does). It falls back to `us-east-1` when neither the parameter nor the URI names one: the region of the environment is not read. For Cloudflare R2, use `storage.s3.region=auto`.
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

For full `HttpClient` customization (custom proxy, executor, SSL context), build the `HttpClient` yourself and pass it through `HttpStorageProvider.open(URI, HttpClient[, HttpAuthentication])`. The returned `Storage` **borrows** the client; closing the `Storage` does NOT close it. The Properties path (`StorageFactory.open(uri, props)`) instead shares one client among the `Storage` instances with the same connect timeout and trust-all setting.

### Local files

A `file:` `Storage` is rooted at an existing directory. Its readers open a file channel on the first read and close it after `storage.file.idle-timeout` (ISO-8601 duration, default `PT60S`; `PT0S` keeps the channels open; a value above zero must be at least 1 millisecond) with no read in progress, which keeps long-lived servers from holding stale NFS handles:

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
| `io.tileverse.storage.batch.executor` | Executor for batched fetches: `auto` (virtual threads on Java 24+, `pool` before), `virtual` or `pool` | `auto` |
| `io.tileverse.storage.batch.pool.size` | Size of the `pool` executor | the larger of 8 and the processor count |

The S3 connection pool and the cache pools take six settings, each read from its system property and then from its environment variable:

| Property | Environment variable | Description | Default |
| :--- | :--- | :--- | :--- |
| `io.tileverse.storage.s3-http-client.max-concurrency` | `IO_TILEVERSE_STORAGE_S3HTTPCLIENT_MAXCONCURRENCY` | Connections pooled per host, shared by single reads, batches and uploads | a tenth of the memory limit seen by the JVM, or of four times its maximum heap when that is smaller, divided by 7 MiB, between 50 and 500 |
| `io.tileverse.storage.s3-http-client.connection-timeout` | `IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONTIMEOUT` | Longest wait for a connection to open | 2 seconds |
| `io.tileverse.storage.s3-http-client.connection-acquisition-timeout` | `IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONACQUISITIONTIMEOUT` | Longest wait of a request for a pooled connection | 30 seconds |
| `io.tileverse.cache.data.max-size` | `IO_TILEVERSE_CACHE_DATA_MAXSIZE` | Budget of the data cache pool | `20%` |
| `io.tileverse.cache.metadata.max-size` | `IO_TILEVERSE_CACHE_METADATA_MAXSIZE` | Budget of the metadata cache pool | `10%` |
| `io.tileverse.cache.data.max-read-size` | `IO_TILEVERSE_CACHE_DATA_MAXREADSIZE` | Largest read kept by the byte-range cache, as a byte count | `16M` |

A duration reads as ISO-8601 (`PT30S`) or as a number and a unit (`30s`, `500ms`). [Caching Layers](#caching-layers) describes the cache pools and the format of a size.

A multipart upload keeps at most an eighth of the pool in flight, 6 connections of 50; the rest serves single reads, batches and other uploads.
