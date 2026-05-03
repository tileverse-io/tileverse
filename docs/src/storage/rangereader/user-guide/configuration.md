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
        RangeReader cachedReader = CachingRangeReader.builder(s3Reader)
            // Strategy 1: Max entries (good for header/directory blocks)
            .maximumSize(1000)

            // Strategy 2: Max memory (e.g., 128MB)
            .maxSizeBytes(128L * 1024 * 1024)

            // Strategy 3: Expiration
            .expireAfterAccess(10, TimeUnit.MINUTES)

            .build()) {
    // ...
}
```

### Disk Cache (`DiskCachingRangeReader`)

Best for: **Large datasets**, **Repeated runs**, **Offline capability**.

```java
try (Storage storage = StorageFactory.open(bucket);
        RangeReader s3Reader = storage.openRangeReader(leaf);
        RangeReader diskCachedReader = DiskCachingRangeReader.builder(s3Reader)
            .cacheDirectory(Path.of("/mnt/fast-ssd/cache"))
            // Hard limit on disk usage (e.g., 10GB)
            .maxCacheSizeBytes(10L * 1024 * 1024 * 1024)
            .build()) {
    // ...
}
```

## Read Optimization

### Block Alignment

Cloud storage APIs (S3, GCS) often perform better (and cost less) when reading aligned blocks rather than many tiny, fragmented ranges.

```java
// Align reads to 64KB boundaries
RangeReader alignedReader = BlockAlignedRangeReader.builder(reader)
    .blockSize(64 * 1024) 
    .build();
```

**Impact:** If you request bytes `100-200`, the reader fetches `0-65536`. If you essentially request `200-300`, it's served from memory/disk cache immediately.

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

For full `HttpClient` customization (custom proxy, executor, SSL context, request timeout per call), build the `HttpClient` yourself and pass it through `HttpStorageProvider.open(URI, HttpClient[, HttpAuthentication])`. The returned `Storage` **borrows** the client; closing the `Storage` does NOT close it. The Properties path (`StorageFactory.open(uri, props)`) instead acquires a refcounted lease from `HttpClientCache`, so identical configs across multiple `Storage` instances share one underlying client.

### Global Properties

For environments where code changes are difficult, you can configure defaults via system properties:



| Property | Description | Default |
| :--- | :--- | :--- |
| `storage.http.timeout-millis` | Global HTTP timeout | 5000 |
| `storage.http.trust-all-certificates` | Disable SSL verification (Dev only) | false |

## Stack Recommendations

### For Tile Servers
A tile server needs low latency. Stack memory caching on top of disk caching.

```java
// 1. Base S3 Reader via Storage
URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/tiles.pmtiles");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader base = storage.openRangeReader(leaf);

        // 2. Disk Cache (Persistent L2)
        RangeReader l2 = DiskCachingRangeReader.builder(base)
            .cacheDirectory(cacheDir)
            .maxCacheSizeBytes(50_000_000_000L) // 50GB
            .build();

        // 3. Memory Cache (Fast L1)
        RangeReader reader = CachingRangeReader.builder(l2)
            .maximumSize(10_000) // Keep hot tiles in RAM
            .softValues()        // Let JVM reclaim memory if needed
            .build()) {
    // serve tiles from `reader`
}
```

### For Data Pipelines (ETL)
ETL jobs often read large chunks sequentially. Memory caching is less useful; focusing on throughput is key.

```java
try (Storage storage = StorageFactory.open(bucket);
        RangeReader base = storage.openRangeReader(leaf);
        RangeReader reader = DiskCachingRangeReader.builder(base)
            .maxCacheSizeBytes(1_000_000_000L) // 1GB buffer
            .deleteOnClose()                   // Clean up after job
            .build()) {
    // ...
}
```