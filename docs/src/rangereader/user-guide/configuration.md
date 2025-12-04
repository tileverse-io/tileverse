# Performance Configuration

This guide covers how to tune the `RangeReader` for different workloads, such as random access (tiles), sequential processing (ETL), or high-latency environments.

## Caching Layers

Caching is critical when reading from remote sources (S3, HTTP) to minimize latency and cost.

### Memory Cache (`CachingRangeReader`)

Best for: **Random access**, **Tile servers**, **Metadata headers**.

```java
var s3Reader = S3RangeReader.builder().uri(uri).build();

var cachedReader = CachingRangeReader.builder(s3Reader)
    // Strategy 1: Max entries (good for header/directory blocks)
    .maximumSize(1000)
    
    // Strategy 2: Max memory (e.g., 128MB)
    .maxSizeBytes(128L * 1024 * 1024)
    
    // Strategy 3: Expiration
    .expireAfterAccess(10, TimeUnit.MINUTES)
    
    .build();
```

### Disk Cache (`DiskCachingRangeReader`)

Best for: **Large datasets**, **Repeated runs**, **Offline capability**.

```java
var diskCachedReader = DiskCachingRangeReader.builder(s3Reader)
    .cacheDirectory(Path.of("/mnt/fast-ssd/cache"))
    // Hard limit on disk usage (e.g., 10GB)
    .maxCacheSizeBytes(10L * 1024 * 1024 * 1024)
    .build();
```

## Read Optimization

### Block Alignment

Cloud storage APIs (S3, GCS) often perform better (and cost less) when reading aligned blocks rather than many tiny, fragmented ranges.

```java
// Align reads to 64KB boundaries
var alignedReader = BlockAlignedRangeReader.builder(reader)
    .blockSize(64 * 1024) 
    .build();
```

**Impact:** If you request bytes `100-200`, the reader fetches `0-65536`. If you essentially request `200-300`, it's served from memory/disk cache immediately.

## Provider-Specific Tuning

### Amazon S3

Using the native AWS SDK client allows for deep configuration:

```java
var s3Client = S3Client.builder()
    .region(Region.US_WEST_2)
    .httpClient(ApacheHttpClient.builder()
        .maxConnections(50)
        .socketTimeout(Duration.ofSeconds(10))
        .build())
    .build();

var reader = S3RangeReader.builder()
    .client(s3Client)
    .bucket("maps")
    .key("planet.pmtiles")
    .build();
```

### HTTP / HTTPS

You can tune the underlying HTTP connection pool and timeouts:

```java
var reader = HttpRangeReader.builder()
    .uri(uri)
    .connectTimeout(Duration.ofSeconds(5))
    .readTimeout(Duration.ofSeconds(30))
    // Enable GZIP if the server supports range requests with compression
    .compressionEnabled(true) 
    .build();
```

### Global Properties

For environments where code changes are difficult, you can configure defaults via system properties:



| Property | Description | Default |
| :--- | :--- | :--- |
| `io.tileverse.rangereader.http.timeout-millis` | Global HTTP timeout | 5000 |
| `io.tileverse.rangereader.http.trust-all-certificates` | Disable SSL verification (Dev only) | false |

## Stack Recommendations

### For Tile Servers
A tile server needs low latency. Stack memory caching on top of disk caching.

```java
// 1. Base S3 Reader
var base = S3RangeReader.builder().uri(uri).build();

// 2. Disk Cache (Persistent L2)
var l2 = DiskCachingRangeReader.builder(base)
    .cacheDirectory(cacheDir)
    .maxCacheSizeBytes(50_000_000_000L) // 50GB
    .build();

// 3. Memory Cache (Fast L1)
var reader = CachingRangeReader.builder(l2)
    .maximumSize(10_000) // Keep hot tiles in RAM
    .softValues()        // Let JVM reclaim memory if needed
    .build();
```

### For Data Pipelines (ETL)
ETL jobs often read large chunks sequentially. Memory caching is less useful; focusing on throughput is key.

```java
var reader = DiskCachingRangeReader.builder(base)
    .maxCacheSizeBytes(1_000_000_000L) // 1GB buffer
    .deleteOnClose()                   // Clean up after job
    .build();
```