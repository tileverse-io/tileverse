# Performance

Performance optimization techniques and guidelines for the Tileverse Range Reader library.

## Overview

The Tileverse Range Reader is designed for high-performance cloud-native data access. This section covers optimization strategies, measurement techniques, and best practices for achieving optimal performance in production environments.

## Performance Fundamentals

### Cloud Storage Performance Characteristics

Different storage backends have distinct performance profiles:

| Storage Type | Latency | Throughput | Best Block Size | Optimization Strategy |
|--------------|---------|------------|-----------------|----------------------|
| **Local Files** | ~1ms | Very High | N/A | OS page cache, direct file access |
| **HTTP/HTTPS** | 50-500ms | Medium-High | 256KB-1MB | Connection pooling, compression |
| **Amazon S3** | 100-200ms | High | 1MB-8MB | Request consolidation, multipart |
| **Azure Blob** | 80-150ms | High | 1MB-4MB | Block blob optimization |
| **Google Cloud** | 50-150ms | High | 1MB-8MB | Regional proximity |

### The Performance Stack

Performance optimization involves multiple layers:

```
Application Layer
    ↓
Memory Cache (CachingRangeReader)
    ↓
Disk Cache (DiskCachingRangeReader)
    ↓
Network/Storage Layer (S3, Azure, HTTP, File)
```

## Optimization Strategies

### 1. Multi-Level Caching

Configure caching layers for maximum efficiency:

#### Memory Caching Configuration

```java
// High-performance memory cache
var reader = CachingRangeReader.builder(baseReader)
    .maximumSize(2000)                    // Number of cached ranges
    .maxSizeBytes(512 * 1024 * 1024)      // 512MB memory limit
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .recordStats()                        // Enable performance monitoring
    .build();

// Memory-constrained environments
var reader = CachingRangeReader.builder(baseReader)
    .maximumSize(500)
    .softValues()                         // Allow GC to reclaim memory
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build();
```

#### Disk Caching Configuration

```java
// Large dataset disk caching
var reader = DiskCachingRangeReader.builder(baseReader)
    .cacheDirectory("/fast-ssd/cache")    // Use fast storage
    .maxCacheSizeBytes(10L * 1024 * 1024 * 1024)  // 10GB cache
    .compressionEnabled(true)             // Compress cached data
    .recordStats()
    .build();

// Temporary processing
var reader = DiskCachingRangeReader.builder(baseReader)
    .maxCacheSizeBytes(1024 * 1024 * 1024)  // 1GB cache
    .deleteOnClose()                      // Clean up automatically
    .build();
```

### 2. Intelligent Caching Strategies

#### Memory Caching for Frequent Access

Optimize for frequently accessed data:

```java
// Cloud storage with memory caching
var reader = CachingRangeReader.builder(cloudReader)
    .maximumSize(2000)                   // Cache frequently accessed ranges
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .recordStats()                       // Monitor cache performance
    .build();

// HTTP with memory caching for repeated requests
var reader = CachingRangeReader.builder(httpReader)
    .maximumSize(1000)                   // Moderate cache size
    .maxSizeBytes(256 * 1024 * 1024)     // 256MB memory limit
    .build();
```

#### Multi-Level Caching Strategy

Combine memory and disk caching for optimal performance:

```java
// Optimal decorator stacking for cloud storage
var reader = 
    // Memory cache for immediate access
    CachingRangeReader.builder(
        // Disk cache for persistence and large datasets
        DiskCachingRangeReader.builder(s3Reader)
            .maxCacheSizeBytes(5L * 1024 * 1024 * 1024)  // 5GB disk
            .build())
        .maximumSize(1000)                              // 1000 entries in memory
        .build();
```

### 3. Connection and Request Optimization

#### HTTP Client Configuration

```java
var reader = HttpRangeReader.builder()
    .uri(uri)
    .connectTimeout(Duration.ofSeconds(10))
    .readTimeout(Duration.ofMinutes(5))
    .maxConnections(50)                   // Connection pool size
    .keepAlive(Duration.ofMinutes(5))     // Connection reuse
    .compressionEnabled(true)             // Enable gzip compression
    .build();
```

#### AWS S3 Client Optimization

The S3 module automatically uses the Apache HttpClient, removing the need to manage Netty dependencies. You can still customize the S3 client for performance tuning:

```java
var s3Client = S3Client.builder()
    .region(Region.US_WEST_2)
    .httpClient(ApacheHttpClient.builder()
        .maxConnections(100)
        .socketTimeout(Duration.ofSeconds(60))
        .build())
    .overrideConfiguration(ClientOverrideConfiguration.builder()
        .apiCallTimeout(Duration.ofMinutes(2))
        .retryPolicy(RetryPolicy.builder().numRetries(3).build())
        .build())
    .build();

var reader = S3RangeReader.builder()
    .client(s3Client)
    .bucket(bucket)
    .key(key)
    .build();
```

## Benchmarking and Measurement

### JMH Benchmarks

The project includes comprehensive JMH benchmarks:

```bash
# Build benchmarks
./mvnw package -pl benchmarks

# Run all benchmarks
java -jar benchmarks/target/benchmarks.jar

# Run specific benchmark categories
java -jar benchmarks/target/benchmarks.jar S3RangeReader
java -jar benchmarks/target/benchmarks.jar HttpRangeReader
java -jar benchmarks/target/benchmarks.jar CachingRangeReader

# Run with profiling
java -jar benchmarks/target/benchmarks.jar -prof gc         # GC profiling
java -jar benchmarks/target/benchmarks.jar -prof stack     # Stack profiling
java -jar benchmarks/target/benchmarks.jar -prof async     # Async profiler

# Custom benchmark parameters
java -jar benchmarks/target/benchmarks.jar \
    -p blockSize=1048576,4194304,8388608 \
    -p cacheSize=100,1000,10000 \
    -f 3 -wi 5 -i 10
```

### Performance Monitoring

#### Cache Statistics

```java
public void monitorCachePerformance(RangeReader reader) {
    if (reader instanceof CachingRangeReader cachingReader) {
        CacheStats stats = cachingReader.getCacheStats();
        
        System.out.printf("Cache Hit Rate: %.2f%%\n", stats.hitRate() * 100);
        System.out.printf("Request Count: %d\n", stats.requestCount());
        System.out.printf("Average Load Time: %.2fms\n", 
            stats.averageLoadPenalty() / 1_000_000.0);
        System.out.printf("Eviction Count: %d\n", stats.evictionCount());
        
        // Alert on poor performance
        if (stats.hitRate() < 0.8 && stats.requestCount() > 100) {
            System.err.println("WARNING: Cache hit rate below 80%");
        }
    }
}
```

#### Throughput Measurement

```java
public void measureThroughput(RangeReader reader, int iterations) throws IOException {
    int blockSize = 1024 * 1024; // 1MB blocks
    long totalBytes = 0;
    long startTime = System.nanoTime();
    
    for (int i = 0; i < iterations; i++) {
        ByteBuffer data = reader.readRange(i * blockSize, blockSize);
        totalBytes += data.remaining();
    }
    
    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
    double throughputMBps = (totalBytes / (1024.0 * 1024.0)) / durationSeconds;
    
    System.out.printf("Throughput: %.2f MB/s\n", throughputMBps);
    System.out.printf("Total: %d bytes in %.2f seconds\n", totalBytes, durationSeconds);
}
```

### Latency Analysis

```java
public void analyzeLatency(RangeReader reader, int samples) throws IOException {
    List<Long> latencies = new ArrayList<>();
    int blockSize = 64 * 1024; // 64KB blocks
    
    // Warm up
    for (int i = 0; i < 10; i++) {
        reader.readRange(i * blockSize, blockSize);
    }
    
    // Measure latencies
    for (int i = 0; i < samples; i++) {
        long startTime = System.nanoTime();
        reader.readRange(i * blockSize, blockSize);
        long endTime = System.nanoTime();
        
        latencies.add(endTime - startTime);
    }
    
    // Calculate statistics
    latencies.sort(Long::compareTo);
    long p50 = latencies.get(samples / 2);
    long p95 = latencies.get((int) (samples * 0.95));
    long p99 = latencies.get((int) (samples * 0.99));
    
    System.out.printf("Latency P50: %.2fms\n", p50 / 1_000_000.0);
    System.out.printf("Latency P95: %.2fms\n", p95 / 1_000_000.0);
    System.out.printf("Latency P99: %.2fms\n", p99 / 1_000_000.0);
}
```

## Cloud-Specific Optimizations

### Amazon S3 Optimization

```java
// Regional optimization
var reader = S3RangeReader.builder()
    .uri(s3Uri)
    .region(Region.US_WEST_2)              // Same region as application
    .acceleratedEndpoint(true)             // S3 Transfer Acceleration
    .pathStyleAccess(false)                // Virtual-hosted style
    .build();

// Large object optimization with caching
var reader = CachingRangeReader.builder(s3Reader)
    .maximumSize(500)                     // Cache for large objects
    .maxSizeBytes(512 * 1024 * 1024)      // 512MB for large chunks
    .build();
```

### Azure Blob Storage Optimization

```java
// Premium storage optimization
var reader = AzureBlobRangeReader.builder()
    .uri(azureUri)
    .sasToken(sasToken)
    .retryOptions(new RetryOptions()
        .setMaxRetryDelayInMs(1000)
        .setMaxTries(3)
        .setRetryDelayInMs(100))
    .build();

// Hot tier optimization with caching
var reader = CachingRangeReader.builder(azureReader)
    .maximumSize(1000)                    // Cache for hot tier access
    .expireAfterAccess(1, TimeUnit.HOURS) // Longer retention for hot data
    .build();
```

### HTTP/HTTPS Optimization

```java
// CDN optimization
var reader = HttpRangeReader.builder()
    .uri(cdnUri)
    .withBearerToken(token)
    .compressionEnabled(true)             // Enable compression
    .maxConnections(20)                   // Limit connections to CDN
    .keepAlive(Duration.ofMinutes(10))    // Long keepalive for CDN
    .build();
```

## Performance Troubleshooting

### Common Performance Issues

#### Low Cache Hit Rate

**Symptoms**: High latency, many network requests
**Causes**: Poor access patterns, cache too small, incorrect block alignment
**Solutions**:

```java
// Increase cache size
var reader = CachingRangeReader.builder(baseReader)
    .maximumSize(5000)                    // Increase from default
    .maxSizeBytes(1024 * 1024 * 1024)     // 1GB cache
    .build();

// Optimize caching strategy
var reader = CachingRangeReader.builder(baseReader)
    .maximumSize(2000)                    // Increase cache size
    .maxSizeBytes(512 * 1024 * 1024)      // 512MB memory limit
    .recordStats()                        // Monitor performance
    .build();
```

#### High Memory Usage

**Symptoms**: OutOfMemoryError, GC pressure
**Causes**: Large cache, large block sizes, memory leaks
**Solutions**:

```java
// Use soft references
var reader = CachingRangeReader.builder(baseReader)
    .softValues()                         // Allow GC to reclaim
    .maximumSize(1000)                    // Smaller cache
    .build();

// Use disk caching instead
var reader = DiskCachingRangeReader.builder(baseReader)
    .maxCacheSizeBytes(2L * 1024 * 1024 * 1024)  // 2GB on disk
    .build();
```

#### Slow Network Performance

**Symptoms**: High latency, low throughput, timeouts
**Causes**: Small block sizes, too many requests, network congestion
**Solutions**:

```java
// Use disk caching for better throughput
var reader = DiskCachingRangeReader.builder(cloudReader)
    .maxCacheSizeBytes(2L * 1024 * 1024 * 1024)  // 2GB disk cache
    .build();

// Increase timeouts
var reader = HttpRangeReader.builder()
    .uri(uri)
    .connectTimeout(Duration.ofSeconds(30))
    .readTimeout(Duration.ofMinutes(10))
    .maxRetries(5)
    .build();
```

## Performance Testing Guidelines

### Load Testing

```java
@Test
void loadTest() throws Exception {
    int threadCount = 10;
    int iterationsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicLong totalBytes = new AtomicLong();
    
    long startTime = System.nanoTime();
    
    for (int i = 0; i < threadCount; i++) {
        executor.submit(() -> {
            try {
                for (int j = 0; j < iterationsPerThread; j++) {
                    ByteBuffer data = reader.readRange(j * 1024, 1024);
                    totalBytes.addAndGet(data.remaining());
                }
            } catch (IOException e) {
                fail("Read failed", e);
            } finally {
                latch.countDown();
            }
        });
    }
    
    assertTrue(latch.await(30, TimeUnit.SECONDS), "Load test timed out");
    
    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
    double throughputMBps = (totalBytes.get() / (1024.0 * 1024.0)) / durationSeconds;
    
    System.out.printf("Concurrent throughput: %.2f MB/s\n", throughputMBps);
    assertTrue(throughputMBps > 10.0, "Throughput below minimum threshold");
}
```

### Memory Leak Detection

```java
@Test
void memoryLeakTest() throws Exception {
    Runtime runtime = Runtime.getRuntime();
    System.gc();
    long initialMemory = runtime.totalMemory() - runtime.freeMemory();
    
    // Perform many operations
    for (int i = 0; i < 10000; i++) {
        try (RangeReader reader = createReader()) {
            reader.readRange(0, 1024);
        }
    }
    
    System.gc();
    long finalMemory = runtime.totalMemory() - runtime.freeMemory();
    long memoryIncrease = finalMemory - initialMemory;
    
    System.out.printf("Memory increase: %d bytes\n", memoryIncrease);
    assertTrue(memoryIncrease < 10 * 1024 * 1024, 
        "Memory leak detected: " + memoryIncrease + " bytes");
}
```

## Best Practices Summary

### Configuration Guidelines

1. **Read Strategies**:
   - Local files: Direct access (OS caching is optimal)
   - HTTP: Chunked reads with caching
   - Cloud storage: Large reads with multi-level caching

2. **Cache Sizing**:
   - Memory cache: 10-20% of available heap
   - Disk cache: Based on available storage
   - Consider data access patterns

3. **Connection Management**:
   - Reuse connections where possible
   - Configure appropriate timeouts
   - Limit concurrent connections

### Monitoring Recommendations

1. **Key Metrics**:
   - Cache hit rate (target: >80%)
   - Average latency (target: <200ms for cloud)
   - Throughput (baseline and trends)
   - Memory usage (heap and off-heap)

2. **Alerting Thresholds**:
   - Cache hit rate < 70%
   - P95 latency > 1000ms
   - Memory usage > 80% of heap
   - Error rate > 1%

### Production Deployment

1. **JVM Tuning**:
   ```bash
   -Xmx4g                                 # Adequate heap size
   -XX:+UseG1GC                          # Low-latency GC
   -XX:MaxGCPauseMillis=100              # GC pause target
   -XX:+UnlockExperimentalVMOptions
   -XX:+UseCGroupMemoryLimitForHeap      # Container awareness
   ```

2. **Application Configuration**:
   ```java
   // Production-ready configuration
   var reader = CachingRangeReader.builder(
       DiskCachingRangeReader.builder(cloudReader)
           .maxCacheSizeBytes(5L * 1024 * 1024 * 1024)
           .build())
       .maximumSize(2000)
       .recordStats()
       .build();
   ```

This performance guide provides the foundation for optimizing range-based I/O operations across different storage backends and usage patterns.
EOF < /dev/null