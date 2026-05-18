# Performance Optimization

Understanding how `RangeReader` behaves under load is critical for high-throughput applications. This guide covers benchmarking and tuning.

## Benchmarking Methodology

We use **JMH (Java Microbenchmark Harness)** to measure performance. This ensures we avoid JVM warm-up pitfalls and get statistically significant results.

### Running Benchmarks

The `benchmarks` module contains pre-configured tests.

```bash
# Build the benchmarks jar
mvn clean package -pl benchmarks -am

# Run S3 benchmarks
java -jar benchmarks/target/benchmarks.jar S3RangeReaderBenchmark
```

### Key Metrics

We primarily measure:

1.  **Throughput (ops/sec)**: How many ranges can we read per second?
2.  **Latency (p99)**: What is the worst-case time for a single read?
3.  **Allocation Rate**: How much garbage are we generating per read?

## Tuning Strategies

### 1. Buffer Management
*   **Problem**: Allocating a new `byte[]` or `ByteBuffer` for every read puts pressure on the Garbage Collector (GC).

*   **Solution**: Use the `readRange(offset, length, targetBuffer)` overload. Reuse a thread-local or pooled `ByteBuffer`.

### 2. Connection Pooling
*   **Problem**: TLS handshakes are expensive. Creating a new S3/HTTP client for every request kills performance.

*   **Solution**: The library pools connections by default. Ensure you reuse the `RangeReader` instance. Do not create a new `RangeReader` for every tile; create it once per file/session.

### 3. Block Alignment
*   **Problem**: Cloud providers charge per request. Reading 10 bytes here and 10 bytes there generates many requests.

*   **Solution**: Use `BlockAlignedRangeReader`. It effectively "quantizes" reads.
    *   *Scenario*: You read byte 10, then byte 20.
    *   *Without Alignment*: 2 network requests.
    *   *With Alignment (4KB)*: Request 1 fetches 0-4096. Request 2 is served from cache.

### 4. Read Coalescing
*   **Concept**: If an application requests bytes `0-100` and `100-200` in rapid succession (or concurrently), the reader can merge these into a single `0-200` request.

*   **Implementation**: Currently handled via the `CachingRangeReader` and Block Alignment. Future versions may support explicit request coalescing for async patterns.

## Cloud Considerations

*   **AWS S3**: The `S3RangeReader` uses the Apache HTTP client backend instead of Netty to reduce classpath conflicts. We tune the connection pool size to match standard concurrency levels (default 50).
*   **Latency**: S3 Time-to-First-Byte (TTFB) is typically 50-100ms. Caching is mandatory for interactive performance.
