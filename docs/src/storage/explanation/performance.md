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

*   **Solution**: Stack `BlockAlignedRangeReader` above a `CachingRangeReader` and declare the
    byte regions that should be aligned (a header, an index, the whole file). Requests inside a
    declared region "quantize" to whole blocks, cached by the layer beneath.
    *   *Scenario*: You read byte 10, then byte 20, both inside a declared region.
    *   *Without Alignment*: 2 network requests.
    *   *With Alignment (4KB)*: Request 1 fetches block `0-4096` through the cache. Request 2 is served from the memory cache.

### 4. Read Coalescing
*   **Concept**: `RangeReader.readRanges(List<RangeRequest>)` reads several ranges in one call:
    the cache forwards only its distinct misses as one delegate call, and the aligner quantizes
    in-region entries to deduplicated blocks before forwarding them.

*   **Implementation**: Each backend plans a batch before fetching. Nearby ranges merge into
    one fetch when the gap between them costs less than a second round trip
    (`maxGap = ttfb * bandwidth * (1/utilization - 1)`, the model behind Apache Arrow's
    [`CacheOptions`](https://github.com/apache/arrow/blob/main/cpp/src/arrow/io/caching.h)
    and [GDAL's multi-range merging](https://gdal.org/user/configoptions.html#GDAL_HTTP_MERGE_CONSECUTIVE_RANGES)):
    about 350 KB for object stores, 230 KB for plain HTTP, with merged fetches capped at
    32 MiB.
    *   *S3*: planned fetches run on the `S3AsyncClient`, one async `GetObject` each, at
        most `storage.batch.max-in-flight-fetches` of them at once (default 8), each completion
        admitting the next. Every S3 Storage of the process sends them through one shared CRT
        HTTP client: connections are pooled per host, and native memory stays independent of
        the number of endpoints.
    *   *GCS / Azure*: up to `storage.batch.max-in-flight-fetches` planned fetches run
        concurrently on a shared executor (virtual threads on Java 21+, a bounded daemon pool on
        Java 17).
    *   *HTTP*: a batch of several planned fetches travels as multi-range GETs
        (`Range: bytes=a-b,c-d,...`) of at most 100 non-overlapping fetches each, up to
        `storage.batch.max-in-flight-fetches` GETs at once, each answer parsed as it streams (a
        single-range answer holding the group is streamed with gap skips). A fetch missing from
        an answer or cut short gets a GET of its own. With merging disabled, or once a server
        answers a multi-range GET with a 200 or with a single range leaving out bytes requested
        inside the object, the reader sends one GET per fetch.
    *   *Local files*: sequential exact reads; merging buys nothing at zero round-trip cost.
*   **Tuning** (per `Storage`): `storage.batch.max-gap` (bytes; negative disables merging),
    `storage.batch.max-fetch` (bytes) and `storage.batch.max-in-flight-fetches` (0 removes the
    bound) are `StorageConfig` parameters of the object-store and HTTP providers, resolved once
    when the `Storage` is created and handed to every reader it opens. The
    `io.tileverse.storage.batch.executor` (`auto` | `virtual` | `pool`) and
    `io.tileverse.storage.batch.pool.size` system properties pick the shared executor.
*   **Memory**: a merged fetch borrows heap scratch for its whole extent, hence the peak scratch
    of one batch is the in-flight bound times the max-fetch cap.
*   **Amplification**: a batch fetches the requested bytes plus the merged gaps, never more
    than the max-fetch cap per fetch; requests outside any merge are read exactly.
*   **Measuring**: `readRanges` returns a `BatchReadResult` with the bytes read per request and
    what the call cost: backend requests issued, bytes transferred with the bridged gaps
    included, and bytes served from a cache. Compare `bytesTransferred()` with
    `bytesRequested()` to see what a gap setting buys on a real workload.

## Cloud Considerations

*   **AWS S3**: The `S3RangeReader` uses the AWS CRT HTTP client, with no Netty on the classpath. One HTTP client serves the S3 storages opened from a URI or a `StorageConfig`, with one connection pool per host sized from the memory limit seen by the JVM, or from four times its maximum heap when that is smaller: 50 to 500 connections.
*   **Latency**: S3 Time-to-First-Byte (TTFB) is typically 50-100ms. Caching is mandatory for interactive performance.
