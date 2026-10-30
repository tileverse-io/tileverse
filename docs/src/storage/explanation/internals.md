# How tileverse-storage works

This page explains the internal design of `tileverse-storage`, with a focus on the `RangeReader` path: how one API covers different backends, how reads are cached and batched, and where a new backend plugs in.

![System Context Diagram](../../assets/images/storage/structurizr-SystemContext.svg)

## Design decisions

1.  **Synchronous API**: We chose a blocking API over `CompletableFuture` or Reactor. This simplifies the implementation of complex logic (like caching and retries) and aligns with Java's `FileChannel` and standard `InputStream` patterns, as expected by most format parsers.
2.  **ByteBuffers**: All data transfer happens via `java.nio.ByteBuffer`. This allows for off-heap storage, direct memory mapping, and efficient I/O operations without unnecessary array copying.
3.  **Thread Safety**: All implementations must be thread-safe. State (like connection pools) is shared, but individual read operations are isolated.

```java
public interface RangeReader extends Closeable {
    // The fundamental operation; failures throw the unchecked StorageException
    int readRange(long offset, int length, ByteBuffer target);

    // Metadata
    OptionalLong size();
    String getSourceIdentifier();
}
```

## Implementation layers

![Core Module Components](../../assets/images/storage/structurizr-CoreComponents.svg)

`AbstractRangeReader` validates the arguments, manages the buffer positions, and leaves the fetch of the bytes to its subclasses, through `readRangeNoFlip`.

The backends do the network or disk I/O:

*   **`FileRangeReader`**: Wraps a `FileChannel` opened lazily by the file's real path and closed after the idle timeout with no read in progress. A stale NFS handle or a closed channel retires the channel, and the read resumes on a fresh one after the bytes already read, three attempts per call. Identifies the file by its real path. Uses the OS page cache.
*   **`HttpRangeReader`**: Uses `java.net.http.HttpClient` to issue `GET` requests with `Range` headers. The `HttpClient` is refcounted at the `HttpStorage` level (via `HttpClientCache`) and shared across sibling readers; per-reader `close()` is a no-op, as for S3, Azure and GCS. The client shuts down only when the last `HttpStorage` holding a lease closes.
*   **`S3RangeReader`**: Wraps the AWS SDK v2 and maps its exceptions to `StorageException`. Uses an `S3AsyncClient` refcounted by `S3ClientCache` at the `S3Storage` level.
*   **Azure and GCS**: Similar wrappers for their SDKs, with matching refcounted client caches.

The decorators add behavior without changing the backends. They stack as `BlockAlignedRangeReader` above `CachingRangeReader` above the backend:

*   **`CachingRangeReader`**: Intercepts `readRange` and `readRanges`, and looks up the exact `(offset, length)` requested in an in-memory Caffeine cache. On a miss, it calls the delegate, stores the result under that exact key, and returns the data. When another read of the same reader is already fetching the range, the read waits for that fetch instead of starting its own. The cache is shared across readers and partitioned by the `getSourceIdentifier()` of the delegate. For that reason, S3 and GCS readers over a non-default endpoint include the endpoint in their identifier: the same bucket and key on two endpoints must never share cached bytes. A local file identifies as its real path, and one file reached through a symlink or another spelling of its path shares one partition.
*   **`BlockAlignedRangeReader`**: Expands a request that falls fully inside a declared byte region (e.g., "bytes 100-150" inside a declared header region) to the blocks that cover it (e.g., "bytes 0-4095"), fetched from its delegate in one batch call. Requests outside the declared regions pass through untouched. Stacked above `CachingRangeReader`, the cache then stores the aligned blocks: block-grain caching inside declared regions, exact-grain caching elsewhere.

## Batched reads

`RangeReader.readRanges(List<RangeRequest>)` reads several ranges in one call. The cache forwards its misses as one call to its delegate, leaving out the ranges already being fetched by another read through the same reader. The aligner expands the requests inside its regions to blocks and removes the duplicates before it forwards them.

Each backend plans a batch before fetching. Nearby ranges merge into one fetch when the gap between them costs less than a second round trip: `maxGap = ttfb * bandwidth * (1/utilization - 1)`, the model behind Apache Arrow's [`CacheOptions`](https://github.com/apache/arrow/blob/main/cpp/src/arrow/io/caching.h) and [GDAL's multi-range merging](https://gdal.org/user/configoptions.html#GDAL_HTTP_MERGE_CONSECUTIVE_RANGES). That gives about 350 KB for object stores and 230 KB for plain HTTP. A merged fetch holds at most 32 MiB.

*   **S3**: each planned fetch is one async `GetObject`, at most `storage.batch.max-in-flight-fetches` of them at once (default 8). A completion admits the next fetch.
*   **GCS and Azure**: up to `storage.batch.max-in-flight-fetches` planned fetches run at once on a shared executor: virtual threads on Java 24+, a bounded daemon pool before. Java 21 to 23 keep a carrier thread busy while a virtual thread waits inside `synchronized` code, and batch fetches wait that way.
*   **HTTP**: the planned fetches travel as multi-range GETs (`Range: bytes=a-b,c-d,...`) of at most 100 non-overlapping fetches each, up to `storage.batch.max-in-flight-fetches` GETs at once. Each answer is parsed as it streams, and a single-range answer holding the whole group is streamed with gap skips. A fetch missing from an answer, or cut short, gets a GET of its own. The reader sends one GET per fetch when merging is disabled, and after a server answers a multi-range GET with a 200, or with a single range leaving out bytes requested inside the object.
*   **Local files**: exact reads in sequence. Merging buys nothing without round trips.

A merged fetch borrows heap scratch for its whole extent: the peak scratch of one batch is the in-flight bound times the max-fetch cap. A batch fetches the requested bytes plus the merged gaps, never more than the max-fetch cap per fetch, and reads the requests outside any merge exactly.

`readRanges` returns a `BatchReadResult` with the bytes read per request and what the call cost: the backend requests issued, the bytes transferred with the bridged gaps, and the bytes served from a cache. Compare `bytesTransferred()` with `bytesRequested()` to see what a gap setting buys on a real workload. [Configure for performance](../how-to/configure.md#batched-reads) lists the parameters.

## Clients and connections

A TLS handshake per request would slow reads down. The storages opened with the same configuration share one SDK client and its connection pool, and closing the last of them closes the client. Keep the `Storage` open, and open one `RangeReader` per file, not one per tile.

The S3 storages opened from a URI or a `StorageConfig` share one AWS CRT HTTP client, with no Netty on the classpath. It keeps one connection pool per host, sized from the memory limit seen by the JVM: 50 to 500 connections. Native memory does not grow with the number of endpoints. [System Properties](../how-to/configure.md#system-properties) lists the settings.

## Runtime view

### Basic File Range Reading
![Basic File Read](../../assets/images/storage/structurizr-BasicFileRead.svg)

### HTTP Range Reading with Authentication
![HTTP Range Read](../../assets/images/storage/structurizr-HttpRangeRead.svg)

### Cache Miss Scenario
![Cache Miss](../../assets/images/storage/structurizr-CacheMissScenario.svg)

### Block-Aligned Read inside a Declared Region
![Block-Aligned Read](../../assets/images/storage/structurizr-BlockAlignedRead.svg)

## Service Provider Interface (SPI)

`StorageFactory` finds the backends at runtime, for applications driven by configuration:

1.  **Discovery**: Uses `java.util.ServiceLoader` to find registered `StorageProvider` implementations.
2.  **Resolution**: `StorageFactory.open` and `findProvider(StorageConfig)` resolve the provider named by `storage.provider`. With none named, the providers answer `canProcess(StorageConfig)` from the URI scheme, with no request sent, and an `http(s)` URI opens the HTTP provider. Among several providers claiming one scheme the lowest `getOrder()` wins, and equal orders are an error.
3.  **Per-key reads**: `Storage.openRangeReader(String key)` on the returned `Storage` produces a `RangeReader` for a single object under the storage root.
4.  **Extensibility**: Users can write their own backend (e.g., `FtpStorageProvider`) and register it via `META-INF/services` without forking the codebase.

## Dependency structure

![Container Diagram](../../assets/images/storage/structurizr-Containers.svg)

The core module has no heavy dependency. An application without cloud backends gets no cloud SDK, and none of their conflicts:

*   `tileverse-storage-core`: Only depends on SLF4J, the JSpecify annotations and Caffeine.
*   `tileverse-storage-s3`: Pulls in the AWS SDK.
*   `tileverse-storage-azure`: Pulls in the Azure SDK.
*   `tileverse-storage-gcs`: Pulls in the Google Cloud Storage client.

Consumers pick the providers they need.
