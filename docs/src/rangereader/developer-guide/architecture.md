# Architecture

This document explains the internal design of the Range Reader library, detailing how it achieves a unified API across diverse storage backends.

## Core Design

The library is built around a single, synchronous interface: `RangeReader`.

```java
public interface RangeReader extends Closeable {
    // The fundamental atomic operation
    int readRange(long offset, int length, ByteBuffer target) throws IOException;
    
    // Metadata
    long size() throws IOException;
    String getSourceIdentifier();
}
```

### Design Decisions

1.  **Synchronous API**: We chose a blocking API over `CompletableFuture` or Reactor. This simplifies the implementation of complex logic (like caching and retries) and aligns with Java's `FileChannel` and standard `InputStream` patterns, which are what most format parsers expect.
2.  **ByteBuffers**: All data transfer happens via `java.nio.ByteBuffer`. This allows for off-heap storage, direct memory mapping, and efficient I/O operations without unnecessary array copying.
3.  **Thread Safety**: All implementations must be thread-safe. State (like connection pools) is shared, but individual read operations are isolated.

## Implementation Hierarchy

### Base Layer: `AbstractRangeReader`
This abstract class handles the boilerplate:

*   Argument validation (bounds checks).
*   Buffer handling (position management, slicing).
*   Template pattern: delegates the actual byte fetching to `readRangeNoFlip`.

### Backend Layer
These classes implement the actual network/disk I/O:

*   **`FileRangeReader`**: Wraps `FileChannel`. Uses OS page cache.
*   **`HttpRangeReader`**: Uses `java.net.http.HttpClient` to issue `GET` requests with `Range` headers.
*   **`S3RangeReader`**: Wraps AWS SDK v2. Maps exceptions to standard `IOException`.
*   **`Azure` / `GCS`**: Similar wrappers for their respective SDKs.

### Decorator Layer
We use the Decorator pattern to add behaviors without modifying backends.

*   **`CachingRangeReader`**: Intercepts `readRange`. Checks in-memory Caffeine cache. If miss, calls delegate, caches result, returns data.
*   **`DiskCachingRangeReader`**: Similar to above, but persists to a local file store.
*   **`BlockAlignedRangeReader`**: Expands arbitrary read requests (e.g., "bytes 100-150") to align with specific block boundaries (e.g., "bytes 0-4096"), optimizing cache hit rates.

## Service Provider Interface (SPI)

To support dynamic loading (e.g., for configuration-driven applications), we expose a `RangeReaderFactory`.



1.  **Discovery**: Uses `java.util.ServiceLoader` to find registered `RangeReaderProvider` implementations.
2.  **Resolution**: `RangeReaderFactory.create(uri)` iterates providers. The first one returning `true` for `canHandle(uri)` is instantiated.
3.  **Extensibility**: Users can write their own backend (e.g., `FtpRangeReader`) and register it via `META-INF/services` without forking the codebase.

## Dependency Structure

To avoid "dependency hell" (e.g., conflicting Netty versions between Azure and AWS SDKs), the core module has zero heavy dependencies.



*   `tileverse-rangereader-core`: Lightweight. Only depends on SLF4J and Caffeine.
*   `tileverse-rangereader-s3`: Pulls in AWS SDK.
*   `tileverse-rangereader-azure`: Pulls in Azure SDK.

This allows consumers to pick exactly the providers they need.