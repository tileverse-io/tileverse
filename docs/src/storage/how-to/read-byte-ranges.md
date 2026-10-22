# Reading byte ranges

A `RangeReader` reads byte ranges of one object. Format readers use it to fetch only the parts of a file they need: the header and the index of a PMTiles archive, the tiles of a Cloud Optimized GeoTIFF, the row groups of a Parquet file.

## Open a reader

Open a `Storage` for the container, then ask it for a `RangeReader` per object:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.net.URI;
import java.nio.ByteBuffer;

try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/data/"));
        RangeReader reader = storage.openRangeReader("file.bin")) {

    long size = reader.size().orElseThrow();

    ByteBuffer header = reader.readRange(0, 1024);
    header.flip();
}
```

A `Storage` always points at a container: a directory, a bucket, or a prefix. For `file:` URIs the directory must exist. `openRangeReader` also accepts the full URI of the object.

The `Storage` holds the SDK client, and a reader is cheap. To read several objects of one container, keep the `Storage` open and open one reader per object. Closing a `RangeReader` releases only that reader.

A single local file opens without a `Storage`:

```java
import io.tileverse.storage.file.FileStorageProvider;
import java.nio.file.Path;

try (RangeReader reader = FileStorageProvider.openRangeReader(Path.of("/data/world.pmtiles"))) {
    // ...
}
```

## Read into a buffer

`readRange(offset, length)` allocates a new buffer for each call. The buffer is positioned after the bytes read: call `flip()` before reading from it. A range that extends past the end of the object returns the bytes available, and a range that starts at or past the end returns none.

To avoid an allocation per read, pass your own buffer. The call returns the number of bytes read:

```java
ByteBuffer buffer = ByteBuffer.allocate(8192);

for (long offset = 0; offset < size; offset += 8192) {
    buffer.clear();
    int bytesRead = reader.readRange(offset, 8192, buffer);
    buffer.flip();
    process(buffer);
}
```

Large reads can borrow a direct buffer from the shared pool:

```java
import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

try (PooledByteBuffer pooled = ByteBufferPool.directBuffer(1024 * 1024)) {
    ByteBuffer buffer = pooled.buffer();
    reader.readRange(0, 1024 * 1024, buffer);
    buffer.flip();
    process(buffer);
}
```

## Read several ranges in one call

`readRanges` reads several ranges, each into its own buffer. The object-store and HTTP backends merge nearby ranges into shared requests and run them in parallel:

```java
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import java.util.List;

ByteBuffer index = ByteBuffer.allocate(4096);
ByteBuffer tile = ByteBuffer.allocate(16384);

BatchReadResult result = reader.readRanges(List.of(
        RangeRequest.of(0, 4096, index),
        RangeRequest.of(500_000, 16384, tile)));

int tileBytes = result.bytesRead(1);
tile.flip();
```

The result also reports what the call cost: `fetches()` counts the backend requests, and `bytesTransferred()` the bytes received, including the gaps bridged by the merges. [Configure for performance](configure.md#batched-reads) lists the tuning parameters, and [How it works](../explanation/internals.md#batched-reads) describes the merge model.

## Cache and align reads

`CachingRangeReader` keeps the ranges read in memory. A repeated read of the same range is served from the cache. Caching helps remote sources; the operating system already caches local files.

`BlockAlignedRangeReader` expands reads inside the regions you declare to whole blocks. Stack it above the cache: the cache then holds whole blocks for those regions, and exact ranges elsewhere.

```java
import io.tileverse.storage.block.BlockAlignedRangeReader;
import io.tileverse.storage.cache.CachingRangeReader;

RangeReader cached = CachingRangeReader.of(reader);
RangeReader aligned = BlockAlignedRangeReader.builder(cached)
        .blockSize(64 * 1024)
        .alignRegion(0, headerAndIndexLength) // or alignWholeFile()
        .build();
```

The opposite order works, but the cache then stores the expanded blocks under the exact ranges requested, and overlapping reads never share a block. [Configure for performance](configure.md#caching-layers) describes the size and the expiry of the cache.

## Handle errors

A failed read throws the unchecked `StorageException`, or its subclass `NotFoundException` when the object is missing. `close()` declares `IOException`.

```java
import io.tileverse.storage.StorageException;
import java.io.IOException;

try (Storage storage = StorageFactory.open(URI.create("file:///data/"));
        RangeReader reader = storage.openRangeReader("data.bin")) {
    ByteBuffer data = reader.readRange(1000, 500);
} catch (StorageException e) {
    // The read failed; NotFoundException when the object is missing
} catch (IOException e) {
    // Closing failed
}
```

A negative offset or length throws `IllegalArgumentException`.

## The `RangeReader` interface

```java
public interface RangeReader extends Closeable {
    ByteBuffer readRange(long offset, int length);
    int readRange(long offset, int length, ByteBuffer target);
    BatchReadResult readRanges(List<RangeRequest> requests);
    OptionalLong size();
    String getSourceIdentifier();
}
```

Implementations are thread-safe. `get()` returns a `SeekableByteChannel` view of the reader, for libraries that read from channels. The `io.tileverse.storage.adapters` package adds an ImageIO `ImageInputStream` and `java.io.DataInput` views over channels.
