# Quick Start

Get started with the `RangeReader` API of `tileverse-storage` in minutes with these basic examples.

## Basic Usage

The 2.0 model is uniform across every backend: open a `Storage` for the
container (parent URI), then ask it for a `RangeReader` for each leaf
object you want to read.

### Reading from Local Files

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;

URI dir = Path.of("data").toUri();
URI leaf = Path.of("data/data.bin").toUri();
try (Storage storage = StorageFactory.open(dir);
        RangeReader reader = storage.openRangeReader(leaf)) {

    // Read first 1024 bytes
    ByteBuffer header = reader.readRange(0, 1024);
    header.flip(); // Prepare buffer for reading

    // Read a specific section
    ByteBuffer chunk = reader.readRange(50000, 8192);
    chunk.flip(); // Prepare buffer for reading

    // Get total file size
    long size = reader.size().orElseThrow();

    System.out.println("File size: " + size + " bytes");
}
```

A `Storage` always points at a directory / container / prefix — never a
single object. For `file:` URIs the directory must already exist; the
provider does not auto-create it. To address a single file, open the
parent and pass the leaf to `openRangeReader` as shown above.

### Reading from HTTP

```java
import java.util.Properties;

URI parent = URI.create("https://example.com/");
URI leaf = URI.create("https://example.com/data.bin");
try (Storage storage = StorageFactory.open(parent);
        RangeReader reader = storage.openRangeReader(leaf)) {

    // Read range from remote file
    ByteBuffer data = reader.readRange(1000, 500);
    data.flip(); // Prepare buffer for reading

    System.out.println("Read " + data.remaining() + " bytes");
}
```

### Reading from Amazon S3

```java
Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/data.bin");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader reader = storage.openRangeReader(leaf)) {

    // Read from S3 object
    ByteBuffer data = reader.readRange(0, 1024);
    data.flip();

    System.out.println("Read from S3: " + data.remaining() + " bytes");
}
```

When you read multiple objects from the same S3 bucket, hold the
`Storage` once and call `openRangeReader(key)` per object — that's what
the API is designed for, and the SDK client cost is amortized across all
the readers.

## Performance Optimization

### Adding Memory Caching

Memory caching is most beneficial for cloud storage where network latency is significant:

```java
import io.tileverse.storage.cache.CachingRangeReader;

Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/large-file.bin");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader baseReader = storage.openRangeReader(leaf);
        RangeReader cachedReader = CachingRangeReader.builder(baseReader)
            .maximumSize(1000)  // Cache up to 1000 ranges
            .build()) {

    // First read - network request to S3
    ByteBuffer data1 = cachedReader.readRange(0, 1024);
    data1.flip();

    // Second read - served from cache (much faster, no network)
    ByteBuffer data2 = cachedReader.readRange(0, 1024);
    data2.flip();
}
```

> **Note**: For local files, caching provides little benefit since the OS already caches file data efficiently.

### Disk Caching for Large Datasets

```java
import io.tileverse.storage.cache.DiskCachingRangeReader;

URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/large-file.bin");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader baseReader = storage.openRangeReader(leaf);
        RangeReader cachedReader = DiskCachingRangeReader.builder(baseReader)
            .maxCacheSizeBytes(1024 * 1024 * 1024)  // 1GB cache
            .build()) {

    // Reads are cached to disk for persistence across sessions
    ByteBuffer data = cachedReader.readRange(100, 500);
    data.flip();
}
```

### Multi-Level Caching

```java
URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/data.bin");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader baseReader = storage.openRangeReader(leaf);
        RangeReader diskCached = DiskCachingRangeReader.builder(baseReader)
            .maxCacheSizeBytes(10L * 1024 * 1024 * 1024)  // 10GB disk cache
            .build();
        RangeReader optimizedReader = CachingRangeReader.builder(diskCached)
            .maximumSize(1000)  // 1000 entries in memory
            .build()) {

    // Highly optimized reads with multiple caching layers
    ByteBuffer data = optimizedReader.readRange(offset, length);
    data.flip(); // Prepare buffer for reading
}
```

## Working with ByteBuffers

### Reusing Buffers (Recommended)

```java
// Efficient: Reuse the same buffer
ByteBuffer buffer = ByteBuffer.allocate(8192);

for (long offset = 0; offset < fileSize; offset += 8192) {
    buffer.clear();  // Reset for writing
    
    int bytesRead = reader.readRange(offset, 8192, buffer);
    buffer.flip(); // Prepare buffer for reading
    
    // Process buffer contents
    processData(buffer);
}
```

### Direct Buffers for Large Reads

```java
// For large reads, use direct buffers
ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024 * 1024);

try {
    int bytesRead = reader.readRange(0, 1024 * 1024, directBuffer);
    directBuffer.flip();
    
    // Process large chunk efficiently
    processLargeData(directBuffer);
} finally {
    // Clean up direct buffer if needed
    if (directBuffer.isDirect()) {
        ((DirectBuffer) directBuffer).cleaner().clean();
    }
}
```

## Error Handling

```java
import java.io.IOException;

URI dir = Path.of("data").toUri();
URI leaf = Path.of("data/data.bin").toUri();
try (Storage storage = StorageFactory.open(dir);
        RangeReader reader = storage.openRangeReader(leaf)) {
    
    // Validate before reading
    long fileSize = reader.size().orElseThrow();
    long offset = 1000;
    int length = 500;
    
    if (offset >= fileSize) {
        System.out.println("Offset beyond file end");
        return;
    }
    
    // Adjust length if it extends beyond EOF
    if (offset + length > fileSize) {
        length = (int) (fileSize - offset);
    }
    
    ByteBuffer data = reader.readRange(offset, length);
    data.flip(); // Prepare buffer for reading
    
} catch (IOException e) {
    System.err.println("Failed to read data: " + e.getMessage());
} catch (IllegalArgumentException e) {
    System.err.println("Invalid parameters: " + e.getMessage());
}
```

## Common Patterns

### Reading File Headers

```java
// Read different header formats
URI parent = Path.of(".").toUri();
URI tiff = Path.of("image.tiff").toUri();
try (Storage storage = StorageFactory.open(parent);
        RangeReader reader = storage.openRangeReader(tiff)) {

    // Read TIFF header
    ByteBuffer header = reader.readRange(0, 16);
    header.flip(); // Prepare buffer for reading

    // Check magic number
    short magic = header.getShort();

    if (magic == 0x4949 || magic == 0x4D4D) {
        System.out.println("Valid TIFF file");
    }
}
```

### Streaming Large Files

```java
// Process large files in chunks
public void processLargeFile(Path filePath, int chunkSize) throws IOException {
    URI parent = filePath.getParent().toUri();
    try (Storage storage = StorageFactory.open(parent);
            RangeReader reader = storage.openRangeReader(filePath.toUri())) {

        long fileSize = reader.size().orElseThrow();
        long processed = 0;
        
        while (processed < fileSize) {
            int currentChunkSize = (int) Math.min(chunkSize, fileSize - processed);
            
            ByteBuffer chunk = reader.readRange(processed, currentChunkSize);
            chunk.flip(); // Prepare buffer for reading
            
            // Process this chunk
            processChunk(chunk);
            
            processed += currentChunkSize;
            
            // Report progress
            double progress = (double) processed / fileSize * 100;
            System.out.printf("Progress: %.1f%%\n", progress);
        }
    }
}
```

## Next Steps

- **[Configuration](../../user-guide/configuration.md)**: Learn about performance tuning
- **[Authentication](../../user-guide/authentication.md)**: Set up cloud provider access
- **[Troubleshooting](../../user-guide/troubleshooting.md)**: Common issues and solutions