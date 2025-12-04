# Quick Start

Get started with the Tileverse Range Reader library in minutes with these basic examples.

## Basic Usage

### Reading from Local Files

```java
import io.tileverse.rangereader.FileRangeReader;
import java.nio.ByteBuffer;
import java.nio.file.Path;

try (var reader = FileRangeReader.builder()
        .path(Path.of("data.bin"))
        .build()) {
    
    // Read first 1024 bytes
    ByteBuffer header = reader.readRange(0, 1024);
    header.flip(); // Prepare buffer for reading
    
    // Read a specific section
    ByteBuffer chunk = reader.readRange(50000, 8192);
    chunk.flip(); // Prepare buffer for reading
    
    // Get total file size
    long size = reader.size();
    
    System.out.println("File size: " + size + " bytes");
}
```

### Reading from HTTP

```java
import io.tileverse.rangereader.HttpRangeReader;
import java.net.URI;

try (var reader = HttpRangeReader.builder()
        .uri(URI.create("https://example.com/data.bin"))
        .build()) {
    
    // Read range from remote file
    ByteBuffer data = reader.readRange(1000, 500);
    data.flip(); // Prepare buffer for reading
    
    System.out.println("Read " + data.remaining() + " bytes");
}
```

### Reading from Amazon S3

```java
import io.tileverse.rangereader.s3.S3RangeReader;
import software.amazon.awssdk.regions.Region;
import java.net.URI;

try (var reader = S3RangeReader.builder()
        .uri(URI.create("s3://my-bucket/data.bin"))
        .region(Region.US_WEST_2)
        .build()) {
    
    // Read from S3 object
    ByteBuffer data = reader.readRange(0, 1024);
    data.flip();
    
    System.out.println("Read from S3: " + data.remaining() + " bytes");
}
```

## Performance Optimization

### Adding Memory Caching

Memory caching is most beneficial for cloud storage where network latency is significant:

```java
import io.tileverse.rangereader.cache.CachingRangeReader;

// Use caching with cloud storage for maximum benefit
var baseReader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/large-file.bin"))
    .region(Region.US_WEST_2)
    .build();

try (var cachedReader = CachingRangeReader.builder(baseReader)
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
import io.tileverse.rangereader.cache.DiskCachingRangeReader;

var s3Reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/large-file.bin"))
    .build();

try (var cachedReader = DiskCachingRangeReader.builder(s3Reader)
        .maxCacheSizeBytes(1024 * 1024 * 1024)  // 1GB cache
        .build()) {
    
    // Reads are cached to disk for persistence across sessions
    ByteBuffer data = cachedReader.readRange(100, 500);
    data.flip();
}
```

### Multi-Level Caching

```java
// Optimal configuration for cloud storage
try (var optimizedReader = CachingRangeReader.builder(
        DiskCachingRangeReader.builder(
            S3RangeReader.builder()
                .uri(URI.create("s3://bucket/data.bin"))
                .build())
            .maxCacheSizeBytes(10L * 1024 * 1024 * 1024)  // 10GB disk cache
            .build())
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

try (var reader = FileRangeReader.builder()
        .path(Path.of("data.bin"))
        .build()) {
    
    // Validate before reading
    long fileSize = reader.size();
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
try (var reader = FileRangeReader.builder()
        .path(Path.of("image.tiff"))
        .build()) {
    
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
    try (var reader = FileRangeReader.builder()
            .path(filePath)
            .build()) {
        
        long fileSize = reader.size();
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

- **[Configuration](configuration.md)**: Learn about performance tuning
- **[Authentication](authentication.md)**: Set up cloud provider access
- **[Troubleshooting](troubleshooting.md)**: Common issues and solutions