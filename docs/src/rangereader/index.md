# Range Reader

Range Reader is the foundational I/O layer for the Tileverse ecosystem. It provides a unified API for efficient, random-access byte reading across local files, HTTP endpoints, and cloud storage services.

## Core Concepts

The library is built around the `RangeReader` interface, which abstracts the underlying storage mechanism. This allows upper-level applications (like PMTiles readers) to be agnostic about where the data resides.

### Supported Backends

| Backend | Class | Description |
| :--- | :--- | :--- |
| **Local File** | `FileRangeReader` | Uses `java.nio.channels.FileChannel` for efficient local reads. |
| **HTTP/HTTPS** | `HttpRangeReader` | Uses `java.net.http.HttpClient` with `Range` headers. |
| **AWS S3** | `S3RangeReader` | Native AWS SDK integration. |
| **Azure Blob** | `AzureRangeReader` | Native Azure SDK integration. |
| **Google Cloud** | `GcsRangeReader` | Native Google Cloud Storage integration. |

## Performance Features

- **Smart Caching**: Decorate any reader with `CachingRangeReader` to cache frequently accessed headers or index sections in memory or on disk.
- **Block Alignment**: Optimize read requests to align with cloud storage pricing models (e.g., reading full 4KB or 16KB blocks to minimize GET requests).
- **Coalescing**: Automatically merges adjacent read requests to reduce network overhead.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.rangereader</groupId>
    <artifactId>tileverse-rangereader-all</artifactId>
</dependency>
```

## Basic Usage

```java
// 1. Create a basic reader (e.g., S3)
var s3Reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/key"))
    .build();

// 2. Wrap with performance optimizations
var reader = CachingRangeReader.builder(s3Reader)
    .capacity(1024 * 1024 * 10) // 10 MB cache
    .build();

// 3. Read arbitrary byte ranges
ByteBuffer header = reader.readRange(0, 127);
header.flip();
ByteBuffer slice = reader.readRange(5000, 1000); // Read 1000 bytes at offset 5000
slice.flip();
```