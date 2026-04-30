# Range Reader

!!! info "Looking for the broader I/O API?"
    `RangeReader` is the byte-range API of the `tileverse-storage` library, used for single-file reads (PMTiles, COG, single-file Parquet). For listing, writing, deleting, copying, and presigning across the same backends, see the [**Storage**](../index.md) API. Both ship in `tileverse-storage-all` and interoperate via `Storage.openRangeReader(key)`.

`RangeReader` is the byte-range I/O API exposed by `tileverse-storage`. It provides a unified API for efficient, random-access byte reading across local files, HTTP endpoints, and cloud storage services.

## Core Concepts

The API is built around the `RangeReader` interface, which abstracts the underlying storage mechanism. This allows upper-level applications (like PMTiles readers) to be agnostic about where the data resides.

### Supported Backends

| Backend | Class | Description |
| :--- | :--- | :--- |
| **Local File** | `io.tileverse.storage.file.FileRangeReader` | Uses `java.nio.channels.FileChannel` for efficient local reads. |
| **HTTP/HTTPS** | `io.tileverse.storage.http.HttpRangeReader` | Uses `java.net.http.HttpClient` with `Range` headers. |
| **AWS S3** | `io.tileverse.storage.s3.S3RangeReader` | Native AWS SDK integration (general-purpose buckets and S3 Express One Zone). |
| **Azure Blob** | `io.tileverse.storage.azure.AzureBlobRangeReader` | Native Azure SDK integration (Blob Storage and Data Lake Gen2). |
| **Google Cloud** | `io.tileverse.storage.gcs.GoogleCloudStorageRangeReader` | Native Google Cloud Storage integration (flat and HNS buckets). |

## Performance Features

- **Smart Caching**: Decorate any reader with `CachingRangeReader` to cache frequently accessed headers or index sections in memory or on disk.
- **Block Alignment**: Optimize read requests to align with cloud storage pricing models (e.g., reading full 4KB or 16KB blocks to minimize GET requests).
- **Coalescing**: Automatically merges adjacent read requests to reduce network overhead.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.storage</groupId>
    <artifactId>tileverse-storage-all</artifactId>
</dependency>
```

## Basic Usage

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.cache.CachingRangeReader;
import io.tileverse.storage.s3.S3RangeReader;
import java.net.URI;
import java.nio.ByteBuffer;

// 1. Create a basic reader (e.g., S3)
RangeReader s3Reader = S3RangeReader.builder()
    .uri(URI.create("s3://bucket/key"))
    .build();

// 2. Wrap with performance optimizations
RangeReader reader = CachingRangeReader.builder(s3Reader)
    .capacity(1024 * 1024 * 10) // 10 MB cache
    .build();

// 3. Read arbitrary byte ranges
ByteBuffer header = reader.readRange(0, 127);
header.flip();
ByteBuffer slice = reader.readRange(5000, 1000); // Read 1000 bytes at offset 5000
slice.flip();
```