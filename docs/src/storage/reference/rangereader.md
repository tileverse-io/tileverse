# Range Reader

!!! info "Looking for the broader I/O API?"
    `RangeReader` is the byte-range API of the `tileverse-storage` library, used for single-file reads (PMTiles, COG, single-file Parquet). For listing, writing, deleting, copying, and presigning across the same backends, see the [**Storage**](../index.md) API. Both ship in `tileverse-storage-all` and interoperate via `Storage.openRangeReader(key)`.

`RangeReader` is the byte-range I/O API exposed by `tileverse-storage`. It provides a unified API for efficient, random-access byte reading across local files, HTTP endpoints, and cloud storage services.

## Core Concepts

The API is built around the `RangeReader` interface, which abstracts the underlying storage mechanism. This allows upper-level applications (like PMTiles readers) to be agnostic about where the data resides.

### Supported Backends

`RangeReader` instances are produced by `Storage.openRangeReader(key)` /
`Storage.openRangeReader(URI)`. Each backend ships its own `Storage`
implementation; the per-backend `RangeReader` classes are
package-private internals selected based on the URI scheme.

| Backend | Provider | Description |
| :--- | :--- | :--- |
| **Local File** | `FileStorageProvider` | Uses `java.nio.channels.FileChannel` for efficient local reads. |
| **HTTP/HTTPS** | `HttpStorageProvider` | Uses `java.net.http.HttpClient` with `Range` headers. |
| **AWS S3** | `S3StorageProvider` | Native AWS SDK integration (general-purpose buckets and S3 Express One Zone). |
| **Azure Blob** | `AzureBlobStorageProvider` | Native Azure SDK integration (Blob Storage and Data Lake Gen2). |
| **Google Cloud** | `GoogleCloudStorageProvider` | Native Google Cloud Storage integration (flat and HNS buckets). |

## Performance Features

- **Exact-Range Caching**: Decorate any reader with `CachingRangeReader` to cache exactly the byte ranges it is asked for, in memory.
- **Region-Scoped Block Alignment**: Stack `BlockAlignedRangeReader` above the cache and declare the byte regions (a header, an index, the whole file) that should be fetched and cached as whole blocks, aligning with cloud storage pricing models.
- **Batch Reads**: `RangeReader.readRanges(List<RangeRequest>)` reads several ranges in one call; the cache forwards only its misses, and the aligner quantizes in-region entries to deduplicated blocks. Backends then merge nearby ranges into shared fetches and run them in parallel: S3 on the CRT async client, GCS and Azure on a shared executor, HTTP as one `multipart/byteranges` request with a per-fetch fallback. See [Performance Optimization](../explanation/performance.md) for the merge model and the `io.tileverse.storage.batch.*` tuning properties.

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
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;
import java.net.URI;
import java.nio.ByteBuffer;

// 1. Open a Storage at the bucket / parent URI, get a RangeReader for the leaf
URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/key");
try (Storage storage = StorageFactory.open(bucket);
        RangeReader baseReader = storage.openRangeReader(leaf);
        // 2. Wrap with performance optimizations
        RangeReader reader = CachingRangeReader.builder(baseReader)
            .maxSizeBytes(10L * 1024 * 1024) // 10 MB total weight
            .build()) {

    // 3. Read arbitrary byte ranges
    ByteBuffer header = reader.readRange(0, 127);
    header.flip();
    ByteBuffer slice = reader.readRange(5000, 1000); // Read 1000 bytes at offset 5000
    slice.flip();
}
```