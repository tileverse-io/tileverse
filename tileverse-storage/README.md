# Tileverse Storage

A modern Java library providing an I/O abstraction over object storage (local files, HTTP, S3, Azure Blob, Google Cloud Storage): byte-range reads through the `RangeReader` API, and listing, reading, writing, deleting and copying objects through the `Storage` API.

> **Note**: This is part of the [Tileverse](../) project. See the [main README](../README.md) for project overview, requirements, and contributing guidelines.

## Why Tileverse Storage?

- **Universal access**: one API across files, HTTP, S3, Azure, GCS
- **High performance**: in-memory range caching, block alignment, cloud optimizations
- **Thread-safe**: designed for concurrent server environments
- **Composable**: decorator pattern for flexible feature combinations
- **Modular**: include only the providers you need

## Quick Example

Single-file byte-range read (PMTiles, COG, etc.):

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;

try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/"));
        RangeReader reader = CachingRangeReader.of(storage.openRangeReader("data.bin"))) {
    ByteBuffer chunk = reader.readRange(1024, 512); // 512 bytes at offset 1024
}
```

Full storage operations (list, read, write, delete, copy):

```java
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.ListOptions;

try (Storage s = StorageFactory.open(URI.create("file:///data/cache/"))) {
    s.put("hello.txt", "world".getBytes(StandardCharsets.UTF_8));
    try (var stream = s.list("", ListOptions.recursiveGlob("**/*.txt"))) {
        stream.forEach(entry -> System.out.println(entry.key()));
    }
}
```

`StorageFactory.open(URI)` selects the backend by URI scheme alone:

| URI scheme | Backend |
|---|---|
| `file:`, no scheme | local filesystem |
| `http:`, `https:` | HTTP (read-only) |
| `s3:` | AWS S3 (general-purpose buckets and S3 Express One Zone) |
| `az:` | Azure Blob Storage |
| `abfs:`, `abfss:` | Azure Data Lake Storage Gen2 (HNS) |
| `gs:` | Google Cloud Storage (flat or HNS) |

To reach a cloud backend through an `http(s)` URL, name the provider with `storage.provider`.

Use `storage.capabilities()` to discover what each backend supports before
calling optional methods (`atomicMove`, `presignedUrls`, `realDirectories`,
etc.).

## Installation

**Maven:**
```xml
<dependency>
    <groupId>io.tileverse.storage</groupId>
    <artifactId>tileverse-storage-all</artifactId>
    <version>2.0-SNAPSHOT</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'io.tileverse.storage:tileverse-storage-all:2.0-SNAPSHOT'
```

For modular installations (per-backend dependencies only), see the [Installation Guide](https://tileverse.io/storage/how-to/install/).

## Module layout

- `tileverse-storage-core` - RangeReader API, SPI (`StorageProvider`, `StorageConfig`), caching and block-alignment decorators, HTTP and File providers
- `tileverse-storage-s3` - AWS S3 and S3-compatible endpoints (MinIO, etc.)
- `tileverse-storage-azure` - Azure Blob Storage
- `tileverse-storage-gcs` - Google Cloud Storage
- `tileverse-storage-all` - aggregator depending on all providers

Range-reader code lives under `io.tileverse.storage.rangereader.*`; the SPI under `io.tileverse.storage.spi`.

## Configuration keys

Canonical parameter keys live under the `storage.*` namespace (e.g. `storage.s3.region`, `storage.http.timeout-millis`). Legacy `io.tileverse.rangereader.*` keys remain accepted with a one-time WARN per distinct key.

## Related modules

- **[tileverse-pmtiles](../tileverse-pmtiles/)** - PMTiles format support (uses this library)
- **[tileverse-vectortiles](../tileverse-vectortiles/)** - Mapbox Vector Tiles
- **[tileverse-tilematrixset](../tileverse-tilematrixset/)** - OGC tile matrix set math
