# Storage

`tileverse-storage` gives one Java API over local files, HTTP, Amazon S3 and S3-compatible services, Azure Blob Storage and Data Lake Storage, and Google Cloud Storage. A `Storage` covers a container: a directory, a bucket, or a prefix. It lists, reads, writes, deletes, copies and presigns the objects under it, and declares which of these operations its backend supports.

Format readers that need byte ranges of one file, such as PMTiles, Cloud Optimized GeoTIFF or Parquet readers, get a `RangeReader` from `Storage.openRangeReader(key)`. See [Read byte ranges](how-to/read-byte-ranges.md).

## Supported backends

| URI scheme | Backend | Notes |
| :--- | :--- | :--- |
| `file:`, no scheme | Local filesystem | Real directories, atomic rename |
| `http:`, `https:` | HTTP / HTTPS | Read-only; range GET |
| `s3:` | AWS S3 | General-purpose buckets and S3 Express One Zone (Directory Buckets) |
| `az:` | Azure Blob Storage | Flat keyspace + virtual directories. `az://<account>/<container>/<path>` addresses `https://<account>.blob.core.windows.net/<container>/<path>` (DuckDB / fsspec convention). |
| `abfs:`, `abfss:` | Azure Data Lake Storage Gen2 | Real directories + atomic rename when HNS is enabled |
| `gs:` | Google Cloud Storage | Flat or Hierarchical Namespace; HNS auto-detected |

The URI scheme alone selects the backend, and no request is sent to choose it. An `http(s)` URL opens the HTTP backend, the URL of an object in a cloud service included: a public object reads without credentials, and a private one is refused (401 or 403; Azure Blob answers 404 or 409). To reach a cloud backend through an `http(s)` URL, name the provider with `storage.provider` (`s3`, `azure`, `azure-datalake` or `gcs`), or use the native scheme.

S3-compatible services, such as MinIO, Ceph, Garage or Cloudflare R2, work through the S3 backend. See [Use each backend](how-to/use-each-backend.md#s3-compatible-services).

## Open a Storage

```java
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

// Same call works for any supported URI scheme.
try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/datasets/v3/"))) {
    // ... use the Storage ...
}
```

`StorageFactory.open(URI)` opens the backend of the URI scheme. The overloads taking `Properties` or a `StorageConfig` also open the provider named by `storage.provider` when one is set. With no credential parameter, the cloud backends use the default credential chain of their SDK; [Authenticate](how-to/authenticate.md) lists the alternatives.

## What can you do with it?

```java
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.WriteOptions;
import io.tileverse.storage.RangeReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/datasets/v3/"))) {

    // Stat (HEAD)
    storage.stat("year=2024/file.parquet").ifPresent(entry -> {
        System.out.println("size=" + entry.size() + " etag=" + entry.etag().orElse("none"));
    });

    // List with a glob; "**" walks the descendants
    try (Stream<StorageEntry> entries = storage.list("year=2024/**/*.parquet")) {
        entries.filter(e -> e instanceof StorageEntry.File)
                .forEach(e -> System.out.println(e.key()));
    }

    // Write a small object atomically
    storage.put("notes/hello.txt", "hello".getBytes(StandardCharsets.UTF_8));

    // Conditional write (compare-and-set on creation)
    storage.put("locks/leader",
            "node-1".getBytes(StandardCharsets.UTF_8),
            WriteOptions.builder().ifNotExists(true).build());

    // Range read
    try (RangeReader reader = storage.openRangeReader("year=2024/file.parquet")) {
        ByteBuffer buf = reader.readRange(0, 4096);
        // ...
    }

    // Delete + bulk delete
    storage.delete("notes/hello.txt");
    DeleteResult result = storage.deleteAll(List.of("a.txt", "b.txt", "c.txt"));
    System.out.println("deleted=" + result.deleted() + " failed=" + result.failed());

    // Server-side copy + move
    storage.copy("src.bin", "dst.bin", CopyOptions.defaults());
    storage.move("dst.bin", "moved.bin"); // atomic on local FS / DataLake Gen2 HNS / GCS HNS

    // Presigned GET URL (where supported)
    if (storage.capabilities().presignedUrls()) {
        URI url = storage.presignGet("file.parquet", Duration.ofMinutes(15));
        // share `url` with a client that doesn't have credentials
    }
}
```

[Use each backend](how-to/use-each-backend.md#pattern-syntax) describes the glob syntax of `list`.

## Capability matrix

Each backend declares its capabilities at open time, in `Storage.capabilities()`. An operation that a backend does not support throws `UnsupportedCapabilityException`. The matrix below summarizes the most common capabilities; the javadoc of `StorageCapabilities` lists them all.

| Capability | local | http | S3 GP | S3 Express | Azure Blob | DataLake Gen2 | GCS Flat | GCS HNS |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Range read | Y | Y | Y | Y | Y | Y | Y | Y |
| Streaming read from offset | Y | Y | Y | Y | Y | Y | Y | Y |
| List (flat + hierarchical) | Y | N | Y | Y | Y | Y | Y | Y |
| Real directories | Y | N | N | N | N | Y | N | Y |
| Single + multipart writes | Y (single only) | N | Y | Y | Y | Y | Y | Y |
| Bulk delete | Y | N | Y (1000) | Y (1000) | Y (256) | Y | Y (100) | Y (100) |
| Server-side copy | Y | N | Y | Y | Y | Y | Y | Y |
| Atomic move | Y | N | N | N | N | Y | N | Y |
| Conditional ops (`If-Match`, `If-None-Match: *`) | ~ | ~ | Y | Y | Y | Y | Y | Y |
| Presigned URLs | N | N | Y (7d) | Y (5min) | Y, with an account key (SAS, 7d) | N (use blob endpoint) | Y (7d) | Y (7d) |
| Versioning | N | N | Y | N | Y | N | Y | Y |

A user-metadata map (`Map<String, String>`) is exposed by all object-store backends; local FS and HTTP return an empty map.

## Resource lifecycle

- Close a `Storage` when you no longer need it, for example with try-with-resources. The `Storage` instances of one account share one SDK client, counted by references: closing one `Storage` leaves the others working.
- Close the streams returned by `list(...)` and `openOutputStream(...)`, and the `ReadHandle`s returned by `read(...)`.
- Closing a `RangeReader` releases only that reader. The `Storage` stays open.

## See also

- [Use each backend](how-to/use-each-backend.md): the smallest example for each backend, and their particularities.
- [Read byte ranges](how-to/read-byte-ranges.md): the `RangeReader` API, batch reads, caching, and block alignment.
- [Configure for performance](how-to/configure.md) and [Authenticate](how-to/authenticate.md): the configuration parameters.
- [Why tileverse-storage?](explanation/why.md): the scope of the library, and how it compares to other storage abstractions.
