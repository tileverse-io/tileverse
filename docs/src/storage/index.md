# Storage

The `Storage` API is the broader I/O abstraction over object storage in tileverse-storage. It exposes the full set of container operations - listing, range and streaming reads, atomic writes, deletes, server-side copy, and presigned URLs - with an honest capability model so callers can adapt to what each backend supports.

The `Storage` API is the recommended entrypoint when you need anything beyond byte-range reads of a single known file. For pure range-read use cases (PMTiles, COG, single-file Parquet) the existing `RangeReader` API remains a perfectly good choice; both APIs ship in the same module and `Storage.openRangeReader(key)` returns a `RangeReader` so you can mix them freely.

## Supported backends

| URI scheme | Backend | Notes |
| :--- | :--- | :--- |
| `file:` | Local filesystem | Real directories, atomic rename |
| `http:`, `https:` | HTTP / HTTPS | Read-only; HEAD + range GET |
| `s3:`, `s3a:` | AWS S3 | General-purpose buckets and S3 Express One Zone (Directory Buckets) |
| `https://*.blob.core.windows.net`, `az:` | Azure Blob Storage | Flat keyspace + virtual directories. `az://<account>/<container>/<path>` is a short-form alias for the canonical `https://` URL (DuckDB / fsspec convention). |
| `abfs:`, `abfss:`, `https://*.dfs.core.windows.net` | Azure Data Lake Storage Gen2 | Real directories + atomic rename when HNS is enabled |
| `gs:` | Google Cloud Storage | Flat or Hierarchical Namespace; HNS auto-detected |

## S3-compatible endpoints

The `s3://` backend transparently handles **any S3-compatible service**, not just AWS. Provider selection works in two ways:

- For an `s3://bucket/key` URI, the S3 backend is selected outright.
- For an `http(s)://endpoint/bucket/key` URI, the resolver issues a HEAD request and selects the S3 backend if the response carries an `x-amz-*` header (`x-amz-request-id` in particular). All major S3-compatible services do.

Tested or known to work via this path:

| Service | URL form | Notes |
| :--- | :--- | :--- |
| **MinIO** | `http://host:9000/bucket/key` | Self-hosted; the `force-path-style` parameter is auto-enabled for non-AWS hosts. |
| **Ceph RADOS Gateway (RGW)** | `https://rgw.example.com/bucket/key` | The dominant S3-compatible backend in modern OpenStack and on-prem deployments. |
| **OpenStack Swift (via `s3api` middleware)** | `https://swift.example.com/bucket/key` | Most Swift deployments enable `s3api`; native Swift API is intentionally not supported (see below). |
| **Cloudflare R2** | `https://<account>.r2.cloudflarestorage.com/bucket/key` | Set `storage.s3.region=auto`. |
| **Backblaze B2** | `https://s3.<region>.backblazeb2.com/bucket/key` | Standard S3 SDK auth. |
| **Wasabi** | `https://s3.<region>.wasabisys.com/bucket/key` | |
| **DigitalOcean Spaces** | `https://<region>.digitaloceanspaces.com/bucket/key` | |
| **IBM Cloud Object Storage** | `https://s3.<region>.cloud-object-storage.appdomain.cloud/bucket/key` | Use the public S3 endpoint, not the legacy Swift one. |
| **Linode Object Storage** | `https://<region>.linodeobjects.com/bucket/key` | |
| **OVHcloud Object Storage (S3 endpoint)** | `https://s3.<region>.cloud.ovh.net/bucket/key` | OVH offers both S3 and Swift endpoints; use the S3 one. |
| **Custom / enterprise** | `https://s3.company.internal/bucket/key` | Anything that returns `x-amz-*` headers. |

Credentials and region come from the same parameters as AWS S3 (`storage.s3.aws-access-key-id`, `storage.s3.aws-secret-access-key`, `storage.s3.region`); the SDK's default credential chain is used when none are set. If the HEAD probe fails (firewalled endpoint, custom auth required to GET headers), force the provider explicitly via `storage.providerId=s3` in the config.

!!! info "Native protocols for OpenStack Swift, Azure Files, FTP, etc. are intentionally not supported"
    `tileverse-storage`'s scope is *cloud-optimized object storage for the GeoTools/GeoServer ecosystem*. Native OpenStack Swift, Azure Files (SMB), HDFS-via-WebHDFS, FTP, and similar are out of scope: the audience is overwhelmingly served by S3-compatible endpoints (which all of the above offer), and adding alternate-protocol backends would multiply the maintenance surface without serving a real user. See [Why tileverse-storage?](why.md#when-not-to-use-this-library) for the broader scope rationale.

## Capability matrix

Every backend declares its capabilities at open time via `Storage.capabilities()`. The matrix below summarizes the most common flags; see the JavaDoc on `StorageCapabilities` for the full set.

| Capability | local | http | S3 GP | S3 Express | Azure Blob | DataLake Gen2 | GCS Flat | GCS HNS |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Range read | Y | Y | Y | Y | Y | Y | Y | Y |
| Streaming read from offset | Y | Y | Y | Y | Y | Y | Y | Y |
| List (flat + hierarchical) | Y | N | Y | Y | Y | Y | Y | Y |
| Real directories | Y | N | N | ~ | N | Y | N | Y |
| Single + multipart writes | Y | N | Y | Y | Y | Y | Y | Y |
| Bulk delete | Y | N | Y (1000) | Y (1000) | Y (256) | Y | Y (100) | Y (100) |
| Server-side copy | N/A | N | Y | Y | Y | Y | Y | Y |
| Atomic move | Y | N | N | N | N | Y | N | Y |
| Conditional ops (`If-Match`, `If-None-Match: *`) | ~ | Y | Y | Y | Y | Y | Y | Y |
| Presigned URLs | N | N | Y (7d) | Y (5min) | Y (SAS, 7d) | N (use blob endpoint) | Y (7d) | Y (7d) |
| Versioning | N | N | Y | N | Y | Y | Y | Y |

A user-metadata map (`Map<String, String>`) is exposed by all object-store backends; local FS and HTTP return an empty map.

## Open a Storage

```java
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

// Same call works for any supported URI scheme.
try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/datasets/v3/"))) {
    // ... use the Storage ...
}
```

`StorageFactory.open(URI)` selects the registered `StorageProvider` whose `canProcess(StorageConfig)` returns `true` for the URI. Authentication uses the SDK's default credential chain unless you provide a `StorageConfig` with explicit credentials.

## What can you do with it?

```java
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.ListOptions;
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
        System.out.println("size=" + entry.size() + " etag=" + entry.etag());
    });

    // List with glob, recursively. The pattern is a shell-style glob:
    //   "year=2024/"             -> immediate children of year=2024/
    //   "year=2024/**"           -> walk all descendants
    //   "year=2024/**\/*.parquet" -> walk + filter
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

    // Range read via the existing RangeReader API
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

## Discoverability vs. fail-fast

Two equivalent ways to handle backend differences:

```java
// 1. Interrogate capabilities up front
StorageCapabilities caps = storage.capabilities();
if (caps.atomicMove()) {
    storage.move(src, dst);
} else {
    // copy + delete fallback (Storage.move does this automatically on flat backends too)
    storage.move(src, dst);
}

// 2. Or just call - unsupported operations throw UnsupportedCapabilityException
try {
    storage.presignGet("file.parquet", Duration.ofMinutes(15));
} catch (UnsupportedCapabilityException e) {
    // backend doesn't support presigned URLs
}
```

## Storage and RangeReader

`Storage.openRangeReader(key)` returns a `RangeReader` for the named blob. The decorator stack you used before (`CachingRangeReader`, `BlockAlignedRangeReader`, `DiskCachingRangeReader`) still works:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.cache.CachingRangeReader;

try (Storage storage = StorageFactory.open(URI.create("s3://bucket/data/"))) {
    RangeReader reader = storage.openRangeReader("file.pmtiles");
    RangeReader cached = CachingRangeReader.builder(reader)
            .withBlockAlignment()
            .build();
    // use `cached` exactly as before
}
```

`RangeReader.close()` releases only that blob's per-reader state; the parent `Storage` remains open. Closing the `Storage` releases the SDK client (refcounted internally so multiple `Storage` instances against the same account share one client).

## Resource lifecycle

- `Storage` instances should be closed when no longer needed (try-with-resources).
- The underlying SDK clients (`S3Client`, `BlobServiceClient`, GCS `Storage`) are reference-counted by the provider's internal cache - opening many `Storage` instances against the same account does not multiply client cost.
- Streams returned by `list(...)` and `ReadHandle`s returned by `read(...)` must be closed by the caller.

## Documented gotchas

A handful of backend-specific quirks are documented in the JavaDoc of each provider. The most load-bearing:

- **Local FS `FileChannel` is interruptible**: `Thread.interrupt()` during a read closes the channel. The implementation reopens on demand, but be aware if you build cancellation on top of interrupts.
- **Azure sync SDK does not honor `Thread.interrupt`**: use `WriteOptions.timeout` / `ReadOptions.timeout` for hard time bounds.
- **S3 Express ETags are random alphanumeric**, not MD5; presigned URLs cap at 5 minutes.
- **S3 Express list results are not lexicographically ordered** (general S3 GP buckets are).
- **GCS HNS** detection happens at open via `bucket.getHierarchicalNamespace()`; capabilities adjust accordingly.

## Configuration keys

Per-backend configuration uses the `storage.*` flat namespace, e.g.:

| Key | Backend |
|---|---|
| `storage.s3.region` | S3 |
| `storage.s3.aws-access-key-id`, `storage.s3.aws-secret-access-key` | S3 |
| `storage.s3.force-path-style` | S3 |
| `storage.azure.account-key`, `storage.azure.sas-token` | Azure Blob |
| `storage.azure.connection-string` | Azure Blob (Azurite, dev) |
| `storage.azure.anonymous=true` | Azure Blob (public containers, no credentials) |
| `storage.gcs.project-id`, `storage.gcs.default-credentials-chain` | GCS |
| `storage.http.timeout-millis`, `storage.http.auth-bearer-token` | HTTP |

Legacy `io.tileverse.rangereader.*` keys are still accepted with a one-time WARN per distinct key; migrate to `storage.*` at your convenience.

## See also

- [Why tileverse-storage?](why.md) - the rationale for this library and how it compares to alternatives (Hadoop FS, jclouds, Micronaut, Spring Resource, NIO providers)
- [Quick Start](user-guide/quick-start.md) - hands-on examples for each backend
- [Range Reader](rangereader/index.md) - the byte-range read API used by PMTiles / COG / single-file Parquet
- [PMTiles](../pmtiles/index.md) - the canonical consumer of the Storage / RangeReader stack
