# Migration guide: 1.4.x → 2.0.x

The 2.0 release renames the entire library from `tileverse-rangereader` to
`tileverse-storage` and adds a new container-rooted `Storage` API alongside
the existing single-object `RangeReader`. Maven coordinates, package names,
and the SPI surface all change. This is a breaking release; consumers must
update.

The motivation: the project grew from "byte-range reads of one file" into a
broader I/O abstraction over object storage, with directory listing, writes,
copy/move, presigning, and bulk delete. Generalizing the name and the
package layout makes that explicit.

## Maven coordinates

| 1.4.x | 2.0.x |
|---|---|
| `io.tileverse.rangereader:tileverse-rangereader-core` | `io.tileverse.storage:tileverse-storage-core` |
| `io.tileverse.rangereader:tileverse-rangereader-s3` | `io.tileverse.storage:tileverse-storage-s3` |
| `io.tileverse.rangereader:tileverse-rangereader-azure` | `io.tileverse.storage:tileverse-storage-azure` |
| `io.tileverse.rangereader:tileverse-rangereader-gcs` | `io.tileverse.storage:tileverse-storage-gcs` |
| `io.tileverse.rangereader:tileverse-rangereader-all` | `io.tileverse.storage:tileverse-storage-all` |

The `tileverse-bom` artifact (`io.tileverse:tileverse-bom`) keeps its
coordinates and now manages the `io.tileverse.storage:*` versions. Importing
the BOM continues to be the recommended way to align versions across
modules.

## Package renames

Update every Java import:

| 1.4.x | 2.0.x |
|---|---|
| `io.tileverse.rangereader` | `io.tileverse.storage` (core types: `RangeReader`, `AbstractRangeReader`, exceptions) |
| `io.tileverse.rangereader.cache.*` | `io.tileverse.storage.cache.*` |
| `io.tileverse.rangereader.block.*` | `io.tileverse.storage.block.*` |
| `io.tileverse.rangereader.file.*` | `io.tileverse.storage.file.*` |
| `io.tileverse.rangereader.http.*` | `io.tileverse.storage.http.*` |
| `io.tileverse.rangereader.s3.*` | `io.tileverse.storage.s3.*` |
| `io.tileverse.rangereader.azure.*` | `io.tileverse.storage.azure.*` |
| `io.tileverse.rangereader.gcs.*` | `io.tileverse.storage.gcs.*` |
| `io.tileverse.rangereader.spi.*` | `io.tileverse.storage.spi.*` |
| `io.tileverse.io.*` | unchanged |
| `io.tileverse.cache.*` | unchanged |

Generic utilities (`ByteRange`, `ByteBufferPool`, `IOFunction`, the
`io.tileverse.cache` infrastructure) stay at their original paths.

A `find`/`sed` over your source tree is usually enough:

```bash
find . -name '*.java' -print0 | xargs -0 sed -i \
    -e 's/io\.tileverse\.rangereader\.spi/io.tileverse.storage.spi/g' \
    -e 's/io\.tileverse\.rangereader/io.tileverse.storage/g'
```

## SPI class renames

Backends that implement the SPI must rename four classes:

| 1.4.x | 2.0.x |
|---|---|
| `RangeReaderProvider` | `StorageProvider` |
| `RangeReaderConfig` | `StorageConfig` |
| `RangeReaderParameter` | `StorageParameter` |
| `AbstractRangeReaderProvider` | `AbstractStorageProvider` |

The `RangeReader`, `AbstractRangeReader`, and per-backend reader classes
keep their names (only the package moves).

The `META-INF/services` file for backend registration also moves:

```
META-INF/services/io.tileverse.rangereader.spi.RangeReaderProvider
       ↓
META-INF/services/io.tileverse.storage.spi.StorageProvider
```

## Configuration keys

The `storage.*` configuration namespace was introduced in **1.4.0** as a
transitional change with forward compatibility — consumers on 1.4.x can use
either the legacy `io.tileverse.rangereader.*` keys or the new `storage.*`
keys interchangeably. 2.0 keeps both forms working and the legacy keys still
emit a one-time `WARN` per distinct key. If you already migrated your
configuration on 1.4.x, no further key changes are required for 2.0.

Mapping for reference:

| 1.4.x | 2.0.x |
|---|---|
| `io.tileverse.rangereader.uri` | `storage.uri` |
| `io.tileverse.rangereader.provider-id` | `storage.provider-id` |
| `io.tileverse.rangereader.s3.region` | `storage.s3.region` |
| `io.tileverse.rangereader.s3.access-key-id` | `storage.s3.aws-access-key-id` |
| `io.tileverse.rangereader.s3.secret-access-key` | `storage.s3.aws-secret-access-key` |
| `io.tileverse.rangereader.azure.account-key` | `storage.azure.account-key` |
| `io.tileverse.rangereader.azure.sas-token` | `storage.azure.sas-token` |
| `io.tileverse.rangereader.gcs.project-id` | `storage.gcs.project-id` |
| `io.tileverse.rangereader.http.username` | `storage.http.username` |
| `io.tileverse.rangereader.http.password` | `storage.http.password` |
| `io.tileverse.rangereader.http.bearer-token` | `storage.http.bearer-token` |
| `io.tileverse.rangereader.caching.enabled` | `storage.caching.enabled` |

Migrate at your convenience; the legacy keys will be removed in a future
release. Each provider's `getParameters()` now reports the canonical
`storage.*` key.

## Factory API: `RangeReaderFactory` is gone

The single-URI factory entry point was removed. Code that did:

```java
// 1.4.x
try (RangeReader reader = RangeReaderFactory.create(uri)) {
    ByteBuffer header = reader.readRange(0, 1024);
}
```

becomes one of two forms depending on how many objects you read:

**One-shot single-object reads** (one URL in, one closeable out — same shape
as the old API):

```java
// 2.0.x - leaf URL convenience
try (RangeReader reader = StorageFactory.openRangeReader(uri)) {
    ByteBuffer header = reader.readRange(0, 1024);
}
```

`StorageFactory.openRangeReader(URI[, Properties])` opens the appropriate
backend, reads the single object, and returns a reader that owns its
underlying SDK client. Closing the reader releases the client. This is the
direct equivalent of `RangeReaderFactory.create(uri)`.

**Multi-object reads against the same backend** (the case `RangeReaderFactory`
couldn't express): hold a `Storage` once and call `openRangeReader(key)` per
file. The `Storage` is **thread-safe**, owns the underlying SDK client, and
is reference-counted across sibling `Storage` instances against the same
account.

```java
// 2.0.x - container handle, many keys
URI parent = URI.create("s3://my-bucket/datasets/v3/");
try (Storage storage = StorageFactory.open(parent)) {
    try (RangeReader a = storage.openRangeReader("a.pmtiles");
         RangeReader b = storage.openRangeReader("b.pmtiles")) {
        // ...
    }
}
```

Long-lived consumers (GeoTools datastores, application services) that read
many files from the same backend root should hold a single `Storage` and
call `openRangeReader(key)` per request. Close the `Storage` when the
consumer is disposed; the SDK client is released when the last reference
drops.

`Properties`-based configuration is preserved through the
`StorageFactory.open(Properties)`, `StorageFactory.open(URI, Properties)`,
and `StorageFactory.openRangeReader(URI, Properties)` overloads — this is
the bridge for tools (GeoTools datastore params, Spring configuration
binding, etc.) that pass configuration as a flat map.

## SPI factory methods removed

`StorageProvider` no longer exposes `create(URI)` / `create(StorageConfig)`,
and `AbstractStorageProvider.createInternal(StorageConfig)` is gone. Custom
backends now implement only `createStorage(StorageConfig)` (returning a raw
`Storage` rooted at `config.uri()`) and `declaredCapabilities()`, in addition
to the existing `getId`, `getDescription`, `isAvailable`, `canProcess`, and
`buildParameters` hooks.

Caching auto-decoration is no longer the provider's responsibility — it's
applied uniformly by `StorageFactory.open` based on `storage.caching.*`
parameters in the resolved `StorageConfig`. Backends just produce raw
`Storage` instances.

## Per-backend RangeReader Builders are gone

The public `XxxRangeReader.Builder` classes (and the corresponding
`XxxRangeReader.builder()` static factories) are removed in 2.0. The per-backend
`XxxRangeReader` and `XxxStorage` classes are also demoted to package-private:
the only public type per backend is the `StorageProvider`.

For typical SPI / Properties-driven configuration, use `StorageFactory`:

```java
// 1.4.x
S3RangeReader reader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/data.bin"))
    .region(Region.US_WEST_2)
    .credentialsProvider(myProvider)
    .build();

// 2.0.x — Properties-driven via StorageFactory
Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");
try (RangeReader reader = StorageFactory.openRangeReader(
        URI.create("s3://my-bucket/data.bin"), props)) {
    // ...
}
```

For SDK-injection use cases (Spring-managed clients, custom retry policies,
fake/mock SDK objects in tests), each provider exposes a public
`open(URI, sdkClient)` static factory that returns a {@code Storage} backed by
the supplied client. The returned Storage **borrows** the client; closing the
Storage does NOT close the client.

```java
// 2.0.x — SDK-injection escape hatch
@Bean Storage tiles(S3Client springS3) {
    return S3StorageProvider.open(
        URI.create("s3://my-bucket/tiles/"), springS3);
}

// elsewhere:
try (RangeReader r = storage.openRangeReader("00/00.pmtiles")) { ... }
```

| Backend | Public escape-hatch factory |
|---|---|
| HTTP | `HttpStorageProvider.open(URI, HttpClient[, HttpAuthentication])` |
| S3 | `S3StorageProvider.open(URI, S3Client)` *(degraded)* |
| S3 | `S3StorageProvider.open(URI, S3ClientBundle)` *(full feature set)* |
| Azure Blob | `AzureBlobStorageProvider.open(URI, BlobServiceClient)` |
| Azure DataLake Gen2 | `AzureDataLakeStorageProvider.open(URI, DataLakeServiceClient, BlobServiceClient)` |
| GCS | `GoogleCloudStorageProvider.open(URI, com.google.cloud.storage.Storage)` |

S3 has two overloads because `S3Storage` uses up to four SDK objects (sync
`S3Client`, CRT `S3AsyncClient`, `S3TransferManager`, `S3Presigner`) for the full
feature surface. Pass a sync-only `S3Client` to get range reads and small writes;
build an `S3ClientBundle.of(sync, async, tm, presigner)` for full feature parity
with the SPI path. Operations that require an absent SDK object throw
`UnsupportedCapabilityException`.

## New `Storage` API surface

`Storage` is the broader container abstraction. Beyond `openRangeReader`:

- `stat(key)`, `exists(key)` — metadata without fetching the body
- `list(pattern, options)` — directory-style listing with shell-style globs
- `read(key, options)` — sequential reads with optional offset, returns `ReadHandle`
- `put(key, bytes/Path/OutputStream, options)` — atomic writes (capability-gated)
- `delete(key)`, `deleteAll(keys)` — single and bulk deletes
- `copy(srcKey, dstKey)`, `copy(srcKey, dstStorage, dstKey)`, `move(srcKey, dstKey)`
- `presignGet(key, ttl)`, `presignPut(key, ttl, options)`

Not every backend supports every method. Inspect
`storage.capabilities()` (a `StorageCapabilities` record) before calling
optional methods, or rely on `requireXxx` helpers that fail fast with
`UnsupportedCapabilityException`. The `StorageCapabilities` Javadoc
documents what each flag controls and which backends typically report
`true` vs `false`.

## Quick checklist

1. Bump `tileverse-bom` (or pin) to `2.0.0`.
2. Update every Maven dependency `groupId` from `io.tileverse.rangereader`
   to `io.tileverse.storage`.
3. Run a search-and-replace on imports
   (`io.tileverse.rangereader.spi → io.tileverse.storage.spi`,
   then `io.tileverse.rangereader → io.tileverse.storage`).
4. If you implement the SPI: rename four classes, the `META-INF/services`
   file, and replace `createInternal` with `createStorage`.
5. Replace `RangeReaderFactory.create(URI)` callers. For single-object reads
   use `StorageFactory.openRangeReader(uri[, props])` (one closeable, same
   shape as 1.4.x). For consumers that read many objects from the same
   backend, hold a `Storage` via `StorageFactory.open(parent)` for the
   lifetime of your component and call `openRangeReader(key)` per request;
   close the `Storage` on dispose.
6. Replace `XxxRangeReader.builder()...build()` callers. For Properties-driven
   configuration use `StorageFactory.open(uri, props)` /
   `StorageFactory.openRangeReader(uri, props)`. For SDK-client injection use
   `XxxStorageProvider.open(URI, sdkClient)` (e.g.
   `S3StorageProvider.open(uri, mySpringS3)`).
7. Migrate config keys from `io.tileverse.rangereader.*` to
   `storage.*` to silence the legacy-key warnings.
8. Run your tests.
