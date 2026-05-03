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
| `io.tileverse.rangereader.spi.*` | `io.tileverse.storage.spi.*` (provider extension points only; see note below) |
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

Note that `StorageConfig` and `StorageParameter` live in the consumer-facing
`io.tileverse.storage` package -- the SPI package retains only the
extension points (`StorageProvider`, `AbstractStorageProvider`):

| Old (intermediate 2.0-SNAPSHOT) | New (2.0) |
|---|---|
| `io.tileverse.storage.spi.StorageConfig` | `io.tileverse.storage.StorageConfig` |
| `io.tileverse.storage.spi.StorageParameter` | `io.tileverse.storage.StorageParameter` |
| `StorageConfig.matches(config, providerId, schemes...)` (static) | `StorageProvider.matches(config, schemes...)` (default method on the interface; the provider's id is read from `getId()` automatically) |

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

Both `RangeReaderFactory.create(uri)` (the 1.4.x convenience) and the
short-lived 2.0 `StorageFactory.openRangeReader(uri[, props])` overloads
have been removed. The 2.0 model is uniform across every backend: open a
`Storage` for the container, ask it for a `RangeReader` for each leaf you
want to read.

```java
// 1.4.x
try (RangeReader reader = RangeReaderFactory.create(uri)) {
    ByteBuffer header = reader.readRange(0, 1024);
}

// 2.0.x — the only construction model
URI parent = URI.create("s3://my-bucket/datasets/v3/");
URI leaf = URI.create("s3://my-bucket/datasets/v3/file.pmtiles");
try (Storage storage = StorageFactory.open(parent);
        RangeReader reader = storage.openRangeReader(leaf)) {
    ByteBuffer header = reader.readRange(0, 1024);
}
```

`Storage.openRangeReader(URI)` validates that the URI is within the
Storage's namespace (matching scheme + authority, descendant path)
and derives the relative key for you. The String-key overload
`storage.openRangeReader("file.pmtiles")` is equivalent and avoids the URI
parse for callers that already know the key.

For long-lived consumers reading many files from the same root (GeoTools
datastores, application services), hold the `Storage` for the lifetime of
the component and open a per-request `RangeReader`. The `Storage` is
**thread-safe**, owns the underlying SDK client, and is reference-counted
across sibling `Storage` instances against the same account; closing the
last `Storage` against a given account releases the client. This applies
uniformly to `S3`, `Azure`, `GCS`, and `HTTP` — the JDK `HttpClient` is
also refcounted via `HttpClientCache`, keyed by `(connect timeout,
trust-all-certificates)`. Because of that, opening and closing a `Storage`
for a single read is cheap on every backend, including HTTP.

PMTiles consumers have a one-line shortcut:
`PMTilesReader.open(URI)` does the parent/leaf split, opens the parent
`Storage`, gets the `RangeReader`, and bundles them so closing the
returned reader releases everything.

```java
try (PMTilesReader reader = PMTilesReader.open(URI.create("s3://my-bucket/world.pmtiles"))) {
    // ...
}
```

`Properties`-based configuration is preserved through the
`StorageFactory.open(Properties)` and `StorageFactory.open(URI, Properties)`
overloads — the bridge for tools (GeoTools datastore params, Spring
configuration binding, etc.) that pass configuration as a flat map.

## SPI factory methods removed

`StorageProvider` no longer exposes `create(URI)` / `create(StorageConfig)`,
and `AbstractStorageProvider.createInternal(StorageConfig)` is gone. Custom
backends now implement only `createStorage(StorageConfig)` (returning a raw
`Storage` rooted at `config.baseUri()`) and `declaredCapabilities()`, in addition
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
URI bucket = URI.create("s3://my-bucket/");
URI leaf = URI.create("s3://my-bucket/data.bin");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader reader = storage.openRangeReader(leaf)) {
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

## `Storage` URI is a container, never a single object

`Storage.baseUri()` is documented as a directory / container / bucket-prefix.
It is **never** a single object. The cloud backends accept any prefix as-is
(they have no ground truth to detect a leaf URI). The file backend is strict:
a URI that points at an existing regular file is rejected with "must be a
directory"; a URI that points at a non-existent path is rejected with
"must point to an existing directory" (the provider does **not** auto-create it).

If you previously relied on the early-2.0-SNAPSHOT behaviour where
`StorageFactory.open(file:///path/to/file.pmtiles)` silently re-rooted
at the parent directory, you now need to do the parent split yourself —
or use `PMTilesReader.open(URI)`, which does it for you. (1.4.x had no
`StorageFactory`; the equivalent `RangeReaderFactory.create(URI)` is
covered in the [Factory API](#factory-api-rangereaderfactory-is-gone)
section above.)

```java
// 2.0.x — parent-split done explicitly
URI leaf = URI.create("file:///path/to/world.pmtiles");
URI parent = URI.create("file:///path/to/");
try (Storage storage = StorageFactory.open(parent);
        RangeReader reader = storage.openRangeReader(leaf)) {
    // ...
}

// 2.0.x — PMTiles consumers get the one-line shortcut
try (PMTilesReader reader = PMTilesReader.open(URI.create("file:///path/to/world.pmtiles"))) {
    // ...
}
```

## Path-traversal hardening

Every key-accepting `Storage` method (`stat`, `openRangeReader`, `read`,
`put`, `openOutputStream`, `delete`, `deleteAll`, `copy`, `move`,
`presignGet`, `presignPut`) and `list(pattern)` now reject keys with:

- a leading `/`
- a `..` or `.` path segment (split on `/`)
- a NUL byte
- empty (or `null`)

`FileStorage` adds a `Path.startsWith(root)` bounds check after resolving
the key to catch Windows backslash separators and other shapes the
lexical guard cannot see.

Callers that legitimately used `..` in object names will get
`IllegalArgumentException` and need to switch to a different naming
scheme. Cloud-backend consumers are unlikely to be affected — `..` was
treated as a literal segment in S3/Azure/GCS keys, but the convention in
practice is to avoid it.

`Storage.openRangeReader(URI)` additionally rejects URIs whose
percent-encoded path contains a traversal segment (e.g. `%2E%2E`),
because some HTTP servers decode mid-path and would honor it as
traversal. The URI overload also drops fragments silently (RFC 3986
fragments are client-only) and preserves query strings into the derived
key (load-bearing for HTTP signed URLs and SAS tokens on the leaf).

## Quick checklist

1. Bump `tileverse-bom` (or pin) to `2.0.0`.
2. Update every Maven dependency `groupId` from `io.tileverse.rangereader`
   to `io.tileverse.storage`.
3. Run a search-and-replace on imports
   (`io.tileverse.rangereader.spi → io.tileverse.storage.spi`,
   then `io.tileverse.rangereader → io.tileverse.storage`).
4. If you implement the SPI: rename four classes, the `META-INF/services`
   file, and replace `createInternal` with `createStorage`.
5. Replace `RangeReaderFactory.create(URI)` callers with the two-resource
   pattern: `StorageFactory.open(parentUri[, props])` to hold the `Storage`
   for the lifetime of the consumer, then `storage.openRangeReader(leafUri)`
   per request. PMTiles consumers can use `PMTilesReader.open(URI)` for the
   one-line case. Close the `Storage` on dispose.
6. Replace `XxxRangeReader.builder()...build()` callers. For Properties-driven
   configuration use `StorageFactory.open(parentUri, props)`. For SDK-client
   injection use `XxxStorageProvider.open(URI, sdkClient)` (e.g.
   `S3StorageProvider.open(uri, mySpringS3)`).
7. Migrate config keys from `io.tileverse.rangereader.*` to
   `storage.*` to silence the legacy-key warnings.
8. Run your tests.
