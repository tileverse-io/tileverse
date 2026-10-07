# Migrating from 2.0

Version 2.1 moves block alignment out of `CachingRangeReader` and out of configuration,
adds the `readRanges` batch read, and selects the backend by URI scheme alone.

## Block alignment is composed, not configured

`CachingRangeReader` caches exactly the ranges it is asked for. To cache block-grain data,
stack a `BlockAlignedRangeReader` above it and declare the byte regions to align:

```java
CachingRangeReader cache = CachingRangeReader.builder(backend).build();
BlockAlignedRangeReader reader = BlockAlignedRangeReader.builder(cache)
        .blockSize(64 * 1024)
        .alignRegion(0, headerAndIndexLength) // or alignWholeFile()
        .build();
```

Regions can also be declared after construction (`reader.alignRegion(offset, length)`) as the
file layout is discovered. Reads fully inside a declared region are fetched as whole blocks on
an absolute grid; every other read is passed through untouched.

## Removed APIs and keys

| Removed in 2.1 | Replacement |
| --- | --- |
| `CachingRangeReader.Builder.blockSize(int)` | `BlockAlignedRangeReader` above the cache |
| `CachingRangeReader.Builder.withBlockAlignment()` / `withoutBlockAlignment()` | same |
| `CachingRangeReader.Builder.headerSize(int)` / `withHeaderBuffer()` / `withoutHeaderBuffer()` | declare a region over the header; hot header blocks stay cached |
| `storage.caching.blockaligned` config key | compose the aligner in code |
| `storage.caching.blocksize` config key | `BlockAlignedRangeReader.Builder.blockSize(int)` |
| `storage.s3.use-default-credentials-provider` config key and `S3StorageProvider.S3_USE_DEFAULT_CREDENTIALS_PROVIDER` | none: with no anonymous access, access keys, or profile set, S3 uses the AWS default credentials provider chain, as it did in 2.0 |
| `storage.gcs.default-credentials-chain` config key and `GoogleCloudStorageProvider.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS` | `storage.gcs.anonymous`; a saved `storage.gcs.default-credentials-chain=false` still reads as `storage.gcs.anonymous=true` |
| `S3StorageProvider.open(URI, S3Client)` | `S3StorageProvider.open(URI, S3AsyncClient)` |
| `S3StorageProvider.open(URI, S3ClientBundle)` and `S3ClientBundle` | `S3StorageProvider.open(URI, S3AsyncClient)`; presigned URLs need a `Storage` opened from a URI or a `StorageConfig` |
| `StorageProvider.canProcessHeaders(URI, Map)` | none: provider selection sends no request; a provider overriding it drops the override |

The removed keys are ignored like any unknown parameter; `storage.caching.enabled` keeps
working but now defaults to `false`; pass `storage.caching.enabled=true` to keep the previous
behavior. Caching raw byte ranges only pays off when the same ranges are requested again,
which depends on the format: worth it for tile archives such as PMTiles, not for query-driven
reads such as GeoParquet.
`BlockAlignedRangeReader.getSourceIdentifier()` now returns the delegate identifier
unchanged, and a `BlockAlignedRangeReader.builder(...)` with no declared region builds a
pass-through reader; the constructors keep aligning the whole file.

## Batch reads

`RangeReader.readRanges(List<RangeRequest>)` reads several ranges in one call into
caller-provided buffers. Both decorators propagate batches: the cache forwards only its misses
(as one call), the aligner quantizes in-region entries to deduplicated blocks. Backends merge
nearby ranges into shared fetches and run them in parallel (S3 on the async client, GCS
and Azure on a shared executor, HTTP as multi-range GETs of up to 100 fetches
each, with a per-fetch fallback). If you maintain a delegating RangeReader decorator, override
readRanges to forward the batch to the delegate; a wrapper that does not forward it silently
degrades batches to the sequential per-range default.

`readRanges` now returns a `BatchReadResult` instead of an `int[]`: `bytesRead(i)` holds what
the array entry held, and `fetches()`, `bytesTransferred()` and `bytesFromCache()` report what
the call cost. A decorator builds its own result and folds the delegate's numbers into it with
`merge`. Once the call returns or throws, no implementation thread is still writing to any
target, and a caller may reuse or release every target at that point.

```java
BatchReadResult result = reader.readRanges(requests);
for (int i = 0; i < result.requests(); i++) {
    int bytesRead = result.bytesRead(i); // what read[i] used to hold
}
```

The merge gap, the fetch cap and the fetch parallelism are `StorageConfig` parameters of the
object-store and HTTP providers, resolved once per `Storage`: `storage.batch.max-gap`,
`storage.batch.max-fetch` and `storage.batch.max-in-flight-fetches`. The
`io.tileverse.storage.batch.objectstore.maxgap`, `io.tileverse.storage.batch.http.maxgap` and
`io.tileverse.storage.batch.maxfetch` system properties of the 2.1 milestones are gone; set
the parameters instead. The S3 async path, which used to start every planned fetch at once, now
honors the in-flight bound too.

## Provider selection

With no `storage.provider`, the URI scheme alone selects the backend: `s3` -> S3; `gs` -> GCS;
`az` -> Azure Blob; `abfs`, `abfss` -> Azure Data Lake; `http`, `https` -> HTTP; `file` or no scheme ->
local files. 2.0 sent a HEAD request to an `http(s)` URL and picked S3, Azure Blob or GCS from the
response headers or the host.

- An `http(s)` URL with no provider named opens the HTTP backend. Public objects read without
  credentials. What used to reach a cloud backend this way needs the provider named, or the native
  scheme:
    - a private bucket or container is refused: 401 or 403, and 404 or 409 from Azure Blob;
    - `list` and the write operations raise `UnsupportedCapabilityException`;
    - an S3-compatible service addressed by its URL (MinIO, Ceph RGW, Swift `s3api`, R2): keep the URL
      and set `storage.provider=s3`, or use `s3://bucket/key` with `storage.s3.endpoint`;
    - `https://<account>.blob.core.windows.net/...`, `https://<account>.dfs.core.windows.net/...` and
      `https://storage.googleapis.com/...`: set `storage.provider` to `azure`, `azure-datalake` or `gcs`,
      or use `az://`, `abfss://`, `gs://`.
- `canProcess` of the S3, Azure and GCS providers answers `false` for an `http(s)` URI unless the
  config names them.
- A provider of your own claiming `http(s)` URIs competes with the HTTP provider by `getOrder()`
  alone. Claim them only when `isNamedBy(config)` answers `true`, as the built-in providers do.
- Selection sends no request. `URI ambiguity detected` can only come from providers of equal order
  claiming one scheme.
- A blank `storage.provider` names no provider. 2.0 failed with `The specified StorageProvider is not
  found`.

## S3 clients

- S3 storages opened from a URI or a `StorageConfig` send their requests through one CRT HTTP
  client shared by the process, with one connection pool per host: single reads, batches and
  uploads share it. 2.0 built a CRT-based `S3AsyncClient` per endpoint and credentials, each
  reserving a native buffer pool of at least 1 GiB.
- Without `io.tileverse.storage.s3-http-client.max-concurrency`, a pool holds up to a tenth of
  the memory limit seen by the JVM, or of four times its maximum heap when that is smaller,
  divided by 7 MiB connections, between 50 and 500.
- A multipart upload keeps at most an eighth of the pool in flight. 2.0 left the number of
  parts in flight to its CRT S3 client.
- A `Storage.read` stream left unread buffers about 5 MiB of its body.
- `Storage.read` streams an object over one connection. 2.0 split a large read across
  connections.
- `S3StorageProvider.open(URI, S3AsyncClient)` takes the place of `open(URI, S3Client)` and
  `open(URI, S3ClientBundle)`. The `Storage` uses the client as built and leaves it open;
  presigned URLs need a `Storage` opened from a URI or a `StorageConfig`.
- Failures of the SDK client itself, such as a refused connection or missing credentials,
  arrive as `StorageException`. An interrupted `readRange` or `Storage` call fails with an
  `InterruptedIOException` cause and leaves the interrupt flag set; `readRanges` ignores the
  interrupt and completes its batch.
- A wrong region, a missing bucket or a denied listing fails `Storage.list` itself. 2.0 failed
  while the stream was consumed.
- An S3-compatible endpoint answering without an `ETag` header reads like any other. A
  CRT-based client passed by the caller rejects such answers.

## GCS credentials

- A GCS `Storage` authenticates with Application Default Credentials unless `storage.gcs.anonymous=true`.
  2.0 read anonymously by default when opened through `StorageFactory`.
- Opening a `Storage` fails when no Application Default Credentials can be found. 2.0 fell back to
  anonymous access.
- An endpoint override (`storage.gcs.endpoint`) keeps the credentials. 2.0 made it anonymous: emulator
  configurations add `storage.gcs.anonymous=true`.

## Local files

- `getSourceIdentifier()` of a local reader is the file's real path, resolved once when the
  reader is built, instead of the absolute path as spelled: a symlink, a case difference on a
  case-insensitive volume, or `/tmp` against `/private/tmp` no longer yield several identifiers,
  and cache keys derived from it shift once.
- A read on a thread whose interrupt flag is set fails at once, without reading or touching the
  channel, with a `StorageException` caused by an `InterruptedIOException`; the flag stays set,
  and the read is never retried. It used to retry on fresh channels the interrupt kept closing.
- `storage.file.idle-timeout` rejects a value above zero and below one millisecond when the
  `Storage` is created; `PT0S` still disables the idle close.
- `FileStorageProvider.openRangeReader(Path)` and `openRangeReader(Path, Duration)` open a
  reader over one file without a `Storage`, with `NotFoundException` for a missing file and
  `IllegalArgumentException` for a directory.

## PMTiles

`PMTilesReader.getTileIndices()` and `getTileIndicesByZoomLevel(int)` are removed. They
accumulated every leaf directory of the archive into one in-memory list, an unbounded
allocation on planet-scale files. Walk the directories through `getRootDirectory()`,
`getDirectory(PMTilesEntry)`, and the bounded `getTileIndices(PMTilesEntry, Consumer)`
instead.

`PMTilesReader` now block-aligns its header, directory, and metadata reads internally from
the parsed file layout. Compose `new PMTilesReader(CachingRangeReader.of(reader))` and drop
any hand-rolled `BlockAlignedRangeReader` around the input; tile reads stay exact.
