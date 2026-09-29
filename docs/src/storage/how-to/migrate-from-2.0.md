# Migrating from 2.0

Version 2.1 moves block alignment out of `CachingRangeReader` and out of configuration, and
adds the `readRanges` batch read.

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
and Azure on a shared executor, HTTP as one `multipart/byteranges` request with a per-fetch
fallback). If you maintain a delegating RangeReader decorator, override readRanges to forward
the batch to the delegate; a wrapper that does not forward it silently degrades batches to the
sequential per-range default.

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

## S3 clients

- Every S3 `Storage` opened from a URI or a `StorageConfig` runs its async requests on one CRT
  HTTP client shared by the process. 2.0 built a CRT-based `S3AsyncClient` per endpoint and
  credentials, each reserving a native buffer pool of at least 1 GiB.
- `Storage.read` streams an object over one connection, through the sync client. 2.0 split a
  large read across connections.
- An S3-compatible endpoint answering without an `ETag` header reads like any other.

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
