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
working. `BlockAlignedRangeReader.getSourceIdentifier()` now returns the delegate identifier
unchanged, and a `BlockAlignedRangeReader.builder(...)` with no declared region builds a
pass-through reader; the constructors keep aligning the whole file.

## Batch reads

`RangeReader.readRanges(List<RangeRequest>)` reads several ranges in one call into
caller-provided buffers. Both decorators propagate batches: the cache forwards only its misses
(as one call), the aligner quantizes in-region entries to deduplicated blocks. Backends merge
nearby ranges into shared fetches and run them in parallel (S3 on the CRT async client, GCS
and Azure on a shared executor, HTTP as one `multipart/byteranges` request with a per-fetch
fallback). If you maintain a delegating RangeReader decorator, override readRanges to forward
the batch to the delegate; a wrapper that does not forward it silently degrades batches to the
sequential per-range default.
