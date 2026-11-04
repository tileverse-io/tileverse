# Migrating from 2.1

Version 2.2 moves the general-purpose I/O classes and the caches out of `tileverse-storage-core`, into two new modules:

- `io.tileverse.io:tileverse-io` holds the `io.tileverse.io` package: `ByteBufferPool`, `ByteRange`, `IOFunction`, and the stream and channel adapters over buffers.
- `io.tileverse.cache:tileverse-cache` holds the `io.tileverse.cache` package.

`tileverse-storage-core` depends on both modules. A project depending on it builds unchanged, and the packages keep their names.

## Adapters back in `io.tileverse.io`

The adapters of `io.tileverse.storage.adapters` that do not need a `RangeReader` move to `io.tileverse.io`, their package in 2.0. The classes of the old package remain, deprecated, and are removed in 2.3.0.

| Deprecated in 2.2 | Replacement |
| --- | --- |
| `io.tileverse.storage.adapters.ByteBufferOutputStream` | `io.tileverse.io.ByteBufferOutputStream` |
| `io.tileverse.storage.adapters.ByteBufferSinkException` | `io.tileverse.io.ByteBufferSinkException` |
| `io.tileverse.storage.adapters.ReadableByteChannelDataInput` | `io.tileverse.io.ReadableByteChannelDataInput` |
| `io.tileverse.storage.adapters.SeekableByteChannelDataInput` | `io.tileverse.io.SeekableByteChannelDataInput` |
| `io.tileverse.storage.adapters.SeekableByteChannelImageInputStream` | `io.tileverse.io.SeekableByteChannelImageInputStream` |

Each deprecated class extends its replacement. The deprecated `SeekableByteChannelDataInput` does not extend the deprecated `ReadableByteChannelDataInput`: assign it to the type of `io.tileverse.io` instead. The classes in `io.tileverse.io` are not final, and can be extended.

`RangeReaderReadableByteChannel` and `RangeReaderSeekableByteChannel` stay in `io.tileverse.storage.adapters`.
