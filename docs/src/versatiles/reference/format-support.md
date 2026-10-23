# Format support

This page lists the parts of the [Versatiles container specification](https://docs.versatiles.org/compendium/specification_container.html) supported by `tileverse-versatiles`, with the classes, exceptions, and caches involved.

## Container version

The reader supports version 2 of the container format. A version 2 file starts with the ASCII magic `versatiles_v02`. Other files fail to open with `InvalidHeaderException`.

## Tile formats

The header records the format of the tile bodies. `VersatilesReader.getTile` returns the tile bodies, whatever their format. The tile stores decode the formats listed in the last column.

| Code | `TileFormat` | MIME type | `isVector()` | Decoded by |
| :--- | :--- | :--- | :--- | :--- |
| `0x00` | `BIN` | `application/octet-stream` | no | - |
| `0x10` | `PNG` | `image/png` | no | `VersatilesRasterTileStore` |
| `0x11` | `JPEG` | `image/jpeg` | no | `VersatilesRasterTileStore` |
| `0x12` | `WEBP` | `image/webp` | no | `VersatilesRasterTileStore` |
| `0x13` | `AVIF` | `image/avif` | no | `VersatilesRasterTileStore`, with an AVIF plugin for `ImageIO` |
| `0x14` | `SVG` | `image/svg+xml` | no | - |
| `0x20` | `PBF` | `application/x-protobuf` | yes | `VersatilesVectorTileStore` |
| `0x21` | `GEOJSON` | `application/geo+json` | yes | - |
| `0x22` | `TOPOJSON` | `application/topo+json` | yes | - |
| `0x23` | `JSON` | `application/json` | no | - |

PNG and JPEG decoders come with the JDK. The WebP decoder comes from the `com.github.usefulness:webp-imageio` dependency of the module. The raster store refuses an archive when no `ImageIO` decoder reads its format.

## Compression

| Code | `Compression` | Applies to |
| :--- | :--- | :--- |
| `0x00` | `NONE` | metadata and tile bodies |
| `0x01` | `GZIP` | metadata and tile bodies |
| `0x02` | `BROTLI` | metadata and tile bodies |
| `0x03` | `ZSTD` | metadata and tile bodies |

The header declares the compression of the metadata and of the tile bodies. The block index and the tile indexes are always compressed with Brotli, regardless of the header. The reader decompresses tiles before it returns them.

## Tile coordinates

Tiles use the XYZ scheme of web maps, in the `WebMercatorQuad` tile matrix set: row 0 is the northernmost row. `getTile` returns an empty `Optional` for a zoom level outside the range of the archive, for a negative column or row, and for a position without a tile.

The tile matrix set of a store ends at zoom level 24, the last level of `WebMercatorQuad`. `getTile` reads deeper levels too.

## Archives written by the versatiles tool

The `versatiles` tool writes two kinds of files outside the specification. The reader accepts both:

- **An inverted bounding box**, with a minimum above its maximum, when a conversion uses a bounding box outside the data. `VersatilesHeader.geographicBoundingBox()` then returns the bounds of the world.
- **A block index of length zero** for an archive without tiles. Such an archive opens, and `getTile` returns an empty `Optional`.

## Reads

| Operation | Range requests |
| :--- | :--- |
| Open an archive | 3: the header, the metadata, and the block index |
| Read a tile, tile index cached | 1 |
| Read a tile, tile index not cached | 2: the tile index of the block, then the tile |

An archive without metadata, or without tiles, takes one request less to open. The reader reads a part of the archive in one request and decompresses it in memory.

## Caches

| Cache | Contents | Bounds |
| :--- | :--- | :--- |
| `tileverse-versatiles-tile-index-cache` | Tile indexes of recently read blocks, one sub-cache per archive | 5% of the maximum heap; an entry expires 30 seconds after its last use |
| `tileverse-vectortile-cache` | Decoded tiles of `VersatilesVectorTileStore`, shared with the other vector tile stores | 15% of the maximum heap; an entry expires 10 seconds after its last use |

Both caches belong to `CacheManager.getDefault()`, unless `cacheManager(CacheManager)` sets another manager on the reader or on the store. The block index is not cached: the reader reads it once and keeps it until it is closed.

## Exceptions

| Exception | Thrown when |
| :--- | :--- |
| `InvalidHeaderException` | The file is not a version 2 container, is shorter than the header, or has a header field outside the specification: an unknown tile format or compression code, a reversed zoom range, or a section longer than 2 GiB. |
| `UnsupportedTileFormatException` | A tile store cannot decode the tile format of the archive, or no `ImageIO` decoder reads it. |
| `IllegalArgumentException` | A tile store is created over an archive with only zoom levels deeper than 24. |
| `IOException` | The file ends before a part it declares, or the block index or a tile index is damaged. |
| `StorageException` | The storage fails. Its subclass `NotFoundException` means the file is missing. |

`InvalidHeaderException` and `UnsupportedTileFormatException` extend `IOException`. `StorageException` is unchecked. The `loadTile` method of the tile stores wraps I/O errors in `UncheckedIOException`.

## Limits

- The reader loads the metadata into one buffer, and the block index into another. A Java array limits a buffer to 2 GiB.
- The module does not write archives. The [`versatiles`](https://github.com/versatiles-org/versatiles-rs) tool converts other tile containers, such as PMTiles and MBTiles, to Versatiles containers.
