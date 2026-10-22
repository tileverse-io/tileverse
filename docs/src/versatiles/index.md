# Versatiles

This module reads [VersaTiles](https://versatiles.org/) containers, a single-file archive format for map tiles. It implements version 2 of the [container specification](https://docs.versatiles.org/compendium/specification_container.html).

The module reads archives and does not write them. To create an archive, use the [`versatiles`](https://github.com/versatiles-org/versatiles-rs) command-line tool. For example, `versatiles convert input.pmtiles output.versatiles` converts a PMTiles archive.

## Features

- **Cloud-optimized reads**: a tile costs one range request once the index of its block is cached.
- **Storage agnostic**: works with the [`RangeReader`](../storage/how-to/read-byte-ranges.md) backends of `tileverse-storage`: local file, HTTP, S3, Azure Blob, and GCS.
- **Vector and raster tiles**: `VersatilesReader` returns the decompressed bytes of tiles, whatever their format. `VersatilesVectorTileStore` decodes Mapbox Vector Tiles to `VectorTile` objects, and `VersatilesRasterTileStore` decodes PNG, JPEG, and WebP tiles to `RenderedImage` objects.
- **Compression**: reads uncompressed, gzip, Brotli, and Zstandard archives.

## Core concepts

A Versatiles container has four parts:

1. **Header**: 66 bytes at the start of the file. It records the tile format, the compression, the zoom range, the bounding box, and the location of the other parts.
2. **Metadata**: a TileJSON document, compressed like the tiles.
3. **Blocks**: the tiles of one zoom level are grouped in blocks of up to 256 x 256 tiles. A block stores its tile bodies, followed by a tile index with the positions of the tiles inside the block.
4. **Block index**: one 33-byte entry per block, at the end of the file.

To read a tile, the reader finds its block in the block index, loads the tile index of that block, and reads the tile body. The block index is read once, when the reader opens the archive. Tile indexes stay in a cache, and the body of a tile takes a single range request.

Tiles are addressed with the XYZ scheme: zoom level, column from west to east, and row from north to south. Row 0 is the northernmost row, as in PMTiles and in web map clients.

The block index and the tile indexes are always compressed with Brotli. The metadata and the tile bodies use the compression declared in the header: none, gzip, Brotli, or Zstandard. The reader decompresses tiles before it returns them.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.versatiles</groupId>
    <artifactId>tileverse-versatiles</artifactId>
</dependency>
```

The version comes from `tileverse-bom`, as described in [Getting Started](../getting-started.md). Local files and HTTP work out of the box. S3, Azure and GCS URIs need `tileverse-storage-all`, or the matching `tileverse-storage-s3`, `tileverse-storage-azure` or `tileverse-storage-gcs` module, next to `tileverse-versatiles`.

## Usage

```java
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.versatiles.VersatilesHeader;
import io.tileverse.versatiles.VersatilesReader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

try (VersatilesReader reader = VersatilesReader.open(Path.of("data/map.versatiles"))) {
    VersatilesHeader header = reader.getHeader();
    System.out.println("Tile format: " + header.tileFormat());
    System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());

    // The buffer is ready to read, do not flip it
    Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(0, 0, 0));
}
```

`VersatilesReader.open(URI)` accepts the URIs supported by `tileverse-storage`, such as `s3://bucket/map.versatiles` or `https://example.com/map.versatiles`.

## Component view

![tileverse-versatiles components](../assets/images/storage/structurizr-VersatilesComponents.svg)

`VersatilesReader` is the low-level type. It reads the header, finds the block of a tile in the `BlockIndex`, finds the tile inside its block through the `TileIndexCache`, and returns the decompressed tile bytes. It can also stream them through an `IOFunction` supplied by the caller. `VersatilesVectorTileStore` and `VersatilesRasterTileStore` wrap a `VersatilesReader` and decode tiles to `VectorTile` and `RenderedImage` objects.

## Next steps

- [Read Versatiles archives](how-to/read-versatiles.md): open archives in cloud storage, read tiles, and decode them with the tile stores.
- [Format support](reference/format-support.md): tile formats, compression, exceptions, and caches.
