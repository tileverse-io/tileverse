# PMTiles

This module reads [PMTiles](https://github.com/protomaps/PMTiles) version 3 archives, a single-file format for map tiles. The reader fetches only the bytes it needs, with range requests. This suits archives served from object storage or HTTP.

The module reads archives and does not write them. To create an archive, use a tool such as [`pmtiles`](https://github.com/protomaps/go-pmtiles) or [Tippecanoe](https://github.com/felt/tippecanoe).

## Core concepts

A PMTiles archive has four parts:

1. **Header**: 127 bytes at the start of the file. It records the tile type, the compression of the tiles and of the directories, the zoom range, the bounds, and the location of the other parts.
2. **Directories**: the index of the tiles. A root directory sits near the start of the file, and large archives add leaf directories.
3. **Metadata**: a JSON document.
4. **Tile data**: the tile bodies.

To read a tile, the reader turns its position into a tile id along a Hilbert curve, finds the id in the directories, and reads the tile body. Parsed directories stay in a cache.

Tiles are addressed with the XYZ scheme: zoom level, column from west to east, and row from north to south. The header records the tile type: MVT, PNG, JPEG, WebP, AVIF, MLT, or unknown. The reader returns the tiles of any type, after removing the tile compression of the archive. The tile stores decode MVT tiles to `VectorTile` objects, and PNG, JPEG, WebP and AVIF tiles to `RenderedImage` objects.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.pmtiles</groupId>
    <artifactId>tileverse-pmtiles</artifactId>
</dependency>
```

The version comes from `tileverse-bom`, as described in [Getting Started](../getting-started.md). Local files and HTTP work out of the box. S3, Azure and GCS URIs need `tileverse-storage-all`, or the matching `tileverse-storage-s3`, `tileverse-storage-azure` or `tileverse-storage-gcs` module, next to `tileverse-pmtiles`.

## Usage

```java
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.tiling.pyramid.TileIndex;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

try (PMTilesReader reader = PMTilesReader.open(Path.of("data/map.pmtiles").toUri())) {
    PMTilesHeader header = reader.getHeader();
    System.out.println("Tile type: " + header.tileMimeType());
    System.out.println("Zoom levels: " + header.minZoom() + " to " + header.maxZoom());

    // The buffer is ready to read, do not flip it
    Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(0, 0, 0));
}
```

`PMTilesReader.open(URI)` accepts the URIs supported by `tileverse-storage`, such as `s3://bucket/map.pmtiles` or `https://example.com/map.pmtiles`.

## Component view

![tileverse-pmtiles components](../assets/images/storage/structurizr-PMTilesComponents.svg)

`PMTilesReader` is the low-level type. It reads the header, finds a tile in the directories through the `HilbertCurve`, and returns the tile bytes. It can also stream them through an `IOFunction` supplied by the caller. `PMTilesVectorTileStore` and `PMTilesRasterTileStore` wrap a `PMTilesReader` and decode tiles to `VectorTile` and `RenderedImage` objects.

## Next steps

- [Read PMTiles archives](how-to/read-pmtiles.md): open archives in cloud storage, read tiles, and decode them with the tile stores.
- [Tile stores](reference/tile-stores.md): the `TileStore` API, the PMTiles stores, and the TileJSON model.
