# PMTiles

This module provides a pure Java implementation of the [PMTiles v3 specification](https://github.com/protomaps/PMTiles), a single-file archive format for cloud-optimized tiled data.

This library is focused on efficient, cloud-optimized reading of PMTiles archives. For creating archives, use one of the existing tools such as [`pmtiles`](https://github.com/protomaps/go-pmtiles) or [Tippecanoe](https://github.com/felt/tippecanoe).

## Features

- **Cloud-Optimized Reads**: Range-based access tuned for archives served from object storage.
- **Storage Agnostic**: Works with any [`RangeReader`](../storage/reference/rangereader.md) backend from `tileverse-storage` (S3, HTTP, Local File, Azure Blob, GCS).
- **Spatial Indexing**: Implements Hilbert curve indexing for O(log N) tile lookups.
- **Vector and Raster Tiles**: `PMTilesReader` returns the raw tile bytes for any tile type (MVT, PNG, JPEG, WebP, AVIF). For decoded results, the higher-level `PMTilesVectorTileStore` returns parsed `VectorTile` objects and `PMTilesRasterTileStore` returns decoded `java.awt.image.RenderedImage`s. WebP decoding is enabled by default through a bundled `ImageIO` plugin.

## Core concepts

A PMTiles archive contains three regions:

1. **Header**: tileset metadata (bounds, zoom levels, tile format, compression).
2. **Directory**: spatial index built on the Hilbert space-filling curve.
3. **Tiles**: the compressed tile bodies.

Tiles are addressed with the standard XYZ scheme: zoom level, column (west to east), and row (top to bottom in PMTiles' XYZ-with-top-left convention). The `tileType` byte in the header tells you whether the bodies are MVT, PNG, JPEG, WebP, or AVIF; `PMTilesHeader.tileMimeType()` and `isRasterTileType()` give a convenient view of the same.

PMTiles archives can hold either Mapbox Vector Tiles or raster tiles in any of those formats. `PMTilesReader.getTile(...)` returns raw bytes for either case; the high-level `PMTilesVectorTileStore` and `PMTilesRasterTileStore` wrappers decode on the fly to `VectorTile` and `RenderedImage` respectively. WebP decoding is bundled - no extra ImageIO plugin needed. See the [Tile Stores Reference](reference/tile-stores.md) for the API surface.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.pmtiles</groupId>
    <artifactId>tileverse-pmtiles</artifactId>
</dependency>
```

## Usage

The `PMTilesReader` accepts either a `RangeReader` or a `Supplier<SeekableByteChannel>`. The `RangeReader` can provide this via its `asByteChannel()` method.

```java
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import java.nio.file.Path;

try (PMTilesReader reader = PMTilesReader.open(Path.of("data/map.pmtiles").toUri())) {
    // Get metadata
    PMTilesHeader header = reader.getHeader();
    System.out.println("Min Zoom: " + header.minZoom());

    // Fetch tile (z, x, y)
    reader.getTile(0, 0, 0).ifPresent(buffer -> {
        buffer.flip(); // Important: flip the buffer before reading
        // Process tile bytes...
    });
}
```

## Component view

![tileverse-pmtiles components](../assets/images/storage/structurizr-PMTilesComponents.svg)

`PMTilesReader` is the low-level type: it reads the header, walks the directory using `HilbertCurve` for index lookup, and returns raw tile bytes (or streams them through a caller-supplied `IOFunction`). `PMTilesVectorTileStore` and `PMTilesRasterTileStore` are higher-level wrappers built on `PMTilesReader` that decode tiles to `VectorTile` and `RenderedImage` respectively. See the [Tile Stores Reference](reference/tile-stores.md) for the full API surface.
