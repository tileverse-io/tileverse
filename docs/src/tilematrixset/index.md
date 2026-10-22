# Tile Matrix Set

This module implements the [OGC Two Dimensional Tile Matrix Set (17-083r2)](https://docs.ogc.org/is/17-083r2/17-083r2.html) standard. It maps coordinates to tiles and back, describes tile pyramids, and reads and writes the JSON and XML encodings of the standard.

## Core concepts

A tile matrix set has one tile matrix per zoom level. A tile matrix is a grid of tiles over the extent of the set, in the units of its CRS. The resolution of a level gives the CRS units per pixel, and the scale denominator follows from it with the standard rendering pixel size of 0.28 mm.

| Type | Role |
| :--- | :--- |
| `TileMatrixSet` | The top-level OGC object: identifier, CRS, extent, and one `TileMatrix` per zoom level. |
| `TileMatrix` | One zoom level: tile size in pixels, resolution, scale denominator, top-left corner, and matrix dimensions. |
| `TilePyramid` | The grid arithmetic alone, without a CRS: one `TileRange` per level, with a shared `CornerOfOrigin`. |
| `TileRange` | A rectangular range of tile indices at one zoom level. |
| `Tile` | One tile: its `TileIndex` and its extent in CRS units. |
| `TileIndex` | The zoom level `z` (int), the column `x` and the row `y` (long). |
| `BoundingBox2D` | An axis-aligned extent in CRS units. |
| `DefaultTileMatrixSets` | Constants for `WebMercatorQuad`, `WorldCRS84Quad`, and older GeoWebCache-style sets. |
| `TileMatrixSetIO` | Reads and writes the JSON (Annex C) and XML (Annex D) encodings. |

The module also models the limits and links of the standard: `TileMatrixLimits`, `TileMatrixSetLimits`, `TileMatrixSetLink`, and `VariableMatrixWidth`.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.tilematrixset</groupId>
    <artifactId>tileverse-tilematrixset</artifactId>
</dependency>
```

The version comes from `tileverse-bom`, as described in [Getting Started](../getting-started.md).

## Usage

Find the tiles covering a bounding box:

```java
import io.tileverse.geom.BoundingBox2D;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;

TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
TileMatrix matrix = tms.getTileMatrix(12);

// Bounding box in the CRS of the set, here EPSG:3857 meters
BoundingBox2D nyc = BoundingBox2D.extent(-8238000, 4965000, -8230000, 4975000);

matrix.extentToRange(nyc).ifPresent(range -> {
    System.out.println("Tiles needed: " + range.count());
    range.all().forEach(idx ->
        System.out.printf("Fetch z=%d x=%d y=%d%n", idx.z(), idx.x(), idx.y()));
});
```

## Component view

![tileverse-tilematrixset components](../assets/images/storage/structurizr-TileMatrixSetComponents.svg)

A `TileMatrixSet` delegates the grid arithmetic to its `TilePyramid`, and adds the CRS, the extent, and the resolutions.

## Next steps

- [Tutorial](tutorial.md): pick a standard set, map a coordinate to a tile, iterate over an area, and build a custom set.
- [Tile pyramids and ranges](how-to/tile-pyramids.md): grid arithmetic without a CRS.
- [Standard tile matrix sets](how-to/standard-sets.md): the bundled sets.
- [OGC compliance and encodings](reference/ogc-compliance.md): the mapping to the standard, and the JSON and XML encodings.
