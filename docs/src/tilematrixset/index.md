# Tile Matrix Set

A Java implementation of the [OGC Two Dimensional Tile Matrix Set (17-083r2)](https://docs.ogc.org/is/17-083r2/17-083r2.html) standard. The library handles the mathematical model of tile pyramids, coordinate systems and grid definitions, and provides JSON and XML readers and writers for the OGC encodings defined in Annex C and Annex D.

## Features

- **OGC TMS 2D data model**: identifier, optional title/abstract/keywords, supportedCRS URI, well-known scale set, tile matrices.
- **Scale algebra**: `scaleDenominator` derived from CRS units via the standard 0.28 mm rendering pixel size.
- **Standard sets**: bundled definitions for `WebMercatorQuad`, `WorldCRS84Quad` and legacy variants.
- **Limits and links**: `TileMatrixLimits`, `TileMatrixSetLimits`, `TileMatrixSetLink`, `VariableMatrixWidth`.
- **Encodings**: round-trip JSON (Annex C) and XML (Annex D) through `TileMatrixSetIO`.

## Core types

| Type | Role |
| :--- | :--- |
| `TileMatrixSet` | Top-level OGC TMS object: identifier, CRS, extent, list of matrices. |
| `TileMatrix` | A single zoom level: tile width / height in pixels, scale denominator, top-left corner, matrix dimensions. |
| `TilePyramid` | Grid arithmetic alone (no CRS): a sequence of `TileRange` levels with a shared `CornerOfOrigin`. |
| `TileRange` | A rectangular tile-index region at one zoom level. |
| `Tile` | A single tile: `TileIndex` (z, x, y) + the bounding box in CRS units. |
| `TileIndex` | The three integers `(z, x, y)`. |
| `BoundingBox2D` | Axis-aligned extent in CRS units. |
| `TileMatrixSetIO` | Read / write JSON (OGC Annex C) and XML (OGC Annex D). |
| `DefaultTileMatrixSets` | Static constants for `WebMercatorQuad`, `WorldCRS84Quad`, and legacy variants. |

## Installation

```xml
<dependency>
    <groupId>io.tileverse.tilematrixset</groupId>
    <artifactId>tileverse-tilematrixset</artifactId>
</dependency>
```

## Usage

### Looking up tiles for a bounding box

```java
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;

TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
TileMatrix matrix = tms.getTileMatrix(12);

// Bounding box in the TMS CRS (here EPSG:3857 meters)
BoundingBox2D nyc = BoundingBox2D.extent(-8238000, 4965000, -8230000, 4975000);

matrix.extentToRange(nyc).ifPresent(range -> {
    System.out.println("Tiles needed: " + range.count());
    range.all().forEach(idx ->
        System.out.printf("Fetch z=%d x=%d y=%d%n", idx.z(), idx.x(), idx.y()));
});
```

See the [Tutorial](tutorial.md) for a deeper walkthrough and the [OGC Compliance](reference/ogc-compliance.md) page for the JSON and XML encodings.

## Component view

![tileverse-tilematrixset components](../assets/images/storage/structurizr-TileMatrixSetComponents.svg)

`TileMatrixSet` is the top-level OGC TMS object; `TilePyramid` carries the grid arithmetic alone (no CRS). `DefaultTileMatrixSets` ships bundled definitions of `WebMercatorQuad`, `WorldCRS84Quad`, and legacy variants. `TileMatrixSetIO` handles the OGC Annex C (JSON) and Annex D (XML) encodings.
