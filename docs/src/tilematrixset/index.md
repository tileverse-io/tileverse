# Tile Matrix Set

A Java implementation of the OGC Two-Phase Tile Matrix Set (TMS) standard. This library handles the mathematical complexity of tile pyramids, coordinate systems, and grid definitions.

## Features

- **OGC Compliance**: Implements the data model for Tile Matrix Sets.
- **Standard Sets**: Includes built-in definitions for `WebMercatorQuad` and `WorldCRS84Quad`.
- **Math Utilities**: Helper functions for converting between bounding boxes, geographic coordinates, and tile indices.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.tilematrixset</groupId>
    <artifactId>tileverse-tilematrixset</artifactId>
</dependency>
```

## Usage

### Working with Coordinates

```java
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;

var tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
var matrix = tms.tileMatrix(12); // Zoom level 12

// Calculate which tiles cover a specific geographic area
var bbox = new BoundingBox2D(-74.0, 40.7, -73.9, 40.8); // NYC
var tileRange = matrix.getTilesIntersecting(bbox);

System.out.println("Tiles needed: " + tileRange.size());
for (var tile : tileRange) {
    System.out.printf("Fetch z=%d x=%d y=%d%n", 12, tile.x(), tile.y());
}
```