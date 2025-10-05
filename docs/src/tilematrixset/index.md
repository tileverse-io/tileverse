# Tileverse Tile Matrix Set

Generic object model for defining tile pyramids and tiling schemes.

## Overview

Tileverse Tile Matrix Set provides Java implementations of tile pyramid concepts and OGC Tile Matrix Set standard. It includes models for working with tile coordinates, tile ranges, and standard tiling schemes like Web Mercator.

## Features

- **Standard tile matrix sets**: Web Mercator, WGS84, and custom definitions
- **Coordinate transformations**: Convert between geographic and tile coordinates
- **Flexible tiling schemes**: Support for various CRS and tile configurations
- **Tile pyramid models**: Generic abstractions for tiled data structures

## Quick Start

```java
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.pyramid.TileIndex;

// Use a standard tile matrix set
TileMatrixSet webMercator = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

// Get tile matrix for zoom level 10
TileMatrix matrix = webMercator.tileMatrix(10);

// Convert geographic bounds to tile range
BoundingBox2D bbox = new BoundingBox2D(-122.5, 37.7, -122.3, 37.9);
TileRange tiles = matrix.getTilesIntersecting(bbox);

System.out.printf("Tiles at zoom %d: %s%n", 10, tiles);
```

## Use Cases

- **Tile coordinate calculations**: Convert between geographic and tile coordinates
- **Tile server development**: Implement standard tiling schemes
- **Spatial indexing**: Use tile coordinates for data organization
- **Multi-resolution data**: Work with pyramidal tile structures

## Getting Started

<div class="grid cards" markdown>

-   :material-rocket-launch: **User Guide**

    ---

    Learn how to work with tile pyramids and matrix sets.

    [:octicons-arrow-right-24: User Guide](user-guide/index.md)

</div>

## Related Modules

- **[PMTiles](../pmtiles/index.md)**: Uses tile coordinates from matrix sets
- **[Vector Tiles](../vectortiles/index.md)**: Often organized in tile pyramids

## Requirements

- **Java 17+**: Minimum runtime version

## License

Licensed under the Apache License, Version 2.0.
