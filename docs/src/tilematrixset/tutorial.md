# Quick Start

This guide walks through the everyday operations of `tileverse-tilematrixset`: selecting a tile matrix set, mapping coordinates to tiles, iterating over a region, and building a custom set.

## 1. Pick a standard set

Standard sets are available as static constants on `DefaultTileMatrixSets`. Each one carries an OGC `identifier`, a `supportedCRS` URI, and (where applicable) a `wellKnownScaleSet` URI.

```java
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrixSet;

TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

System.out.println(tms.identifier());        // "WebMercatorQuad"
System.out.println(tms.supportedCRS());      // http://www.opengis.net/def/crs/EPSG/0/3857
System.out.println(tms.wellKnownScaleSet()); // Optional[http://...GoogleMapsCompatible]
```

## 2. Map a coordinate to a tile

`coordinateToTile` accepts a coordinate in the CRS of the matrix set and returns the tile that contains it. Inputs must already be in the target CRS; use a projection library if you need to convert from longitude/latitude.

```java
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;

TileMatrixSet tms = DefaultTileMatrixSets.WORLD_CRS84_QUAD;

Coordinate london = new Coordinate(-0.1276, 51.5074); // lon/lat in EPSG:4326
TileIndex tile = tms.coordinateToTile(london, 10);

System.out.printf("Tile z=%d x=%d y=%d%n", tile.z(), tile.x(), tile.y());
```

## 3. Iterate tiles intersecting an area

`TileMatrix.intersection(bbox)` returns a new (sparse) matrix whose `tiles()` stream only emits tiles overlapping the bounding box. `extentToRange(bbox)` returns the raw tile-index range when the matrix is fully populated.

```java
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.TileMatrix;

TileMatrix matrixZ12 = tms.getTileMatrix(12);
BoundingBox2D area = BoundingBox2D.extent(-1.0, 50.0, 1.0, 52.0);

matrixZ12.intersection(area).ifPresent(view ->
    view.tiles().forEach(tile ->
        System.out.println("Processing " + tile.tileIndex())));
```

## 4. Build a custom set

The builder requires identifier, CRS, tile pyramid, extent, and resolutions. Optional OGC metadata (title, abstract, keywords, well-known scale set) goes through fluent setters.

```java
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;

double[] resolutions = {0.703125, 0.3515625, 0.17578125};
TilePyramid pyramid = TilePyramid.builder()
        .cornerOfOrigin(CornerOfOrigin.TOP_LEFT)
        .level(TileRange.of(0, 0, 1, 0, 0, CornerOfOrigin.TOP_LEFT))
        .level(TileRange.of(0, 0, 3, 1, 1, CornerOfOrigin.TOP_LEFT))
        .level(TileRange.of(0, 0, 7, 3, 2, CornerOfOrigin.TOP_LEFT))
        .build();

TileMatrixSet custom = TileMatrixSet.builder()
        .identifier("MyCustomSet")
        .title("Custom 4326 set, zooms 0-2")
        .crs("EPSG:4326")
        .tilePyramid(pyramid)
        .tileSize(256, 256)
        .extent(BoundingBox2D.extent(-180, -90, 180, 90))
        .resolutions(resolutions)
        .build();
```
