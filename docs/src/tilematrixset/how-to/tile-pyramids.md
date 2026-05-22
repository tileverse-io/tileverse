# Tile Pyramids and Ranges

`TileMatrixSet` is the top-level OGC TMS object, but its grid structure is stored as a separate `TilePyramid`. The pyramid can also be used on its own when you need grid arithmetic without spatial coordinates.

## TilePyramid

A pyramid is a collection of `TileRange` levels sharing a single `CornerOfOrigin`. Construct one through `TilePyramid.builder()` or with the `TilePyramid.of(...)` factories.

```java
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;

TilePyramid pyramid = TilePyramid.builder()
        .cornerOfOrigin(CornerOfOrigin.TOP_LEFT)
        .level(TileRange.of(0, 0, 0, 0, 0, CornerOfOrigin.TOP_LEFT)) // z=0
        .level(TileRange.of(0, 0, 1, 1, 1, CornerOfOrigin.TOP_LEFT)) // z=1
        .level(TileRange.of(0, 0, 3, 3, 2, CornerOfOrigin.TOP_LEFT)) // z=2
        .build();

int min = pyramid.minZoomLevel();
int max = pyramid.maxZoomLevel();
TileRange level1 = pyramid.tileRange(1);

TilePyramid trimmed = pyramid.subset(1, 2); // keeps levels 1 and 2 only
```

## TileRange

`TileRange` describes a rectangular region of tiles at a single zoom level. It only stores the corner indices, so it is cheap to create even for large pyramids.

```java
TileRange range = TileRange.of(0, 0, 1, 1, 10, CornerOfOrigin.TOP_LEFT);

range.count();                // 4
range.spanX();                // 2
range.spanY();                // 2
range.all().forEach(System.out::println); // streams the 4 TileIndex values
```

Ranges support set-style operations: `intersection`, `union`, `contains`, and traversal helpers `first()`, `last()`, `next(index)`, `prev(index)`. They are the building blocks of every spatial query exposed by `TileMatrix`.

### Sparse ranges

When a region's tile footprint is not rectangular (for example a TileMatrixSet narrowed by a `TileMatrixSetLimits` or a coverage clipped to a polygon), the implementation uses a sparse range internally. The public API of `TileMatrix` already takes care of this; user code interacts with it through `TileMatrix.intersection(bbox)` or `extentToRange(bbox)`.

## CornerOfOrigin

Tile pyramids can index tiles from either the top-left (the OGC default and the convention used by most web maps and PMTiles) or the bottom-left (the older TMS/WMS-C convention).

```java
import io.tileverse.tiling.common.CornerOfOrigin;

CornerOfOrigin origin = pyramid.cornerOfOrigin();
```

`TileMatrix` honours the pyramid's origin when converting between CRS coordinates and tile indices, so callers do not need to flip Y axes themselves.
