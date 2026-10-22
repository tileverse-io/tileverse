# Tile Pyramids and Ranges

`TileMatrixSet` is the top-level OGC TMS object, but its grid structure is stored as a separate `TilePyramid`. The pyramid can also be used on its own when you need grid arithmetic without spatial coordinates.

## TilePyramid

A pyramid is a collection of `TileRange` levels sharing a single `CornerOfOrigin`. Construct one through `TilePyramid.builder()` or with the `TilePyramid.of(...)` factories.

```java
import io.tileverse.tiling.pyramid.CornerOfOrigin;
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

`TileRange` describes a rectangular region of tiles at a single zoom level. It stores only the corner indices and is cheap to create, even for large pyramids.

```java
TileRange range = TileRange.of(0, 0, 1, 1, 10, CornerOfOrigin.TOP_LEFT);

range.count();                // 4
range.spanX();                // 2
range.spanY();                // 2
range.all().forEach(System.out::println); // streams the 4 TileIndex values
```

Ranges support set-style operations: `intersection`, `union`, `contains`, and traversal helpers `first()`, `last()`, `next(index)`, `prev(index)`. They are the building blocks of every spatial query exposed by `TileMatrix`.

### Sparse ranges

When `TileRange.union` or `TileMatrix.union` combines ranges into a shape that is not rectangular, the result is a sparse range. Code uses it through the same `TileRange` and `TileMatrix` methods as a rectangular range.

## CornerOfOrigin

Tile pyramids can index tiles from either the top-left (the OGC default and the convention used by most web maps and PMTiles) or the bottom-left (the older TMS/WMS-C convention). `TilePyramid.builder()`, `TilePyramid.of(...)` and `TileRange.of(...)` use `BOTTOM_LEFT` unless a corner is given.

```java
import io.tileverse.tiling.pyramid.CornerOfOrigin;

CornerOfOrigin origin = pyramid.cornerOfOrigin();
```

`TileMatrix` honours the origin of the pyramid when it converts between CRS coordinates and tile indices. Callers do not flip the Y axis themselves.
