# Encoding Vector Tiles

`VectorTileBuilder` assembles a `VectorTile` from JTS `Geometry` instances; `VectorTileCodec.encode(...)` serializes the result to MVT bytes.

## Build a tile

```java
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import org.locationtech.jts.geom.Geometry;

VectorTileBuilder builder = new VectorTileBuilder();

builder.layer()
        .name("buildings")
        .feature()
            .id(123)
            .geometry(buildingPolygon)
            .attribute("height", 50)
            .attribute("type", "residential")
            .build()       // returns LayerBuilder
        .build();          // returns VectorTileBuilder

VectorTile tile = builder.build();
```

`LayerBuilder.feature()` opens a `FeatureBuilder`; chain `.id(...) / .geometry(...) / .attribute(...) / .attributes(Map)` then call `.build()` to attach the feature to its layer and return the `LayerBuilder` for the next feature.

## Shorthand for many features

When you have a stream of `(properties, geometry)` pairs (e.g. iterating a `SimpleFeatureCollection`), the `LayerBuilder.feature(Map, Geometry [, long id])` overloads skip the inner builder:

```java
features.forEach(simpleFeature ->
        roadsLayer.feature(simpleFeature.getProperties(), simpleFeature.getGeometry()));
```

## Encode to bytes

Three target shapes:

```java
VectorTileCodec codec = new VectorTileCodec();

byte[] bytes = codec.encode(tile);                       // heap byte[]
codec.encode(tile, OutputStream sink);                   // streaming
codec.encode(tile, ByteBuffer sink);                     // pre-sized buffer
```

The `OutputStream` overload is the right choice when you're piping straight to an HTTP response or writing into a PMTiles writer's tile slot: no intermediate heap copy.

## Configure encoding

`VectorTileBuilder` has tile-wide knobs that take effect at `build()` time:

```java
builder
    .setExtent(4096)                          // tile coordinate extent (default 4096)
    .setClipBuffer(64)                        // pixels of buffer around the tile bbox
    .setAutoIncrementIds(true)                // assign ids automatically when features omit them
    .setSimplificationDistanceTolerance(1.0)  // Douglas-Peucker tolerance in tile units
    .setUsePrecisionModelSnapping(true);      // snap to integer coordinates before encoding
```

These are tile-wide, not per-layer. Tune them once for the output dataset.

## Coordinates and units

The geometries you pass to the builder must already be in **tile coordinates**: origin at the tile's top-left, y growing downward, extent given by `setExtent(...)` (typically 4096). If your source is in a CRS (EPSG:3857, etc.), project to tile coordinates first using the matching `TileMatrixSet`. See [Tile Matrix Set](../../tilematrixset/index.md).

## See also

- [Decoding Vector Tiles](decode.md) for the reverse direction.
- [MVT v2.1 spec](https://github.com/mapbox/vector-tile-spec/tree/master/2.1) for the binary format.
