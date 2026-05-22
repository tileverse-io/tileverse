# Decoding Vector Tiles

`VectorTileCodec` decodes raw [Mapbox Vector Tile](https://github.com/mapbox/vector-tile-spec) bytes into a `VectorTile` model with JTS `Geometry` instances attached to each feature. The codec is stateless and thread-safe; instantiate once and reuse.

## Pick an input shape

`VectorTileCodec` accepts three inputs:

```java
VectorTileCodec codec = new VectorTileCodec();

VectorTile fromBytes  = codec.decode(byte[] data);
VectorTile fromBuffer = codec.decode(ByteBuffer data);  // reads from data.position() to data.limit()
VectorTile fromStream = codec.decode(InputStream in);   // reads to EOF; closes? no - caller closes
```

Prefer `decode(InputStream)` when the source is already a stream (HTTP body, channel-backed input). It avoids materializing the encoded bytes in a transient array.

## Walk layers and features

```java
VectorTile tile = codec.decode(rawBytes);

for (VectorTile.Layer layer : tile.getLayers()) {
    System.out.println("Layer: " + layer.getName());
    System.out.println("Extent: " + layer.getExtent());  // tile coordinate extent, usually 4096

    for (VectorTile.Layer.Feature feature : layer.getFeatures()) {
        long id = feature.getId();
        Geometry geom = feature.getGeometry();          // JTS Geometry in tile coordinates
        Map<String, Object> props = feature.getAttributes();
    }
}
```

`VectorTile.Layer.Feature.getGeometry()` returns a JTS `Geometry` whose coordinates are in the layer's own tile extent (`getExtent()`, typically `4096`). Origin is the top-left of the tile, y grows downward. To project into a real-world CRS you need the tile's `(z, x, y)` and the matching `TileMatrixSet` (see [Tile Matrix Set](../../tilematrixset/index.md)).

## Filter features without materializing geometries

Geometry decoding is the expensive step in MVT parsing. When you only need a subset of features, push a predicate into the layer so geometry decoding only runs for the matches:

```java
import java.util.function.Predicate;

Predicate<VectorTile.Layer.Feature> roadsOnly =
        f -> "primary".equals(f.getAttributes().get("type"));

tile.getFeatures("roads", roadsOnly, VectorTileCodec.newGeometryReader())
        .forEach(f -> process(f.getGeometry()));
```

The two-arg `Layer.getFeatures(Predicate)` and three-arg `Layer.getFeatures(Predicate, GeometryReader)` overloads let you skip the geometry decode for features that don't match.

## Cross-layer queries

Convenience methods on `VectorTile` route to the right layer:

```java
tile.getFeatures("roads").forEach(...);                  // all features of one layer
tile.getFeatures().forEach(...);                          // every feature of every layer
tile.getLayer("buildings").ifPresent(layer -> ...);       // null-safe layer lookup
```

## Error handling

`decode(...)` throws `IOException` on malformed input. The codec validates the protobuf envelope and the MVT geometry command stream; corrupt tiles surface as a wrapped exception rather than silent partial decoding.

```java
try {
    VectorTile tile = codec.decode(bytes);
} catch (IOException e) {
    log.warn("Skipping malformed tile {}/{}/{}: {}", z, x, y, e.getMessage());
}
```

## See also

- [Encoding Vector Tiles](encode.md) for the reverse direction.
- [Tile Stores Reference](../../pmtiles/reference/tile-stores.md) for `VectorTileStore`, which decodes tiles served from a PMTiles archive and gives you `VectorTile` directly.
