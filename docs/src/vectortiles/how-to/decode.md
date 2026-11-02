# Decoding Vector Tiles

`VectorTileCodec` decodes raw [Mapbox Vector Tile](https://github.com/mapbox/vector-tile-spec) bytes into a `VectorTile` model with JTS `Geometry` instances attached to each feature. A codec is configured when created and is thread-safe. Create it once and reuse it.

## Pick an input shape

`VectorTileCodec` accepts three inputs:

```java
VectorTileCodec codec = new VectorTileCodec();

VectorTile fromBytes  = codec.decode(byte[] data);
VectorTile fromBuffer = codec.decode(ByteBuffer data);  // reads from data.position() to data.limit()
VectorTile fromStream = codec.decode(InputStream in);   // reads to the end; the caller closes the stream
```

The tile keeps a copy of the encoded bytes. `decode(byte[])` and `decode(ByteBuffer)` copy the bytes once. `decode(InputStream)` reads the stream into a pooled array, then copies the bytes of the tile. Prefer `decode(byte[])` or `decode(ByteBuffer)` when the bytes are at hand, and `decode(InputStream)` when the source is a stream (HTTP body, channel-backed input).

## Walk layers and features

```java
VectorTile tile = codec.decode(rawBytes);

for (VectorTile.Layer layer : tile.getLayers()) {
    System.out.println("Layer: " + layer.getName());
    System.out.println("Extent: " + layer.getExtent());  // tile coordinate extent, usually 4096

    layer.getFeatures().forEach(feature -> {
        long id = feature.getId();
        Geometry geom = feature.getGeometry();          // JTS Geometry in tile coordinates
        Map<String, Object> props = feature.getAttributes();
    });
}
```

`VectorTile.Layer.Feature.getGeometry()` returns a JTS `Geometry` whose coordinates are in the layer's own tile extent (`getExtent()`, typically `4096`). Origin is the top-left of the tile, y grows downward. To project into a real-world CRS you need the tile's `(z, x, y)` and the matching `TileMatrixSet` (see [Tile Matrix Set](../../tilematrixset/index.md)).

## Filter features without materializing geometries

Decoding a tile decodes the geometry commands of each feature. A feature builds its JTS geometry on the first call to `getGeometry()`. A predicate reading only attributes builds no JTS geometry for the rejected features:

```java
import java.util.function.Predicate;

Predicate<VectorTile.Layer.Feature> roadsOnly =
        f -> "primary".equals(f.getAttributes().get("type"));

tile.getFeatures("roads", roadsOnly, VectorTileCodec.newGeometryReader())
        .forEach(f -> process(f.getGeometry()));
```

The `Layer.getFeatures(Predicate)` and `Layer.getFeatures(Predicate, GeometryReader)` overloads do the same on a layer you already hold.

## Cross-layer queries

Convenience methods on `VectorTile` route to the right layer:

```java
tile.getFeatures("roads").forEach(...);                  // all features of one layer
tile.getFeatures().forEach(...);                          // every feature of every layer
tile.getLayer("buildings").ifPresent(layer -> ...);       // null-safe layer lookup
```

## Error handling

`decode(...)` throws `IOException` when the bytes are not a valid MVT protobuf message. The geometries decode with the tile. A malformed geometry reads as the empty geometry of its type, and its feature keeps its id and its attributes. A codec from `VectorTileCodec.failingOnMalformedGeometry()` throws an `IOException` from `decode(...)` instead.

```java
try {
    VectorTile tile = codec.decode(bytes);
} catch (IOException e) {
    log.warn("Skipping malformed tile {}/{}/{}: {}", z, x, y, e.getMessage());
}
```

## See also

- [Encoding Vector Tiles](encode.md) for the reverse direction.
- [Tile Stores Reference](../../pmtiles/reference/tile-stores.md) for `PMTilesVectorTileStore`, which decodes tiles served from a PMTiles archive and gives you `VectorTile` directly.
