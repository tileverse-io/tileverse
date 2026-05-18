# Vector Tiles Quick Start

Get started with Tileverse Vector Tiles quickly.

## Decoding MVT

```java
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.model.VectorTile;

VectorTileCodec codec = new VectorTileCodec();
byte[] mvtBytes = ...; // Load from file, network, etc.

VectorTile tile = codec.decode(mvtBytes);

// Iterate layers and features
for (VectorTile.Layer layer : tile.getLayers()) {
    System.out.println("Layer: " + layer.getName());
    for (VectorTile.Layer.Feature feature : layer.getFeatures()) {
        System.out.println("  Feature ID: " + feature.getId());
        System.out.println("  Geometry: " + feature.getGeometry());
        System.out.println("  Attributes: " + feature.getAttributes());
    }
}
```

## Encoding MVT

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;

VectorTileBuilder builder = new VectorTileBuilder();
builder.layer()
        .name("poi")
        .feature()
            .geometry(point)
            .attributes(Map.of("name", "Restaurant"))
            .build();

VectorTile tile = builder.build();
byte[] mvtBytes = codec.encode(tile);
```

## Next Steps

- [Encoding Tiles](how-to/encode.md)
- [Decoding Tiles](how-to/decode.md)
