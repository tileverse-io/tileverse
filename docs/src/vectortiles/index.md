# Vector Tiles

This module encodes and decodes [Mapbox Vector Tiles](https://github.com/mapbox/vector-tile-spec) (MVT), version 2.1. It converts between the protobuf encoding and a `VectorTile` model with JTS geometries.

## Core concepts

A vector tile holds named layers, and a layer holds features. A feature has an optional id, attributes, and a geometry. Geometries are in tile coordinates: the origin is the top-left corner of the tile, y grows downward, and the extent of the layer, usually 4096, spans the tile.

Decoding is lazy. A feature decodes its geometry and its attributes on first access.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.vectortiles</groupId>
    <artifactId>tileverse-vectortiles</artifactId>
</dependency>
```

The version comes from `tileverse-bom`, as described in [Getting Started](../getting-started.md).

## Usage

Decode MVT bytes, for example a tile read from a PMTiles archive:

```java
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

VectorTileCodec codec = new VectorTileCodec();
VectorTile tile = codec.decode(mvtBytes);

for (VectorTile.Layer layer : tile.getLayers()) {
    System.out.println("Layer: " + layer.getName());
    layer.getFeatures().forEach(feature -> {
        Geometry geometry = feature.getGeometry();
        Map<String, Object> attributes = feature.getAttributes();
    });
}
```

Build a tile from JTS geometries and encode it:

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;

VectorTileBuilder builder = new VectorTileBuilder();
builder.layer()
        .name("buildings")
        .feature()
            .id(123)
            .geometry(polygon)       // in tile coordinates
            .attribute("height", 50)
            .build()                 // returns the LayerBuilder
        .build();                    // adds the layer, returns the VectorTileBuilder

byte[] encoded = codec.encode(builder.build());
```

## Component view

![tileverse-vectortiles components](../assets/images/storage/structurizr-VectorTilesComponents.svg)

`VectorTileCodec` encodes and decodes between MVT bytes and the `VectorTile` model. `VectorTileBuilder` builds `VectorTile` instances from JTS geometries.

## Next steps

- [Encode vector tiles](how-to/encode.md): build tiles, configure clipping and simplification, and write the bytes.
- [Decode vector tiles](how-to/decode.md): read layers and features, and filter features without decoding their geometries.
