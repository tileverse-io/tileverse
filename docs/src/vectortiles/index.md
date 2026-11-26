# Vector Tiles

This module encodes and decodes [Mapbox Vector Tiles](https://github.com/mapbox/vector-tile-spec) (MVT), version 2.1. It converts between the protobuf encoding and a `VectorTile` model with JTS geometries.

## Core concepts

A vector tile holds named layers, and a layer holds features. A feature has an optional id, attributes, and a geometry. Geometries are in tile coordinates: the origin is the top-left corner of the tile, y grows downward, and the extent of the layer, usually 4096, spans the tile.

Decoding a tile copies its bytes, indexes its layers and features, and decodes the geometry commands of each feature into packed coordinates. A feature builds its JTS geometry and decodes its attributes when asked.

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

## Performance

A decoded tile holds 2.2 to 3.1 times its encoded size, and 8% to 75% less heap than the same tile decoded by [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) 1.4.2.

Against java-vector-tile 1.4.2, on the benchmark tiles:

- Decoding a tile takes between 62% less time and 3% more time.
- Decoding a tile and reading its features is 1.5 to 3.6 times as fast.
- Reading the features of a decoded tile is 1.5 to 10.3 times as fast.

Reading a decoded tile is a repeated cost: the PMTiles and VersaTiles vector tile stores keep decoded tiles in a cache, and a map drawing several layers reads a cached tile once per layer.

The gain depends on the tile and on the processor. The benchmarks ran on an Apple M4 Max, an AMD Ryzen 9 7950X and an Intel Xeon Gold 6138. The [benchmarks README](https://github.com/tileverse-io/tileverse/blob/main/internal/vectortiles-benchmarks/README.md) describes the tiles and gives the results.

## Component view

![tileverse-vectortiles components](../assets/images/storage/structurizr-VectorTilesComponents.svg)

`VectorTileCodec` encodes and decodes between MVT bytes and the `VectorTile` model. `VectorTileBuilder` builds `VectorTile` instances from JTS geometries.

## Next steps

- [Encode vector tiles](how-to/encode.md): build tiles, configure clipping and simplification, and write the bytes.
- [Decode vector tiles](how-to/decode.md): read layers and features, and filter features without building their geometries.
