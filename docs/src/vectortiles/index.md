# Vector Tiles

A high-performance library for encoding and decoding [Mapbox Vector Tiles (MVT)](https://github.com/mapbox/vector-tile-spec) in Java. It bridges the gap between raw Protocol Buffers data and usable Java objects (JTS Geometries).

## Capabilities

- **MVT v2.1 Compliance**: Full support for layers, features, and values.
- **JTS Integration**: Direct conversion between MVT geometry commands and JTS `Geometry` objects.
- **Zero-Copy Decoding**: Optimized parsing strategies to minimize object allocation during reads.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.vectortiles</groupId>
    <artifactId>tileverse-vectortiles</artifactId>
</dependency>
```

## Usage

### Decoding

Convert raw MVT bytes (e.g., from a PMTiles archive or network request) into a structured Java object.

```java
import io.tileverse.vectortile.mvt.VectorTileCodec;

byte[] rawBytes = ...; // your MVT data

var codec = VectorTileCodec.getDefault();
var tile = codec.decode(rawBytes);

for (var layer : tile.getLayers()) {
    System.out.println("Layer: " + layer.getName());
    
    for (var feature : layer.getFeatures()) {
        // Access JTS Geometry directly
        org.locationtech.jts.geom.Geometry geom = feature.getGeometry();
        Map<String, Object> props = feature.getAttributes();
    }
}
```

### Encoding

Create MVTs from scratch using JTS geometries.

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;

var builder = VectorTileBuilder.create();
var layer = builder.layer("buildings");

layer.feature()
     .id(123)
     .geometry(jtsPolygon)
     .attribute("height", 50)
     .attribute("type", "residential");

byte[] encoded = codec.encode(builder.build());
```