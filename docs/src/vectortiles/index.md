# Tileverse Vector Tiles

Encoding and decoding Mapbox Vector Tiles (MVT) in Java.

## Overview

Tileverse Vector Tiles is a Java library for working with Mapbox Vector Tiles (MVT), a compact binary format for storing and transmitting vector data in tiles. The library provides efficient encoding and decoding of MVT data, with full support for the MVT specification.

## What are Vector Tiles?

Vector tiles are a method of delivering map data where the geometry is transmitted as vectors (points, lines, polygons) rather than pre-rendered images. This provides several advantages:

- **Smaller file sizes**: Vector data is more compact than raster images
- **Dynamic styling**: Style vector data on the client side
- **Better scalability**: Adapt to different screen resolutions
- **Interactive features**: Query and interact with individual features
- **Efficient updates**: Update styling without re-generating tiles

## Features

- **MVT format encoding/decoding**: Full support for Mapbox Vector Tiles v2 specification
- **Protocol Buffers**: Efficient binary serialization
- **Geometry transformations**: Convert between coordinate systems
- **Layer and feature manipulation**: Work with MVT structure programmatically
- **JTS integration**: Use familiar JTS Geometry types
- **High performance**: Optimized for server-side tile generation

## Quick Start

### Decoding Vector Tiles

```java
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.model.VectorTile;
import org.locationtech.jts.geom.Geometry;

// Create a codec
VectorTileCodec codec = VectorTileCodec.getDefault();

// Decode MVT bytes
byte[] mvtBytes = ...; // from file, network, PMTiles, etc.
VectorTile tile = codec.decode(mvtBytes);

// Access layers
for (VectorTile.Layer layer : tile.getLayers()) {
    System.out.printf("Layer: %s (%d features)%n",
        layer.getName(), layer.getFeatures().size());

    // Access features
    for (VectorTile.Layer.Feature feature : layer.getFeatures()) {
        Geometry geometry = feature.getGeometry();
        Map<String, Object> attributes = feature.getAttributes();

        // Process feature...
    }
}
```

### Encoding Vector Tiles

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import io.tileverse.vectortile.mvt.LayerBuilder;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

// Create a builder
VectorTileBuilder tileBuilder = VectorTileBuilder.create()
    .setExtent(4096);  // Standard tile extent

// Add a layer
LayerBuilder layerBuilder = tileBuilder.layer("cities");

// Add features
GeometryFactory gf = new GeometryFactory();
Point point = gf.createPoint(new Coordinate(100, 200));

layerBuilder.feature()
    .id(1)
    .geometry(point)
    .attributes(Map.of(
        "name", "San Francisco",
        "population", 800000
    ));

// Build and encode
VectorTile tile = tileBuilder.build();
byte[] mvtBytes = codec.encode(tile);
```

## Use Cases

- **Tile server development**: Generate MVT tiles on-the-fly
- **Data processing**: Convert between vector formats
- **Map rendering**: Decode MVT for client-side rendering
- **Spatial analysis**: Extract and analyze vector data from tiles
- **PMTiles integration**: Work with vector tile content in PMTiles archives

## Performance

- **Efficient Protocol Buffers encoding/decoding**
- **Memory-efficient streaming** for large tile sets
- **Optimized geometry transformations**
- **Thread-safe operations** for concurrent processing

## Getting Started

<div class="grid cards" markdown>

-   :material-rocket-launch: **User Guide**

    ---

    Learn how to encode and decode vector tiles.

    [:octicons-arrow-right-24: User Guide](user-guide/index.md)

-   :material-book-open: **MVT Specification**

    ---

    Understand the Mapbox Vector Tiles format.

    [:octicons-arrow-right-24: Specification](https://github.com/mapbox/vector-tile-spec)

</div>

## Related Modules

This library works together with other Tileverse modules:

- **[PMTiles](../pmtiles/index.md)**: Store vector tiles in PMTiles archives
- **[Tile Matrix Set](../tilematrixset/index.md)**: Work with tile coordinates

## Requirements

- **Java 17+**: Minimum runtime version
- **JTS Topology Suite**: For geometry types
- **Protocol Buffers**: For MVT encoding/decoding

## License

Licensed under the Apache License, Version 2.0.
