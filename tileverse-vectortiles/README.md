# Tileverse Vector Tiles

A high-performance Java library for reading and writing [Mapbox Vector Tiles](https://docs.mapbox.com/data/tilesets/guides/vector-tiles-standards/) (MVT) with JTS geometry integration and a clean, fluent API.

## Features

### **High Performance**
- **Streaming Builder**: Writes the features into the buffer of their layer as they are added, one layer at a time
- **Optimized Decoding**: Minimal object allocation with coordinate sequence reuse
- **Geometry Pipeline**: Size filter, simplification, clipping, grid snapping and polygon repair steps, or steps of your own

### **Developer Experience**
- **Fluent API**: Intuitive builder pattern for tile construction
- **Type Safety**: Strong typing with JTS geometry integration
- **Zero Configuration**: Sensible defaults with full customization options

### **Advanced Features**
- **Custom GeometryFactory Support**: Configure coordinate sequence implementations
- **Generic GeometryCollection Handling**: Automatic decomposition into multiple features
- **Tiles from Tiles**: Merges the layers and the features of other tiles into a tile under construction
- **Thread Safety**: The codec, the decoded tiles and the pipelines are safe for concurrent use. The builders are not.

## Quick Start

### Maven Dependency
```xml
<dependency>
    <groupId>io.tileverse.vectortiles</groupId>
    <artifactId>tileverse-vectortiles</artifactId>
    <version><!-- latest version --></version>
</dependency>
```

### Creating Vector Tiles

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

// Geometries in tile coordinates: the tile spans 0 to the extent on both axes
WKTReader reader = new WKTReader();
Geometry road = reader.read("LINESTRING(0 0, 1600 1600, 3200 800)");
Geometry building = reader.read("POLYGON((500 500, 1500 500, 1500 1500, 500 1500, 500 500))");

VectorTileBuilder builder = new VectorTileBuilder()
    .setExtent(4096)           // the extent of the layers (default 4096)
    .setClipBuffer(64);        // the band kept around the tile, in tile units (default 32)

// One layer at a time
builder.layer()
    .name("transportation")
    .feature()
        .geometry(road)
        .attribute("highway", "primary")
        .attribute("name", "Main Street")
        .build()               // writes the feature, returns the LayerBuilder
    .build();                  // writes the layer, returns the VectorTileBuilder
builder.layer()
    .name("buildings")
    .feature(Map.of("type", "residential"), building)
    .build();

// The encoded tile, written to a file
try (OutputStream out = Files.newOutputStream(Paths.get("tile.mvt"))) {
    builder.writeTo(out);
}
```

`builder.toByteArray()` returns the same bytes, and `builder.build()` the tile as a `VectorTile`. [Writing Vector Tiles](#writing-vector-tiles) gives the rules of the builder.

### Reading Vector Tiles

```java
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

// Decode MVT bytes
VectorTileCodec codec = new VectorTileCodec();
VectorTile tile = codec.decode(mvtBytes);

// Access layers and features
tile.getLayer("transportation")
    .ifPresent(layer -> {
        System.out.println("Layer: " + layer.getName());
        System.out.println("Extent: " + layer.getExtent());
        System.out.println("Feature count: " + layer.count());
        
        layer.getFeatures().forEach(feature -> {
            Geometry geometry = feature.getGeometry();
            Map<String, Object> attributes = feature.getAttributes();
            System.out.println("Highway: " + attributes.get("highway"));
            System.out.println("Geometry: " + geometry);
        });
    });
```

### Custom GeometryFactory

```java
import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.util.stream.Stream;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

// Use array-based coordinates for faster access
GeometryFactory customFactory = new GeometryFactory(
    CoordinateArraySequenceFactory.instance()
);
GeometryReader reader = VectorTileCodec.newGeometryReader().withGeometryFactory(customFactory);

VectorTile tile = new VectorTileCodec().decode(mvtBytes);
Stream<Feature> features = tile.getFeatures("transportation", feature -> true, reader); // Geometries use custom factory
```

## Architecture

### Key Components

- **VectorTileBuilder**, **LayerBuilder** and **FeatureBuilder**: write a tile one layer at a time, as MVT bytes or as a `VectorTile`
- **GeometryPipeline** and its **GeometryStep**s: the changes to the geometries before they are written, such as clipping and simplification
- **VectorTileCodec**: decodes MVT bytes into a `VectorTile`, and encodes a `VectorTile` into MVT bytes
- **Model Interfaces**: `VectorTile`, `VectorTile.Layer` and `VectorTile.Layer.Feature`

## Writing Vector Tiles

`VectorTileBuilder` writes a tile one layer at a time. The [encoding how-to](../docs/src/vectortiles/how-to/encode.md) describes it in full.

### Layers

The features of a layer are added between `layer()` and `LayerBuilder.build()`, and the build writes the layer into the tile. A tile holds one layer per name: `name(...)` fails with an `IllegalArgumentException` for the name of a layer written before. A layer with features must be built before the next layer starts, and before the tile is written. A layer without features is left out. A `GeometryCollection` gives one feature per member.

### The Bytes

`toByteArray()` returns the encoded tile, and `writeTo(OutputStream)` writes it to a stream without copying it first. `build()` returns the tile as a `VectorTile`, read from its bytes without a copy. The three methods clear the builder for the next tile. `VectorTileCodec.encode` writes a built or decoded tile as its bytes, and encodes the tiles of other `VectorTile` implementations too.

### The Pipeline

The geometry of a feature goes through a `GeometryPipeline` of `GeometryStep`s. The builder then rounds its coordinates and writes it. The settings derive the pipeline, in this order:

1. `SizeFilterStep(1, 1)` drops the lines shorter than one tile unit and the polygons of area below one square tile unit.
2. `SimplificationStep` simplifies with the tolerance of `setSimplificationDistanceTolerance`, when the tolerance is positive.
3. `ClipStep` clips to the extent of `setExtent`, 4096 by default, and a band of `setClipBuffer` tile units around it, 32 by default.
4. `GridSnapStep` snaps to the integer grid with the precision model of JTS, when `setUsePrecisionModelSnapping(true)` is set. Otherwise the builder rounds the coordinates when it writes them.
5. `PolygonRepairStep` repairs the polygons invalid once rounded, when the simplification tolerance is positive.

`setPipeline` replaces the derived pipeline, for example with the size filter alone, or with a step of your own:

```java
import io.tileverse.vectortile.pipeline.ClipStep;
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import io.tileverse.vectortile.pipeline.GeometryStep;
import io.tileverse.vectortile.pipeline.SizeFilterStep;
import org.locationtech.jts.geom.Puntal;

// Encoding only: no clip, no simplification
builder.setPipeline(GeometryPipeline.of(new SizeFilterStep(1, 1)));

// A step returns the geometry for the next steps, or null to drop the feature
GeometryStep withoutPoints = geometry -> geometry instanceof Puntal ? null : geometry;
builder.setPipeline(GeometryPipeline.of(new SizeFilterStep(1, 1), withoutPoints, new ClipStep(4096, 32)));
```

A pipeline and its steps are safe to share between threads. A later call to `setExtent`, `setClipBuffer`, `setInputScale`, `setSimplificationDistanceTolerance` or `setUsePrecisionModelSnapping` derives the pipeline from the settings again: the last call wins.

### Ids

A feature without an id takes the next automatic id. The automatic ids start at `setFirstFeatureId(long)`, 0 by default, continue after the largest id of the tile, and restart for the next tile. A negative id means no id, and `setAutoIncrementIds(false)` turns the automatic ids off.

### Input Scale

`setInputScale(16)` takes geometries from a grid of 256 units to a tile of extent 4096. The builder multiplies the coordinates by the scale when it rounds them, without a scaled copy. The settings stay in tile units, and the derived steps take them divided by the scale.

### Tiles from Tiles

`LayerBuilder.merge(Layer)` adds the features of a layer of another tile, with their ids and attributes, without the steps of the pipeline. `merge(VectorTile)` adds the layers of a tile, and `init(VectorTile)` starts a tile from one, with its extent.

### Heap

`heapSize()` returns the heap held by the tile under construction, for a memory guard. It counts the capacity of the buffers, plus an estimate of the keys and values of the open layer.

### Coordinates

The builder takes geometries in tile coordinates: the tile spans 0 to the extent on both axes, and y grows downward. The caller transforms the geometries from a coordinate reference system, such as Web Mercator, to tile coordinates first. The default pipeline clips the geometries to the tile and its buffer. Without a `ClipStep`, the builder writes the coordinates outside the tile as they are, once rounded.

## Performance Characteristics

- **Memory Efficient**: Streaming construction avoids intermediate collections
- **Optimized Encoding**: Direct OutputStream/ByteBuffer encoding avoids byte array allocation
- **Fast Decoding**: A feature builds its JTS geometry and decodes its attributes when asked
- **Compact Decoded Tiles**: A decoded tile holds its bytes, an index of its layers and features, and the packed coordinates of its geometries
- **No Protobuf Runtime**: Reads and writes the MVT wire format itself
- **Thread Safe**: The codec, the decoded tiles and the pipelines are safe for concurrent use. The builders are not.

### Performance Improvements

This library builds upon the original [Electronic Chart Centre's java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) with significant optimizations:

- **Memory usage**: Reduced object allocation with streaming patterns

The [benchmarks](../internal/vectortiles-benchmarks/README.md) measure the time, the allocation and the heap of both libraries on public tiles, when they decode tiles and when they write them.

### Encoding Performance Tips

```java
// Best: write the tile under construction to the output, without a copy first
builder.writeTo(outputStream);

// A built or decoded tile holds its bytes, and the codec writes them as they are
codec.encode(tile, outputStream);

// An array of the bytes, for an API taking arrays
byte[] bytes = builder.toByteArray();
```

## MVT Compliance

Full compliance with the [Mapbox Vector Tile Specification 2.1](https://github.com/mapbox/vector-tile-spec):

- All geometry types (Point, LineString, Polygon, Multi*)
- Feature attributes (string, number, boolean)
- ZigZag coordinate encoding
- Command-based geometry encoding
- Layer extent and feature ID handling
- Exterior rings with a positive area by the surveyor's formula on the written coordinates, holes with a negative one
- One layer per name in a tile

## Requirements

- **Java 17+** (runtime)
- **JDK 25+** (to build the project)
- **JTS Topology Suite** for geometry operations

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.

## Contributing

Contributions are welcome. The [contribution guide](../docs/src/developer-guide/how-to/contribute.md) describes the pull request process, the coding standards and the commit messages.