# Tileverse Vector Tiles

A high-performance Java library for reading and writing [Mapbox Vector Tiles](https://docs.mapbox.com/data/tilesets/guides/vector-tiles-standards/) (MVT) with JTS geometry integration and a clean, fluent API.

## Features

### **High Performance**
- **Streaming Builder**: Memory-efficient tile creation with direct protobuf encoding
- **Optimized Decoding**: Minimal object allocation with coordinate sequence reuse
- **Precision Snapping**: JTS GeometryPrecisionReducer for robust integer coordinate handling
- **Geometry Clipping**: Configurable buffer zones for efficient tile boundary handling

### **Developer Experience**
- **Fluent API**: Intuitive builder pattern for tile construction
- **Type Safety**: Strong typing with JTS geometry integration
- **Comprehensive Testing**: AssertJ-style assertions for MVT command validation
- **Zero Configuration**: Sensible defaults with full customization options

### **Advanced Features**
- **Custom GeometryFactory Support**: Configure coordinate sequence implementations
- **Generic GeometryCollection Handling**: Automatic decomposition into multiple features
- **Coordinate Validation**: Automatic bounds checking and invalid geometry filtering
- **Thread Safety**: All components designed for concurrent usage

## Quick Start

### Maven Dependency
```xml
<dependency>
    <groupId>io.tileverse</groupId>
    <artifactId>tileverse-vectortiles</artifactId>
    <version><!-- latest version --></version>
</dependency>
```

### Creating Vector Tiles

```java
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

// Create geometries (coordinates must be in extent space [0, extent-1])
WKTReader reader = new WKTReader();
Geometry road = reader.read("LINESTRING(0 0, 1600 1600, 3200 800)");
Geometry building = reader.read("POLYGON((500 500, 1500 500, 1500 1500, 500 1500, 500 500))");

// Build tile using fluent API
VectorTileBuilder builder = new VectorTileBuilder()
    .setExtent(4096)           // Coordinate precision (default: 4096)
    .setClipBuffer(64);        // Clipping buffer in extent units (default: 32)

Tile tile = builder
    .layer()
        .name("transportation")
        .feature()
            .geometry(road)
            .attribute("highway", "primary")
            .attribute("name", "Main Street")
            .build()
        .build()
    .layer()
        .name("buildings")  
        .feature()
            .geometry(building)
            .attribute("type", "residential")
            .build()
        .build()
    .build();

// Encode to MVT bytes
VectorTileCodec codec = new VectorTileCodec();

// Most efficient: stream directly to OutputStream (recommended)
try (OutputStream out = Files.newOutputStream(Paths.get("tile.mvt"))) {
    codec.encode(tile, out);
}

// Alternative: encode to ByteBuffer (avoids byte array allocation)
ByteBuffer buffer = ByteBuffer.allocate(codec.getSerializedSize(tile));
codec.encode(tile, buffer);

// Simple but less efficient: byte array (creates intermediate allocation)
byte[] mvtBytes = codec.encode(tile);
```

### Reading Vector Tiles

```java
// Decode MVT bytes
VectorTileCodec codec = new VectorTileCodec();
Tile tile = codec.decode(mvtBytes);

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
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

// Use array-based coordinates for faster access
GeometryFactory customFactory = new GeometryFactory(
    CoordinateArraySequenceFactory.instance()
);

VectorTileCodec codec = new VectorTileCodec(customFactory);
Tile tile = codec.decode(mvtBytes); // Geometries use custom factory
```

## Architecture

### Clean Separation of Concerns

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  VectorTile     │    │  Model Objects   │    │  VectorTile     │
│  Builder        │───▶│  (Tile, Layer,   │───▶│  Codec          │
│                 │    │   Feature)       │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
      │                           │                       │
      ▼                           ▼                       ▼
  Tile Creation              Business Logic          Serialization
  • Geometry prep           • Coordinate space       • MVT protobuf
  • Feature building        • JTS integration        • Encoding/decoding
  • Validation             • Type safety             • Format conversion
```

### Key Components

- **VectorTileBuilder**: Streaming tile construction with geometry processing
- **VectorTileCodec**: Serialization between model objects and MVT bytes
- **Model Interfaces**: Clean abstractions (Tile, Layer, Feature)
- **GeometryEncoder**: MVT command generation with precision handling

## Coordinate System

Vector tiles expect coordinates in **extent space** [0, extent-1]:

```java
// For extent = 4096, valid coordinates are [0, 4095]
Geometry valid = reader.read("POINT(2048 1024)");    // Valid
Geometry invalid = reader.read("POINT(5000 -100)");  // Invalid
```

The library handles coordinate transformation from real-world CRS to extent space **before** tile creation:

```java
// Your responsibility: CRS → Extent space
Envelope tileEnvelope = new Envelope(xmin, xmax, ymin, ymax); // Real-world bounds
Geometry transformed = transformToExtentSpace(geometry, tileEnvelope, 4096);

// Library responsibility: Extent space → MVT encoding  
builder.layer().name("data").feature().geometry(transformed).build();
```

## Advanced Configuration

### Precision Snapping
```java
VectorTileBuilder builder = new VectorTileBuilder()
    .setUsePrecisionModelSnapping(true);  // Use JTS GeometryPrecisionReducer

// Fractional coordinates get snapped to integers
Geometry input = reader.read("POINT(100.7 200.3)");
// Becomes: POINT(101 200) after precision snapping
```

### Geometry Clipping  
```java
VectorTileBuilder builder = new VectorTileBuilder()
    .setExtent(4096)
    .setClipBuffer(128);  // Allow geometries 128 units outside tile bounds

// Clipping envelope: [-128, -128] to [4224, 4224]
```

### Simplification
```java
VectorTileBuilder builder = new VectorTileBuilder()
    .setSimplificationDistanceTolerance(2.0);  // Douglas-Peucker tolerance

// Complex geometries simplified before encoding
```

## Testing Support

The library includes comprehensive testing utilities:

```java
import static io.tileverse.vectortile.mvt.TileAssertions.assertThat;

@Test
void testTileCreation() {
    Tile tile = builder.layer()
        .name("test")
        .feature()
            .geometry(geom("LINESTRING(0 0, 100 100)"))
            .attribute("type", "test")
            .build()
        .build()
        .build();

    // Fluent assertions for MVT validation
    assertThat(tile)
        .hasLayerCount(1)
        .layer("test")
            .hasFeatureCount(1)
            .hasExtent(4096)
            .feature(0)
                .hasAttribute("type", "test")
                .geometry()
                    .isLineString()
                    .moveTo(0, 0)
                    .lineTo(100, 100)
                    .matches();
}
```

## Performance Characteristics

- **Memory Efficient**: Streaming construction avoids intermediate collections
- **Optimized Encoding**: Direct OutputStream/ByteBuffer encoding avoids byte array allocation
- **Fast Decoding**: Heavily optimized geometry decoder shows ~2.5x performance improvement over original java-vector-tile library (9.0s vs 23.2s on large tile benchmark)
- **Minimal Allocations**: Reuses objects where possible with configurable GeometryFactory
- **Protobuf Lite**: Uses protobuf-javalite for reduced binary size and faster serialization
- **Thread Safe**: All components safe for concurrent use

### Performance Improvements

This library builds upon the original [Electronic Chart Centre's java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) with significant optimizations:

- **Geometry decoding**: ~2.5x faster coordinate sequence processing
- **Memory usage**: Reduced object allocation with streaming patterns
- **Binary size**: Smaller footprint with protobuf-javalite dependency

*Note: Performance numbers from `VectorTileCodecTest.testBigTile(1000)` benchmark (9077ms vs 23215ms). Comprehensive JMH benchmarks are planned for future releases.*

### Encoding Performance Tips

```java
// Best: Stream directly to output (zero intermediate allocation)
codec.encode(tile, outputStream);

// Good: Use pre-sized ByteBuffer (single allocation)
ByteBuffer buffer = ByteBuffer.allocate(codec.getSerializedSize(tile));
codec.encode(tile, buffer);

// Avoid: byte[] method creates unnecessary intermediate array
byte[] bytes = codec.encode(tile);  // Less efficient
```

## MVT Compliance

Full compliance with the [Mapbox Vector Tile Specification 2.1](https://github.com/mapbox/vector-tile-spec):

- All geometry types (Point, LineString, Polygon, Multi*)
- Feature attributes (string, number, boolean)
- ZigZag coordinate encoding
- Command-based geometry encoding
- Layer extent and feature ID handling

## Requirements

- **Java 17+** (runtime)
- **Java 21+** (development)
- **JTS Topology Suite** for geometry operations
- **Protocol Buffers Java Lite** for efficient MVT serialization

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../../LICENSE) for details.

## Contributing

We welcome contributions! Please see our [contributing guidelines](../../CONTRIBUTING.md) for details on how to submit pull requests, report issues, and suggest improvements.