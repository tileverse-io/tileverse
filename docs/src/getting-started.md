# Getting Started with Tileverse

This guide will help you get started with the Tileverse libraries in your Java project.

## Installation

All Tileverse modules are published to Maven Central. The easiest way to use them is through the Tileverse BOM (Bill of Materials), which manages all module versions consistently.

### Maven

Add the Tileverse BOM to your `dependencyManagement`:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.tileverse</groupId>
      <artifactId>tileverse-bom</artifactId>
      <version>1.1-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

Then add the modules you need without specifying versions:

```xml
<dependencies>
  <!-- Range Reader with all providers -->
  <dependency>
    <groupId>io.tileverse.rangereader</groupId>
    <artifactId>tileverse-rangereader-all</artifactId>
  </dependency>

  <!-- PMTiles support -->
  <dependency>
    <groupId>io.tileverse.pmtiles</groupId>
    <artifactId>tileverse-pmtiles</artifactId>
  </dependency>

  <!-- Tile Matrix Set model -->
  <dependency>
    <groupId>io.tileverse.tilematrixset</groupId>
    <artifactId>tileverse-tilematrixset</artifactId>
  </dependency>

  <!-- Vector Tiles -->
  <dependency>
    <groupId>io.tileverse.vectortiles</groupId>
    <artifactId>tileverse-vectortiles</artifactId>
  </dependency>
</dependencies>
```

### Gradle

```gradle
dependencies {
    implementation platform('io.tileverse:tileverse-bom:1.1-SNAPSHOT')

    implementation 'io.tileverse.rangereader:tileverse-rangereader-all'
    implementation 'io.tileverse.pmtiles:tileverse-pmtiles'
    implementation 'io.tileverse.tilematrixset:tileverse-tilematrixset'
    implementation 'io.tileverse.vectortiles:tileverse-vectortiles'
}
```

## Quick Examples

### Reading PMTiles from S3

Combining Range Reader and PMTiles to access map tiles from cloud storage:

```java
import io.tileverse.rangereader.s3.S3RangeReader;
import io.tileverse.pmtiles.PMTilesReader;

// Create an S3 range reader with caching
RangeReader rangeReader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/world.pmtiles"))
    .withCaching()
    .build();

// Read PMTiles using the range reader
try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
    // Get tile at zoom 10, x=885, y=412
    Optional<byte[]> tileData = reader.getTile(10, 885, 412);

    if (tileData.isPresent()) {
        System.out.printf("Tile size: %d bytes%n", tileData.get().length);
    }
}
```

### Decoding Vector Tiles

```java
import io.tileverse.vectortile.mvt.VectorTileCodec;
import io.tileverse.vectortile.model.VectorTile;

// Decode MVT bytes
VectorTileCodec codec = VectorTileCodec.getDefault();
VectorTile tile = codec.decode(mvtBytes);

// Access layers and features
for (VectorTile.Layer layer : tile.getLayers()) {
    System.out.printf("Layer: %s (%d features)%n",
        layer.getName(), layer.getFeatures().size());

    for (VectorTile.Layer.Feature feature : layer.getFeatures()) {
        Geometry geometry = feature.getGeometry();
        Map<String, Object> attributes = feature.getAttributes();
        // Process feature...
    }
}
```

### Working with Tile Matrix Sets

```java
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;

// Use a standard tile matrix set
TileMatrixSet webMercator = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

// Get tile matrix for a zoom level
TileMatrix matrix = webMercator.tileMatrix(10);

// Calculate tile coordinates from geographic coordinates
BoundingBox2D bbox = new BoundingBox2D(-122.4, 37.7, -122.3, 37.8);
TileRange tiles = matrix.getTilesIntersecting(bbox);

System.out.printf("Zoom %d covers tiles: %s%n", 10, tiles);
```

## Module Relationships

Understanding how the modules work together:

```
┌─────────────────────────────────────────────────────┐
│                   Applications                      │
│  (GeoServer, Custom Tile Servers, ETL Pipelines)   │
└─────────────────┬───────────────────────────────────┘
                  │
    ┌─────────────┼─────────────┬─────────────┐
    │             │             │             │
    ▼             ▼             ▼             ▼
┌─────────┐  ┌──────────┐  ┌────────┐  ┌──────────┐
│ PMTiles │  │  Vector  │  │  Tile  │  │  Other   │
│         │  │  Tiles   │  │Matrix  │  │ Formats  │
└────┬────┘  └────┬─────┘  └────┬───┘  └────┬─────┘
     │            │             │            │
     └────────────┴─────────────┴────────────┘
                       │
                       ▼
              ┌────────────────┐
              │ Range Reader   │
              │  (Core Layer)  │
              └────────┬───────┘
                       │
         ┌─────────────┼─────────────┬────────────┐
         │             │             │            │
         ▼             ▼             ▼            ▼
    ┌────────┐    ┌───────┐    ┌────────┐   ┌─────┐
    │  File  │    │  HTTP │    │   S3   │   │ ... │
    └────────┘    └───────┘    └────────┘   └─────┘
```

- **Range Reader** provides the foundation for efficient byte-range access
- **Tile Matrix Set** defines coordinate systems and tiling schemes
- **Vector Tiles** handles MVT encoding/decoding
- **PMTiles** combines Range Reader with tile storage format

## Next Steps

Now that you have a basic understanding, dive deeper into specific modules:

- **[Range Reader User Guide](rangereader/user-guide/index.md)** - Learn about data sources, caching, and authentication
- **[PMTiles User Guide](pmtiles/user-guide/index.md)** - Read and write PMTiles archives
- **[Vector Tiles User Guide](vectortiles/user-guide/index.md)** - Encode and decode MVT data
- **[Tile Matrix Set User Guide](tilematrixset/user-guide/index.md)** - Work with tile pyramids and coordinate systems

## Requirements

- **Java 17+** (runtime)
- **Java 21+** (development - recommended)
- **Maven 3.9+** or **Gradle 7.0+** for building

## Community

- **GitHub**: [https://github.com/tileverse-io/tileverse](https://github.com/tileverse-io/tileverse)
- **Issues**: [https://github.com/tileverse-io/tileverse/issues](https://github.com/tileverse-io/tileverse/issues)
- **Discussions**: [https://github.com/tileverse-io/tileverse/discussions](https://github.com/tileverse-io/tileverse/discussions)
