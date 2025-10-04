# Tileverse

A comprehensive Java library ecosystem for working with geospatial tiles, cloud-optimized formats, and efficient data access.

## Overview

Tileverse is a modular collection of libraries designed for building high-performance geospatial applications. It provides everything from low-level efficient data access to high-level tile format support, with a focus on cloud-native architectures and modern Java practices.

## Libraries

### 🌐 [Range Reader](tileverse-rangereader/)

Efficient random access to byte ranges from local files, HTTP servers, and cloud storage (S3, Azure Blob, Google Cloud Storage).

**Features:**

- Universal API for multiple data sources
- Multi-level caching (memory + disk)
- Block alignment optimization
- Thread-safe concurrent access
- HTTP authentication support

**[📖 Documentation](https://tileverse-io.github.io/tileverse-rangereader/)** | **[🚀 Quick Start](https://tileverse-io.github.io/tileverse-rangereader/user-guide/quick-start/)**

### 🗺️ [PMTiles](tileverse-pmtiles/)

Reading and writing PMTiles archives - a cloud-optimized format for map tiles.

**Features:**

- PMTiles v3 format support
- Hilbert curve spatial indexing
- Cloud-optimized access patterns
- Memory-efficient streaming
- Vector and raster tile support

### 📐 [Tile Matrixset](tileverse-tilematrixset/)

Generic object model for defining tile pyramids and tiling schemes.

**Features:**

- Standard tile matrix set definitions
- Coordinate transformations
- Flexible tiling schemes
- Web Mercator, WGS84, and custom CRS support

### 🎨 [Vector Tiles](tileverse-vectortiles/)

Encoding and decoding Mapbox Vector Tiles (MVT).

**Features:**

- MVT format encoding/decoding
- Protocol Buffers support
- Geometry transformations
- Layer and feature manipulation

## Installation

### Maven

Add the Tileverse BOM to your `dependencyManagement`:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.tileverse</groupId>
      <artifactId>tileverse-bom</artifactId>
      <version>1.0-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

Then add the modules you need:

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

  <!-- Tile Pyramid model -->
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
    implementation platform('io.tileverse:tileverse-bom:1.0-SNAPSHOT')

    implementation 'io.tileverse.rangereader:tileverse-rangereader-all'
    implementation 'io.tileverse.pmtiles:tileverse-pmtiles'
    implementation 'io.tileverse.tilematrixset:tileverse-tilematrixset'
    implementation 'io.tileverse.vectortiles:tileverse-vectortiles'
}
```

## Use Cases

- **Geospatial Servers**: Build high-performance tile servers (GeoServer, MapServer plugins)
- **Cloud-Native Applications**: Efficient access to tiles stored in S3, Azure, or GCS
- **Map Rendering**: Client and server-side map tile rendering
- **Data Processing**: ETL pipelines for geospatial data transformation
- **Analytics**: Random access to large geospatial datasets

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Applications                      │
│  (GeoServer, Custom Tile Servers, ETL Pipelines)    │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
                ┌──────────────┐
                │   PMTiles    │
                │   (Format)   │
                └─┬────┬─────┬─┘
                  │    │     │
      ┌───────────┘    │     └──────────┐
      │                │                │
      ▼                ▼                ▼
┌──────────┐    ┌────────────┐   ┌──────────────┐
│  Vector  │    │    Tile    │   │Range Reader  │
│  Tiles   │    │  Pyramid   │   │ (Data Access)│
└──────────┘    └────────────┘   └──────┬───────┘
                                        │
                                ┌───────┼──────┬────────┬────────┐
                                │       │      │        │        │
                                ▼       ▼      ▼        ▼        ▼
                              ┌────┐ ┌────┐ ┌─────┐ ┌───────┐ ┌─────┐
                              │File│ │HTTP│ │ S3  │ │ Azure │ │ GCS │
                              └────┘ └────┘ └─────┘ └───────┘ └─────┘
```

## Requirements

- **Java 17+** (runtime)
- **Java 21+** (development - recommended)
- **Maven 3.9+** or **Gradle 7.0+** (build)

## Development

Quick development commands using the included Makefile:

```bash
make help      # Show all available targets
make           # Build and test everything
make test      # Run all tests (unit + integration)
make verify    # Full verification (lint + test)
make format    # Format code (Spotless + SortPOM)
make lint      # Check code formatting
make clean     # Clean build artifacts
```

### Maven Commands

```bash
# Build everything
./mvnw clean install

# Run tests
./mvnw test                    # Unit tests
./mvnw verify                  # Unit + integration tests
./mvnw verify -DskipTests      # Skip tests

# Run tests for specific modules
./mvnw test -pl tileverse-rangereader/core
./mvnw test -pl tileverse-pmtiles

# Code formatting
./mvnw validate                # Apply formatting
./mvnw validate -Pqa          # Check formatting without changes

# Generate coverage reports
./mvnw verify -pl coverage-report
```

### Project Structure

```
tileverse/
├── tileverse-rangereader/    # Range-based data access
│   ├── core/                 # Core interfaces and implementations
│   ├── s3/                   # AWS S3 support
│   ├── azure/                # Azure Blob Storage support
│   ├── gcs/                  # Google Cloud Storage support
│   └── all/                  # Aggregator with all providers
├── tileverse-vectortiles/    # Mapbox Vector Tiles support
├── tileverse-tilematrixset/    # Tile pyramid model
├── tileverse-pmtiles/        # PMTiles format support
├── dependencies/             # BOM for dependency management
├── bom/                      # BOM for Tileverse modules
└── coverage-report/          # Aggregate coverage reports
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

### Guidelines

1. Follow the existing code style (enforced by Spotless)
2. Add tests for new functionality
3. Update documentation as needed
4. Ensure all tests pass: `make verify`
5. Add license headers to new files

See the **[Contributing Guide](CONTRIBUTING.md)** for more details.

## License

[Apache License 2.0](LICENSE)

## Acknowledgments

- [Protomaps](https://github.com/protomaps/PMTiles) for the PMTiles specification
- [Mapbox](https://github.com/mapbox/tippecanoe) for vector tile innovations
- The geospatial open-source community

## Links

- **Documentation**: [tileverse.io](https://tileverse.io)
- **Issues**: [GitHub Issues](https://github.com/tileverse-io/tileverse/issues)
- **Discussions**: [GitHub Discussions](https://github.com/tileverse-io/tileverse/discussions)
