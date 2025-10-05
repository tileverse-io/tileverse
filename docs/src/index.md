# Tileverse

A comprehensive Java library ecosystem for working with geospatial tiles, cloud-optimized formats, and efficient data access.

## Overview

Tileverse is a modular collection of libraries designed for building high-performance geospatial applications. It provides everything from low-level efficient data access to high-level tile format support, with a focus on cloud-native architectures and modern Java practices.

## Libraries

### 🌐 Range Reader

Efficient random access to byte ranges from local files, HTTP servers, and cloud storage (S3, Azure Blob, Google Cloud Storage).

**Key Features:**

- Universal API for multiple data sources
- Multi-level caching (memory + disk)
- Block alignment optimization
- Thread-safe concurrent access
- HTTP authentication support

[:octicons-arrow-right-24: Learn More](rangereader/index.md){ .md-button .md-button--primary }

### 🗺️ PMTiles

Reading and writing PMTiles archives - a cloud-optimized format for map tiles.

**Key Features:**

- PMTiles v3 format support
- Hilbert curve spatial indexing
- Cloud-optimized access patterns
- Memory-efficient streaming
- Vector and raster tile support

[:octicons-arrow-right-24: Learn More](pmtiles/index.md){ .md-button .md-button--primary }

### 📐 Tile Matrix Set

Generic object model for defining tile pyramids and tiling schemes.

**Key Features:**

- Standard tile matrix set definitions
- Coordinate transformations
- Flexible tiling schemes
- Web Mercator, WGS84, and custom CRS support

[:octicons-arrow-right-24: Learn More](tilematrixset/index.md){ .md-button .md-button--primary }

### 🎨 Vector Tiles

Encoding and decoding Mapbox Vector Tiles (MVT).

**Key Features:**

- MVT format encoding/decoding
- Protocol Buffers support
- Geometry transformations
- Layer and feature manipulation

[:octicons-arrow-right-24: Learn More](vectortiles/index.md){ .md-button .md-button--primary }

## Quick Start

Get started with Tileverse in minutes:

<div class="grid cards" markdown>

-   :material-clock-fast: **Quick Start**

    ---

    Jump right in with installation and basic examples.

    [:octicons-arrow-right-24: Getting Started](getting-started.md)

-   :material-book-open: **Architecture**

    ---

    Understand how the modules fit together.

    [:octicons-arrow-right-24: Architecture](architecture/index.md)

-   :fontawesome-brands-github: **Source Code**

    ---

    Browse the source code on GitHub.

    [:octicons-arrow-right-24: GitHub](https://github.com/tileverse-io/tileverse)

-   :material-package: **Maven Central**

    ---

    Find published artifacts and versions.

    [:octicons-arrow-right-24: Maven Central](https://central.sonatype.com/search?q=io.tileverse)

</div>

## Use Cases

- **Geospatial Servers**: Build high-performance tile servers (GeoServer, MapServer plugins)
- **Cloud-Native Applications**: Efficient access to tiles stored in S3, Azure, or GCS
- **Map Rendering**: Client and server-side map tile rendering
- **Data Processing**: ETL pipelines for geospatial data transformation
- **Analytics**: Random access to large geospatial datasets

## Requirements

- **Java 17+** (runtime)
- **Java 21+** (development - recommended)
- **Maven 3.9+** or **Gradle 7.0+** (build)

## License

All Tileverse libraries are released under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

---

*Built with ❤️ by [Multiversio LLC](https://multivers.io) and [Camptocamp](https://camptocamp.com) for the geospatial community.*
