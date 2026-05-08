# PMTiles User Guide

Welcome to the Tileverse PMTiles User Guide. This guide will help you understand and use the PMTiles library effectively.

## What You'll Learn

This guide covers:

- **[Quick Start](quick-start.md)**: Get up and running quickly
- **[Reading PMTiles](reading.md)**: Read tiles from PMTiles archives
- **[Cloud Storage](cloud-storage.md)**: Work with PMTiles in S3, Azure, and GCS

## Installation

Add the PMTiles dependency to your project:

=== "Maven"

    ```xml
    <dependency>
        <groupId>io.tileverse.pmtiles</groupId>
        <artifactId>tileverse-pmtiles</artifactId>
        <version>2.0.0</version>
    </dependency>

    <!-- Also add a Range Reader provider -->
    <dependency>
        <groupId>io.tileverse.storage</groupId>
        <artifactId>tileverse-storage-all</artifactId>
        <version>2.0.0</version>
    </dependency>
    ```

=== "Gradle"

    ```gradle
    dependencies {
        implementation 'io.tileverse.pmtiles:tileverse-pmtiles:2.0.0'
        implementation 'io.tileverse.storage:tileverse-storage-all:2.0.0'
    }
    ```

## Core Concepts

### PMTiles Structure

A PMTiles archive contains:

1. **Header**: Metadata about the tileset (bounds, zoom levels, tile format)
2. **Directory**: Spatial index using Hilbert curves
3. **Tiles**: Compressed tile data

### Tile Addressing

Tiles are addressed using the standard XYZ scheme:

- **z**: Zoom level (0 = world view, higher = more detailed)
- **x**: Column number (west to east)
- **y**: Row number (north to south in TMS, south to north in XYZ)

### Data Sources

PMTiles can be read from any source supported by Range Reader:

- Local files
- HTTP/HTTPS servers
- Amazon S3
- Azure Blob Storage
- Google Cloud Storage

## Next Steps

- **[Quick Start](quick-start.md)**: Begin with a simple example
- **[Reading PMTiles](reading.md)**: Learn about reading tiles
- **[Cloud Storage](cloud-storage.md)**: Access PMTiles from the cloud
