# Tileverse PMTiles

A Java library for reading and writing PMTiles - a cloud-optimized format for map tiles.

## Overview

Tileverse PMTiles is a Java implementation of the PMTiles format that provides efficient reading and writing capabilities for PMTiles archives. Built on top of [Tileverse Range Reader](../rangereader/index.md), it supports both local files and cloud storage sources (S3, Azure Blob Storage, Google Cloud Storage, HTTP).

## What is PMTiles?

PMTiles is a single-file archive format for tiled data. A .pmtiles file can contain millions of tiles compressed and organized for efficient, random access. It's designed to be hosted on commodity storage and served directly over HTTP with range requests, without needing a specialized tile server.

**Key characteristics:**

- **Cloud-optimized**: Designed for HTTP range requests
- **Single-file**: No complex directory structures
- **Efficient indexing**: Hilbert curve spatial indexing for fast tile lookup
- **Flexible**: Supports both raster and vector tiles
- **Open format**: Specification-driven, multiple implementations

## Features

- **Read PMTiles v3 files** from local storage or cloud sources
- **Write PMTiles v3 files** with efficient spatial indexing
- **Cloud-optimized access** via HTTP range requests
- **High-performance tile retrieval** using Hilbert curves
- **Multi-source support** through tileverse-rangereader integration
- **Thread-safe operations** for concurrent access
- **Memory-efficient streaming** for large datasets

## Quick Start

### Reading Tiles

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.rangereader.file.FileRangeReader;

// Create a range reader for the local file
RangeReader rangeReader = FileRangeReader.builder()
    .path(Path.of("mymap.pmtiles"))
    .build();

// Read PMTiles using the range reader
try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
    // Get metadata
    PMTilesHeader header = reader.getHeader();
    System.out.println("Tile type: " + header.tileType());

    // Read a specific tile
    Optional<byte[]> tileData = reader.getTile(10, 885, 412);

    if (tileData.isPresent()) {
        System.out.printf("Tile size: %d bytes%n", tileData.get().length);
    }
}
```

### Reading from Cloud Storage

```java
import io.tileverse.rangereader.s3.S3RangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;

// Create an S3 range reader with caching
RangeReader s3Reader = S3RangeReader.builder()
    .uri(URI.create("s3://my-bucket/tiles.pmtiles"))
    .region(Region.US_WEST_2)
    .build();

RangeReader cachedReader = CachingRangeReader.builder(s3Reader)
    .maximumSize(1000)  // Cache up to 1000 ranges
    .withBlockAlignment()  // Optimize for block-aligned reads
    .build();

try (PMTilesReader reader = new PMTilesReader(cachedReader)) {
    // Access tiles efficiently from cloud storage
    Optional<byte[]> tile = reader.getTile(10, 885, 412);
}
```

## Use Cases

- **Static tile serving**: Host PMTiles on S3/Azure/GCS, serve directly with CloudFront/CDN
- **Offline maps**: Bundle large map datasets in a single portable file
- **Tile generation pipelines**: Convert tilesets to PMTiles format
- **GeoServer integration**: Serve PMTiles through GeoServer plugins
- **Data analysis**: Random access to historical tile data

## Performance

Tileverse PMTiles is designed for high-performance access:

- **Efficient spatial indexing** using Hilbert curves for O(log n) tile lookup
- **Multi-level caching** through tileverse-rangereader integration
- **Block-aligned reads** to minimize cloud storage requests
- **Memory-efficient streaming** for processing large tile sets
- **Thread-safe concurrent access** for server applications

## Getting Started

<div class="grid cards" markdown>

-   :material-rocket-launch: **User Guide**

    ---

    Learn how to read and write PMTiles archives.

    [:octicons-arrow-right-24: User Guide](user-guide/index.md)

-   :material-book-open: **Format Specification**

    ---

    Understand the PMTiles v3 format.

    [:octicons-arrow-right-24: Specification](https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md)

</div>

## Related Modules

This library works together with other Tileverse modules:

- **[Range Reader](../rangereader/index.md)**: Provides the underlying data access layer
- **[Vector Tiles](../vectortiles/index.md)**: For decoding vector tile content
- **[Tile Matrix Set](../tilematrixset/index.md)**: For working with tile coordinates

## Requirements

- **Java 17+**: Minimum runtime version
- **Range Reader**: Core dependency for data access
- **Maven/Gradle**: For dependency management

## License

Licensed under the Apache License, Version 2.0.
