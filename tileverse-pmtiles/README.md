# Tileverse PMTiles

A Java library for reading and writing PMTiles - a cloud-optimized format for map tiles.

> **Note**: This is part of the [Tileverse](../) project. See the [main README](../README.md) for installation instructions and project overview.

## Overview

Tileverse PMTiles is a Java implementation of the PMTiles format that provides efficient reading and writing capabilities for PMTiles archives. Built on top of [Tileverse Storage](../tileverse-storage/), it supports both local files and cloud storage sources (S3, Azure Blob Storage, Google Cloud Storage, HTTP).

## Features

- **Read PMTiles v3 files** from local storage or cloud sources
- **Write PMTiles v3 files** with efficient spatial indexing
- **Cloud-optimized access** via HTTP range requests
- **High-performance tile retrieval** using Hilbert curve spatial indexing
- **Multi-source support** through tileverse-storage integration
- **Thread-safe operations** for concurrent access
- **Memory-efficient streaming** for large datasets

## Installation

**Maven:**
```xml
<dependency>
    <groupId>io.tileverse.pmtiles</groupId>
    <artifactId>tileverse-pmtiles</artifactId>
    <version>2.0.0</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'io.tileverse.pmtiles:tileverse-pmtiles:2.0.0'
```

See the [main README](../README.md#installation) for BOM usage.

## Usage Examples

#### Reading PMTiles from Local Files

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.storage.rangereader.file.FileRangeReader;

// Create a range reader for the local file
RangeReader rangeReader = FileRangeReader.builder()
    .path(Path.of("mymap.pmtiles"))
    .build();

// Read PMTiles using the range reader
try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
    // Get metadata
    PMTilesHeader header = reader.getHeader();
    System.out.println("Map bounds: " + 
        header.minLonE7() / 10000000.0 + "," + 
        header.minLatE7() / 10000000.0 + "," + 
        header.maxLonE7() / 10000000.0 + "," + 
        header.maxLatE7() / 10000000.0);
    
    // Read a specific tile
    Optional<byte[]> tileData = reader.getTile(10, 885, 412);
    
    if (tileData.isPresent()) {
        System.out.printf("Tile data size: %d bytes%n", tileData.get().length);
    }
}
```

#### Reading PMTiles from Cloud Storage

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.block.BlockAlignedRangeReader;
import io.tileverse.storage.cache.CachingRangeReader;
import java.util.Properties;

// Open a Storage for the bucket, then get a RangeReader for the tileset key
Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

try (Storage storage = StorageFactory.open(URI.create("s3://my-bucket/"), props);
        RangeReader s3Reader = storage.openRangeReader(URI.create("s3://my-bucket/tiles.pmtiles"));
        // Cache exact ranges, then align hot regions on top of the cache
        RangeReader cachedReader = CachingRangeReader.builder(s3Reader).build();
        RangeReader alignedReader = BlockAlignedRangeReader.builder(cachedReader)
            .alignWholeFile()
            .build();
        PMTilesReader reader = new PMTilesReader(alignedReader)) {
    // Access tiles efficiently from cloud storage
    Optional<byte[]> tile = reader.getTile(10, 885, 412);
}
```

#### Reading PMTiles from HTTP Sources

```java
import io.tileverse.storage.rangereader.http.HttpRangeReader;

// Read from HTTP with authentication
RangeReader httpReader = HttpRangeReader.builder()
    .uri(URI.create("https://example.com/tiles.pmtiles"))
    .bearerToken("your-api-token")
    .build();

try (PMTilesReader reader = new PMTilesReader(httpReader)) {
    PMTilesHeader header = reader.getHeader();
    System.out.printf("Tile format: %s%n", header.tileType());
}
```

## Documentation

For more detailed information, see the documentation:

- [PMTiles Format Specification](docs/pmtiles_format_specification.md) - Technical details of the PMTiles format
- [Cloud Storage Support](docs/cloud_storage_support.md) - Using PMTiles with S3, Azure, and HTTP

## Related Modules

This library works together with other Tileverse modules:

- **[tileverse-storage](../tileverse-storage/)**: Provides the underlying data access layer
- **[tileverse-vectortiles](../tileverse-vectortiles/)**: For working with Mapbox Vector Tiles
- **[tileverse-tilematrixset](../tileverse-tilematrixset/)**: Generic tile pyramid and tiling scheme models

## Performance

Tileverse PMTiles is designed for high-performance access to PMTiles archives:

- **Efficient spatial indexing** using Hilbert curves for fast tile lookup
- **Multi-level caching** through tileverse-storage integration
- **Block-aligned reads** to minimize cloud storage requests
- **Memory-efficient streaming** for processing large tile sets
- **Thread-safe concurrent access** for server applications

## Further Reading

- **[PMTiles Format Specification](https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md)**: Official specification
- **[Tileverse Documentation](https://tileverse.io)**: Complete documentation for all Tileverse libraries
- **[Tileverse Storage](../tileverse-storage/README.md)**: Learn about the underlying data access layer
