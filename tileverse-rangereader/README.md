# Tileverse Range Reader

A modern Java library for efficient random access to byte ranges from local files, HTTP servers, and cloud storage services (S3, Azure Blob, Google Cloud Storage).

> **Note**: This is part of the [Tileverse](../) project. See the [main README](../README.md) for project overview, requirements, and contributing guidelines.

**📖 [Documentation](https://tileverse-io.github.io/tileverse-rangereader/)** | **🚀 [Quick Start](https://tileverse-io.github.io/tileverse-rangereader/user-guide/quick-start/)** | **🏗️ [Architecture](https://tileverse-io.github.io/tileverse-rangereader/arc42/)**

## Why Tileverse Range Reader?

- **🌐 Universal Access**: Read ranges from files, HTTP, S3, Azure, GCS with a unified API
- **⚡ High Performance**: Multi-level caching, block alignment, and cloud optimizations
- **🔒 Thread-Safe**: Designed for concurrent server environments
- **🧩 Composable**: Decorator pattern for flexible feature combinations
- **📦 Modular**: Include only the providers you need

## Quick Example

```java
// Read ranges from any source with the same API
try (RangeReader reader = S3RangeReader.builder()
        .uri(URI.create("s3://my-bucket/data.bin"))
        .withCaching()
        .build()) {
    
    ByteBuffer chunk = reader.readRange(1024, 512); // Read 512 bytes at offset 1024
}
```

## Installation

**Maven:**
```xml
<dependency>
    <groupId>io.tileverse.rangereader</groupId>
    <artifactId>tileverse-rangereader-all</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'io.tileverse.rangereader:tileverse-rangereader-all:1.0-SNAPSHOT'
```

For modular installations and cloud-specific dependencies, see the **[Installation Guide](https://tileverse-io.github.io/tileverse-rangereader/user-guide/installation/)**.

## Key Features

| Feature | Description | Documentation |
|---------|-------------|---------------|
| **Multiple Sources** | Local files, HTTP, S3, Azure Blob, Google Cloud Storage | [Data Sources](https://tileverse-io.github.io/tileverse-rangereader/user-guide/data-sources/) |
| **Authentication** | AWS credentials, Azure SAS tokens, HTTP auth, and more | [Authentication](https://tileverse-io.github.io/tileverse-rangereader/user-guide/authentication/) |
| **Performance Optimization** | Memory/disk caching, block alignment, concurrent access | [Performance](https://tileverse-io.github.io/tileverse-rangereader/user-guide/performance/) |
| **Configuration** | Flexible builder pattern with sensible defaults | [Configuration](https://tileverse-io.github.io/tileverse-rangereader/user-guide/configuration/) |

## Use Cases

- **Geospatial Applications**: Efficient access to PMTiles, GeoTIFF, and other tiled formats
- **Media Processing**: Extract specific segments from large audio/video files
- **Data Analysis**: Random access to large datasets without full loading
- **Cloud-Native Applications**: Optimize data access across cloud storage services

## Documentation

- **[User Guide](https://tileverse-io.github.io/tileverse-rangereader/user-guide/)**: Installation, usage, and configuration
- **[Developer Guide](https://tileverse-io.github.io/tileverse-rangereader/developer-guide/)**: Building, testing, and contributing
- **[Architecture](https://tileverse-io.github.io/tileverse-rangereader/arc42/)**: Design decisions and technical details
- **[Examples](https://tileverse-io.github.io/tileverse-rangereader/user-guide/examples/)**: Common usage patterns and best practices

## Related Modules

This library is used by other Tileverse modules:

- **[tileverse-pmtiles](../tileverse-pmtiles/)**: PMTiles format support
- **[tileverse-vectortiles](../tileverse-vectortiles/)**: Mapbox Vector Tiles support
- **[tileverse-tilematrixset](../tileverse-tilematrixset/)**: Tile matrixset models
