# PMTiles

This module provides a pure Java implementation of the [PMTiles v3 specification](https://github.com/protomaps/PMTiles), a single-file archive format for cloud-optimized tiled data.

## Features

- **Read & Write**: Full support for creating and consuming PMTiles archives.
- **Storage Agnostic**: Works seamlessly with any [Range Reader](../rangereader/index.md) backend (S3, HTTP, Local File).
- **Spatial Indexing**: Implements Hilbert curve indexing for O(log N) tile lookups.
- **Type Support**: Handles both vector (MVT) and raster (PNG, JPG, WebP) tiles.

## Installation

```xml
<dependency>
    <groupId>io.tileverse.pmtiles</groupId>
    <artifactId>tileverse-pmtiles</artifactId>
</dependency>
```

## Usage

### Reading

The `PMTilesReader` accepts a `Supplier<SeekableByteChannel>`. The `RangeReader` can provide this via its `asByteChannel()` method.

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.rangereader.file.FileRangeReader;

var source = FileRangeReader.builder()
    .path(Path.of("data/map.pmtiles"))
    .build();

try (var reader = new PMTilesReader(source::asByteChannel)) {
    // Get metadata
    var header = reader.getHeader();
    System.out.println("Min Zoom: " + header.minZoom());
    
    // Fetch tile (z, x, y)
    reader.getTile(0, 0, 0).ifPresent(buffer -> {
        buffer.flip(); // Important: flip the buffer before reading
        // Process tile bytes...
    });
}
```

### Writing

Writing involves the `PMTilesWriter` which organizes input data into the proper directory structure with Hilbert ordering.

```java
var writer = new PMTilesWriter(outputStream);

// Add tiles (order doesn't matter, writer handles sorting)
writer.addTile(0, 0, 0, tileBytes);

// Finalize and write directory
writer.finish();
```