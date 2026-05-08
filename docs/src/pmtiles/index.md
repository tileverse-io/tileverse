# PMTiles

This module provides a pure Java implementation of the [PMTiles v3 specification](https://github.com/protomaps/PMTiles), a single-file archive format for cloud-optimized tiled data.

This library is focused on efficient, cloud-optimized reading of PMTiles archives. For creating archives, use one of the existing tools such as [`pmtiles`](https://github.com/protomaps/go-pmtiles) or [Tippecanoe](https://github.com/felt/tippecanoe).

## Features

- **Cloud-Optimized Reads**: Range-based access tuned for archives served from object storage.
- **Storage Agnostic**: Works seamlessly with any [`RangeReader`](../storage/rangereader/index.md) backend from `tileverse-storage` (S3, HTTP, Local File, Azure Blob, GCS).
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

The `PMTilesReader` accepts either a `RangeReader` or a `Supplier<SeekableByteChannel>`. The `RangeReader` can provide this via its `asByteChannel()` method.

```java
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import java.nio.file.Path;

try (PMTilesReader reader = PMTilesReader.open(Path.of("data/map.pmtiles").toUri())) {
    // Get metadata
    PMTilesHeader header = reader.getHeader();
    System.out.println("Min Zoom: " + header.minZoom());

    // Fetch tile (z, x, y)
    reader.getTile(0, 0, 0).ifPresent(buffer -> {
        buffer.flip(); // Important: flip the buffer before reading
        // Process tile bytes...
    });
}
```
