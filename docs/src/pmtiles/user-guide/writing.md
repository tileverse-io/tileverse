# Writing PMTiles

This guide covers creating PMTiles archives.

!!! note "Coming Soon"
    Detailed documentation for PMTiles writing is under development. For now, refer to the JavaDoc API documentation and code examples in the source repository.

## Basic Writing Example

```java
import io.tileverse.pmtiles.PMTilesWriter;
import java.nio.file.Path;

// Create a new PMTiles archive
PMTilesWriter writer = PMTilesWriter.create(Path.of("output.pmtiles"))
    .tileType("mvt")  // Vector tiles
    .minZoom(0)
    .maxZoom(14)
    .build();

// Add tiles
writer.addTile(10, 885, 412, tileData);

// Close when done
writer.close();
```

## Tile Format

PMTiles supports multiple tile formats:

- **MVT**: Mapbox Vector Tiles (Protocol Buffers)
- **PNG**: Raster images with transparency
- **JPEG**: Raster images
- **WEBP**: Modern image format

## Best Practices

1. **Write tiles in Hilbert order** for optimal spatial locality
2. **Set appropriate compression** for your tile type
3. **Include metadata** in the header (attribution, description, etc.)
4. **Validate tiles** before adding to archive

## See Also

- [Reading PMTiles](reading.md)
- [PMTiles Specification](https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md)
