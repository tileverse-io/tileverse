# Reading PMTiles

This guide covers advanced topics for reading PMTiles archives.

## Opening PMTiles Archives

`PMTilesReader.open(URI)` is the one-line entry point: it opens the parent {@link Storage} for the URI's container, gets a `RangeReader` for the leaf, and bundles them so closing the reader releases both. The same call works for any URI scheme.

```java
import io.tileverse.pmtiles.PMTilesReader;

// Local file
PMTilesReader fileReader = PMTilesReader.open(Path.of("tiles.pmtiles").toUri());

// HTTP
PMTilesReader httpReader = PMTilesReader.open(URI.create("https://example.com/tiles.pmtiles"));

// S3, GCS, Azure - same shape
PMTilesReader s3Reader = PMTilesReader.open(URI.create("s3://bucket/tiles.pmtiles"));
```

For configuration (region, credentials, endpoint overrides, custom HTTP auth), open a `Storage` explicitly with `Properties`, then pass its `RangeReader` to `new PMTilesReader(reader)`:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import java.util.Properties;

Properties props = new Properties();
props.setProperty("storage.s3.region", "us-west-2");

URI bucket = URI.create("s3://bucket/");
URI leaf = URI.create("s3://bucket/tiles.pmtiles");
try (Storage storage = StorageFactory.open(bucket, props);
        RangeReader reader = storage.openRangeReader(leaf);
        PMTilesReader pmtiles = new PMTilesReader(reader)) {
    // ...
}
```

## Reading Header Information

The header contains essential metadata about the tileset:

```java
try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
    PMTilesHeader header = reader.getHeader();

    // Tile format (MVT, PNG, JPEG, WEBP, etc.)
    String tileType = header.tileType();

    // Zoom level range
    int minZoom = header.minZoom();
    int maxZoom = header.maxZoom();

    // Geographic bounds (in E7 format: degrees * 10,000,000)
    double minLon = header.minLonE7() / 10_000_000.0;
    double minLat = header.minLatE7() / 10_000_000.0;
    double maxLon = header.maxLonE7() / 10_000_000.0;
    double maxLat = header.maxLatE7() / 10_000_000.0;

    System.out.printf("Bounds: [%.6f, %.6f, %.6f, %.6f]%n",
        minLon, minLat, maxLon, maxLat);
}
```

## Reading Individual Tiles

Tiles are retrieved using the standard Z/X/Y addressing:

```java
Optional<ByteBuffer> tileData = reader.getTile(zoom, x, y);

if (tileData.isPresent()) {
    ByteBuffer tile = tileData.get();
    tile.flip(); // Important: flip the buffer before reading
    // Process tile data...
} else {
    // Tile doesn't exist in the archive
}
```

## Bulk Tile Operations

### Reading a Tile Range

```java
int zoom = 10;
for (int x = 880; x <= 890; x++) {
    for (int y = 410; y <= 420; y++) {
                    Optional<ByteBuffer> tile = reader.getTile(zoom, x, y);
                    if (tile.isPresent()) {
                        tile.get().flip(); // Flip before processing
                        processTile(zoom, x, y, tile.get());        }
    }
}
```

### Parallel Processing

PMTilesReader is thread-safe for read operations:

```java
IntStream.range(880, 891)
    .parallel()
    .forEach(x -> {
        IntStream.range(410, 421).forEach(y -> {
            reader.getTile(zoom, x, y).ifPresent(buffer -> {
                buffer.flip();
                processTile(zoom, x, y, buffer);
            });
        });
    });
```

## Performance Tips

1. **Use caching** for cloud storage sources
2. **Enable block alignment** for optimal read patterns
3. **Reuse readers** instead of creating new instances
4. **Batch operations** when processing multiple tiles

See [Cloud Storage](cloud-storage.md) for detailed performance optimization strategies.

## Error Handling

```java
try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
    Optional<byte[]> tile = reader.getTile(zoom, x, y);
    // Process tile...
} catch (UncheckedIOException e) {
    // Handle I/O errors (network issues, file not found, etc.)
    System.err.println("Failed to read PMTiles: " + e.getMessage());
} catch (Exception e) {
    // Handle other errors (invalid format, etc.)
    System.err.println("Error: " + e.getMessage());
}
```

## Next Steps

- **[Writing PMTiles](writing.md)**: Create your own PMTiles archives
- **[Cloud Storage](cloud-storage.md)**: Optimize for cloud storage
