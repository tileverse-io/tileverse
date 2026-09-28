# Reading PMTiles

This guide covers advanced topics for reading PMTiles archives.

## Opening PMTiles Archives

`PMTilesReader.open(URI)` is the one-line entry point: it opens the parent `Storage` for the URI's container, gets a `RangeReader` for the leaf, and bundles them so closing the reader releases both. The same call works for any URI scheme.

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

    // Tile type byte (compare against PMTilesHeader.TILETYPE_MVT, _PNG, _JPEG, _WEBP, _AVIF).
    byte tileType = header.tileType();
    String mimeType = header.tileMimeType();   // e.g. "application/vnd.mapbox-vector-tile", "image/webp"
    boolean isRaster = header.isRasterTileType();

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

Tiles are addressed by a `TileIndex` built from the standard Z/X/Y coordinates. The returned buffer is positioned at the start of the tile bytes and ready to read. Do not flip it.

```java
import io.tileverse.tiling.pyramid.TileIndex;

Optional<ByteBuffer> tileData = reader.getTile(TileIndex.zxy(zoom, x, y));

if (tileData.isPresent()) {
    ByteBuffer tile = tileData.get();
    // Process tile data...
} else {
    // Tile doesn't exist in the archive
}
```

`getTile(long tileId)` takes the scalar PMTiles tile id instead, and `reader.getTileId(tileIndex)` converts between the two.

## Bulk Tile Operations

### Reading a Tile Range

```java
int zoom = 10;
for (int x = 880; x <= 890; x++) {
    for (int y = 410; y <= 420; y++) {
        Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(zoom, x, y));
        if (tile.isPresent()) {
            processTile(zoom, x, y, tile.get());
        }
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
            reader.getTile(TileIndex.zxy(zoom, x, y)).ifPresent(buffer ->
                processTile(zoom, x, y, buffer));
        });
    });
```

## High-Level Tile Stores

`PMTilesReader.getTile(...)` returns the raw on-disk tile bytes. For most consumers it's more convenient to work with the decoded tile model. The `*TileStore` classes wrap a `PMTilesReader` and expose a uniform `TileStore<T>` API parameterized on the decoded payload type, while reusing the same `TileMatrixSet` math for tile coordinate / extent conversions.

| Archive type | Wrapper class | Payload type | Decoder |
| :--- | :--- | :--- | :--- |
| MVT (`tileType == TILETYPE_MVT`) | `PMTilesVectorTileStore` | `io.tileverse.vectortile.model.VectorTile` | `VectorTileCodec` |
| Raster (`TILETYPE_PNG/JPEG/WEBP/AVIF`) | `PMTilesRasterTileStore` | `java.awt.image.RenderedImage` | `javax.imageio.ImageIO` |

Both wrappers validate the archive's `tileType` at construction and throw `UnsupportedTileTypeException` if you point a vector store at a raster archive (or vice versa). A misconfigured URI fails fast.

For the full API (interfaces, base classes, the TileJSON v3 model) see the [Tile Stores Reference](../reference/tile-stores.md).

### Reading Vector Tiles

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.store.PMTilesVectorTileStore;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;

try (PMTilesReader reader = PMTilesReader.open(uri)) {
    PMTilesVectorTileStore store = new PMTilesVectorTileStore(reader);

    Tile tile = store.matrixSet().getTileMatrix(10).tile(TileIndex.xyz(885, 412, 10)).orElseThrow();
    Optional<TileData<VectorTile>> data = store.loadTile(tile);

    data.ifPresent(td -> {
        VectorTile vt = td.data();
        // walk layers / features...
    });
}
```

### Reading Raster Tiles

`PMTilesRasterTileStore` decodes WebP/PNG/JPEG straight from the channel via `ImageIO.read`. The encoded payload never sits in a separately allocated `ByteBuffer`. WebP support ships with `tileverse-pmtiles` (via a transitively pulled-in `ImageIO` plugin); PNG and JPEG decoders come with the JDK.

```java
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.store.PMTilesRasterTileStore;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.store.TileData;
import java.awt.image.RenderedImage;
import java.util.Optional;

try (PMTilesReader reader = PMTilesReader.open(uri)) {
    PMTilesRasterTileStore store = new PMTilesRasterTileStore(reader);

    System.out.println("MIME type: " + store.mimeType());  // "image/webp", "image/png", etc.

    Tile tile = store.matrixSet().getTileMatrix(10).tile(TileIndex.xyz(885, 412, 10)).orElseThrow();
    Optional<TileData<RenderedImage>> data = store.loadTile(tile);

    data.ifPresent(td -> {
        RenderedImage img = td.data();
        // hand to javax.imageio.ImageIO.write, paint into a BufferedImage, build a GridCoverage2D, ...
    });
}
```

The store's payload type is the `RenderedImage` interface (not the concrete `BufferedImage` that `ImageIO.read` produces). The result flows directly into APIs like `GridCoverageFactory.create(String, RenderedImage, ReferencedEnvelope)` without intermediate copies.

If you need the raw encoded bytes (e.g. an HTTP proxy that serves WebP straight to clients), bypass the store and use the streaming `PMTilesReader` overload with your own mapper, without a `ByteBuffer` allocation or a second copy:

```java
import io.tileverse.io.IOFunction;

IOFunction<InputStream, byte[]> toBytes = in -> in.readAllBytes();
Optional<byte[]> encoded = reader.getTile(reader.getTileId(TileIndex.xyz(885, 412, 10)), toBytes);
```

## Performance Tips

1. **Use caching** for cloud storage sources
2. **Cache the reader** with `CachingRangeReader.of(...)`: it declares its own block-aligned regions for the header and directories, and tile reads stay exact
3. **Reuse readers** instead of creating new instances
4. **Batch operations** when processing multiple tiles

See [Cloud Storage](cloud-storage.md) for detailed performance optimization strategies.

## Error Handling

```java
try (PMTilesReader reader = new PMTilesReader(rangeReader)) {
    Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(zoom, x, y));
    // Process tile...
} catch (InvalidHeaderException e) {
    // Not a PMTiles archive, or a version this reader does not support
    System.err.println("Invalid PMTiles header: " + e.getMessage());
} catch (IOException e) {
    // I/O errors (network issues, missing object) and UnsupportedTileTypeException
    System.err.println("Failed to read PMTiles: " + e.getMessage());
}
```

## Next Steps

- **[Cloud Storage](cloud-storage.md)**: Optimize for cloud storage
