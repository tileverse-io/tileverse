# Reading PMTiles archives

This guide shows how to open a PMTiles archive, read its tiles, and decode them.

## Open an archive

`PMTilesReader.open` opens the `Storage` of the parent location and gets a `RangeReader` for the file. Closing the `PMTilesReader` closes both. The call accepts a URI with a scheme supported by `tileverse-storage`. The cloud schemes need their storage module, as described in [Installation](../index.md#installation).

```java
import io.tileverse.pmtiles.PMTilesReader;
import java.net.URI;
import java.nio.file.Path;

// Local file
PMTilesReader fileReader = PMTilesReader.open(Path.of("map.pmtiles").toUri());

// HTTP
PMTilesReader httpReader = PMTilesReader.open(URI.create("https://example.com/map.pmtiles"));

// S3, Azure, and GCS use the same call
PMTilesReader s3Reader = PMTilesReader.open(URI.create("s3://bucket/map.pmtiles"));
```

To configure the storage backend, for example its region, credentials, or endpoint, pass a `StorageConfig`. The [storage configuration guide](../../storage/how-to/configure.md) and the [authentication guide](../../storage/how-to/authenticate.md) list the parameters.

```java
import io.tileverse.storage.StorageConfig;

StorageConfig config = new StorageConfig(URI.create("s3://bucket/map.pmtiles"))
        .setParameter("storage.s3.region", "us-west-2");

try (PMTilesReader reader = PMTilesReader.open(config)) {
    // ...
}
```

To read through a `RangeReader` opened by your own code, pass it to the constructor. The `PMTilesReader` takes ownership of the `RangeReader` and closes it in `close()`. The following example caches the ranges read from S3 in memory:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;

try (Storage storage = StorageFactory.open(URI.create("s3://bucket/"));
        PMTilesReader reader = new PMTilesReader(CachingRangeReader.of(storage.openRangeReader("map.pmtiles")))) {
    // ...
}
```

Opening an archive reads its header and its metadata. An `InvalidHeaderException` means the file is not a PMTiles version 3 archive.

## Read the header and metadata

```java
import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.pmtiles.PMTilesHeader;
import java.util.List;

PMTilesHeader header = reader.getHeader();
byte tileType = header.tileType();          // PMTilesHeader.TILETYPE_MVT, TILETYPE_PNG, ...
String mimeType = header.tileMimeType();    // "application/vnd.mapbox-vector-tile", "image/webp", ...
boolean raster = header.isRasterTileType();
int minZoom = header.minZoom();
int maxZoom = header.maxZoom();

// The bounds are in degrees, stored multiplied by 10,000,000
double minLon = header.minLonE7() / 10_000_000.0;
double minLat = header.minLatE7() / 10_000_000.0;
double maxLon = header.maxLonE7() / 10_000_000.0;
double maxLat = header.maxLatE7() / 10_000_000.0;

PMTilesMetadata metadata = reader.getMetadata();
String name = metadata.name();
List<VectorLayer> layers = metadata.vectorLayers();

String json = reader.getMetadataAsString();  // the metadata document, decompressed
```

## Read tiles

`getTile` returns the bytes of a tile after removing the tile compression of the archive, or an empty `Optional` when the archive has no tile at that position. The buffer is ready to read. Do not flip it.

```java
import io.tileverse.tiling.pyramid.TileIndex;
import java.nio.ByteBuffer;
import java.util.Optional;

Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(10, 885, 412));
```

`getTile(long)` takes the PMTiles tile id instead, and `reader.getTileId(tileIndex)` converts a `TileIndex` to it.

To decode a tile as it is read, pass a function to `getTile` with the tile id. The function receives the tile bytes as an `InputStream`, and `getTile` returns its result. This overload avoids a copy of the tile into a `ByteBuffer`:

```java
import io.tileverse.io.IOFunction;
import java.io.InputStream;

IOFunction<InputStream, byte[]> toBytes = InputStream::readAllBytes;
Optional<byte[]> bytes = reader.getTile(reader.getTileId(TileIndex.zxy(10, 885, 412)), toBytes);
```

`PMTilesReader` is thread-safe. Several threads can read tiles from one reader at the same time:

```java
IntStream.rangeClosed(880, 890).parallel().forEach(x ->
        IntStream.rangeClosed(410, 420).forEach(y -> {
            try {
                reader.getTile(TileIndex.zxy(10, x, y)).ifPresent(tile -> process(x, y, tile));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
```

## Decode tiles with a tile store

The tile stores wrap a `PMTilesReader` and implement the `TileStore<T>` API described in the [Tile Stores Reference](../reference/tile-stores.md). They decode tiles to the payload type of the store:

| Tile type | Store | Payload |
| :--- | :--- | :--- |
| MVT | `PMTilesVectorTileStore` | `io.tileverse.vectortile.model.VectorTile` |
| PNG, JPEG, WebP, AVIF | `PMTilesRasterTileStore` | `java.awt.image.RenderedImage` |

A store checks the tile type of the archive when it is created. It throws `UnsupportedTileTypeException` when the archive holds another type. To pick the store from the archive:

```java
import io.tileverse.tiling.store.TileStore;

TileStore<?> store = reader.getHeader().isRasterTileType()
        ? new PMTilesRasterTileStore(reader)
        : new PMTilesVectorTileStore(reader);
```

The stores borrow the reader. Close the reader when you are done; a store holds no other resources.

### Read vector tiles

```java
import io.tileverse.pmtiles.store.PMTilesVectorTileStore;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;

try (PMTilesReader reader = PMTilesReader.open(uri)) {
    PMTilesVectorTileStore store = new PMTilesVectorTileStore(reader);

    Optional<TileData<VectorTile>> data = store.findTile(TileIndex.zxy(10, 885, 412));
    data.ifPresent(td -> {
        VectorTile vectorTile = td.data();
        // walk layers and features...
    });
}
```

`store.getVectorLayersMetadata()` returns the `vector_layers` of the archive metadata.

### Read raster tiles

```java
import io.tileverse.pmtiles.store.PMTilesRasterTileStore;
import java.awt.image.RenderedImage;

try (PMTilesReader reader = PMTilesReader.open(uri)) {
    PMTilesRasterTileStore store = new PMTilesRasterTileStore(reader);

    System.out.println("MIME type: " + store.mimeType());  // "image/webp", "image/png", ...

    Optional<TileData<RenderedImage>> data = store.findTile(TileIndex.zxy(10, 885, 412));
}
```

PNG and JPEG decoders come with the JDK, and `tileverse-pmtiles` brings a WebP decoder for `javax.imageio`. AVIF needs an `ImageIO` plugin of your own.

`loadTile` reports I/O errors as `UncheckedIOException`, because the `TileStore` API declares no checked exceptions.

## Caches

Keep a reader open and reuse it: opening an archive reads its header and its metadata.

For a remote archive, wrap the `RangeReader` in a `CachingRangeReader`, as shown in [Open an archive](#open-an-archive). The reader aligns its reads of the header, the directories and the metadata to blocks, and the cache keeps those blocks. Tile reads stay exact. The [storage configuration guide](../../storage/how-to/configure.md#memory-cache-cachingrangereader) describes the size of the cache.

A reader also keeps parsed directories in a cache, and the vector store caches decoded tiles. Both caches belong to `CacheManager.getDefault()` unless you set another `CacheManager`:

```java
import io.tileverse.cache.CacheManager;

CacheManager cacheManager = CacheManager.newInstance();
reader.cacheManager(cacheManager);

// For the vector store, this sets the cache manager of the store and of its reader
PMTilesVectorTileStore store = new PMTilesVectorTileStore(reader).cacheManager(cacheManager);
```

## Handle errors

```java
import io.tileverse.pmtiles.InvalidHeaderException;
import io.tileverse.storage.StorageException;
import java.io.IOException;

try (PMTilesReader reader = PMTilesReader.open(uri)) {
    Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(zoom, x, y));
} catch (InvalidHeaderException e) {
    // Not a PMTiles archive, or a version this reader does not support
} catch (StorageException e) {
    // The storage failed; NotFoundException when the file is missing
} catch (IOException e) {
    // Unreadable directories, metadata or tile compression
}
```

`InvalidHeaderException` and `StorageException` (in `io.tileverse.storage`) are unchecked.
