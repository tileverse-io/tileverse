# Reading Versatiles archives

This guide shows how to open a Versatiles archive, read its tiles, and decode them.

## Open an archive

`VersatilesReader.open` opens the `Storage` of the parent location and gets a `RangeReader` for the file. Closing the `VersatilesReader` closes both. The call accepts a local path, or a URI with a scheme supported by `tileverse-storage`. The cloud schemes need their storage module, as described in [Installation](../index.md#installation).

```java
import io.tileverse.versatiles.VersatilesReader;
import java.net.URI;
import java.nio.file.Path;

// Local file
VersatilesReader fileReader = VersatilesReader.open(Path.of("map.versatiles"));

// HTTP
VersatilesReader httpReader = VersatilesReader.open(URI.create("https://example.com/map.versatiles"));

// S3, Azure, and GCS use the same call
VersatilesReader s3Reader = VersatilesReader.open(URI.create("s3://bucket/map.versatiles"));
```

To configure the storage backend, for example its region, credentials, or endpoint, pass a `StorageConfig`. The [storage configuration guide](../../storage/how-to/configure.md) lists the parameters.

```java
import io.tileverse.storage.StorageConfig;

StorageConfig config = new StorageConfig(URI.create("s3://bucket/map.versatiles"))
        .setParameter("storage.s3.region", "eu-central-1");

try (VersatilesReader reader = VersatilesReader.open(config)) {
    // ...
}
```

To read through a `RangeReader` opened by your own code, pass it to the constructor. The `VersatilesReader` takes ownership of the `RangeReader` and closes it in `close()`. The following example caches the ranges read from S3 in memory:

```java
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.cache.CachingRangeReader;

try (Storage storage = StorageFactory.open(URI.create("s3://bucket/"));
        VersatilesReader reader = new VersatilesReader(CachingRangeReader.of(storage.openRangeReader("map.versatiles")))) {
    // ...
}
```

Opening an archive reads its header, its metadata, and its block index. An `InvalidHeaderException` means the file is not a Versatiles 2 container.

## Read the header and metadata

```java
import io.tileverse.geom.BoundingBox2D;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.jackson.databind.versatiles.v2.VersatilesMetadata;
import io.tileverse.versatiles.Compression;
import io.tileverse.versatiles.TileFormat;
import io.tileverse.versatiles.VersatilesHeader;

VersatilesHeader header = reader.getHeader();
TileFormat format = header.tileFormat();         // PBF, PNG, JPEG, WEBP, ...
String mimeType = format.mimeType();             // "application/x-protobuf", "image/webp", ...
Compression compression = header.compression();  // NONE, GZIP, BROTLI, or ZSTD
int minZoom = header.minZoom();
int maxZoom = header.maxZoom();
BoundingBox2D bounds = header.geographicBoundingBox();  // WGS84 longitude and latitude, in degrees

VersatilesMetadata metadata = reader.getMetadata();
String name = metadata.name();
List<VectorLayer> layers = metadata.vectorLayers();

String json = reader.getMetadataAsString();  // the TileJSON document as stored in the archive
```

`VersatilesMetadata` exposes the common TileJSON fields. The fields without a dedicated getter are in `metadata.extras()`.

## Read tiles

`getTile` returns the bytes of a tile after decompression, or an empty `Optional` when the archive has no tile at that position. The buffer is ready to read. Do not flip it.

```java
import io.tileverse.tiling.pyramid.TileIndex;
import java.nio.ByteBuffer;
import java.util.Optional;

Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(14, 8261, 6050));
```

A position outside the zoom range of the archive, or outside the tile grid of its zoom level, returns an empty `Optional`.

To decode a tile as it is read, pass a function to `getTile`. The function receives the decompressed bytes as an `InputStream`, and `getTile` returns its result. This overload avoids a copy of the tile into a `ByteBuffer`:

```java
import io.tileverse.io.IOFunction;
import java.io.InputStream;

IOFunction<InputStream, byte[]> toBytes = InputStream::readAllBytes;
Optional<byte[]> bytes = reader.getTile(TileIndex.zxy(14, 8261, 6050), toBytes);
```

`VersatilesReader` is thread-safe. Several threads can read tiles from one reader at the same time.

## Decode tiles with a tile store

The tile stores wrap a `VersatilesReader` and implement the `TileStore<T>` API described in the [Tile Stores Reference](../../pmtiles/reference/tile-stores.md). They decode tiles to the payload type of the store:

| Tile format | Store | Payload |
| :--- | :--- | :--- |
| `PBF` (Mapbox Vector Tiles) | `VersatilesVectorTileStore` | `io.tileverse.vectortile.model.VectorTile` |
| `PNG`, `JPEG`, `WEBP`, `AVIF` | `VersatilesRasterTileStore` | `java.awt.image.RenderedImage` |

A store checks the tile format of the archive when it is created. It throws `UnsupportedTileFormatException` when the archive holds another format. To pick the store from the archive:

```java
import io.tileverse.tiling.store.TileStore;

TileStore<?> store = reader.getTileFormat() == TileFormat.PBF
        ? new VersatilesVectorTileStore(reader)
        : new VersatilesRasterTileStore(reader);
```

The stores borrow the reader. Close the reader when you are done; a store holds no other resources.

The tile matrix set of a store ends at zoom level 24, the last level of `WebMercatorQuad`. The reader returns the tiles of deeper levels, outside the stores.

### Read vector tiles

```java
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.versatiles.store.VersatilesVectorTileStore;

try (VersatilesReader reader = VersatilesReader.open(uri)) {
    VersatilesVectorTileStore store = new VersatilesVectorTileStore(reader);

    Tile tile = store.matrixSet().getTileMatrix(14).tile(TileIndex.xyz(8261, 6050, 14)).orElseThrow();
    Optional<TileData<VectorTile>> data = store.loadTile(tile);

    data.ifPresent(td -> {
        VectorTile vectorTile = td.data();
        // walk layers and features...
    });
}
```

`store.getVectorLayersMetadata()` returns the `vector_layers` of the archive metadata.

### Read raster tiles

```java
import io.tileverse.versatiles.store.VersatilesRasterTileStore;
import java.awt.image.RenderedImage;

try (VersatilesReader reader = VersatilesReader.open(uri)) {
    VersatilesRasterTileStore store = new VersatilesRasterTileStore(reader);

    System.out.println("MIME type: " + store.mimeType());  // "image/webp", "image/png", ...

    Tile tile = store.matrixSet().getTileMatrix(10).tile(TileIndex.xyz(885, 412, 10)).orElseThrow();
    Optional<TileData<RenderedImage>> data = store.loadTile(tile);
}
```

PNG and JPEG decoders come with the JDK, and `tileverse-versatiles` brings a WebP decoder for `javax.imageio`. AVIF has no bundled decoder. Without an `ImageIO` plugin for AVIF, the raster store refuses AVIF archives with `UnsupportedTileFormatException`.

`loadTile` reports I/O errors as `UncheckedIOException`, because the `TileStore` API declares no checked exceptions.

## Share caches across readers

A reader keeps the tile indexes of recently read blocks in a cache. The vector store also caches decoded tiles. Both caches belong to `CacheManager.getDefault()` unless you set another `CacheManager`:

```java
import io.tileverse.cache.CacheManager;

CacheManager cacheManager = CacheManager.newInstance();
reader.cacheManager(cacheManager);

// For a store, this sets the cache manager of the store and of its reader
VersatilesVectorTileStore store = new VersatilesVectorTileStore(reader).cacheManager(cacheManager);
```

Readers of the same archive share entries in the same `CacheManager`. The [Format support](../reference/format-support.md#caches) reference lists the cache names and bounds.

## Handle errors

```java
import io.tileverse.storage.StorageException;
import io.tileverse.versatiles.InvalidHeaderException;
import java.io.IOException;

try (VersatilesReader reader = VersatilesReader.open(uri)) {
    Optional<ByteBuffer> tile = reader.getTile(TileIndex.zxy(zoom, x, y));
} catch (InvalidHeaderException e) {
    // Not a Versatiles 2 container, or a header with an unknown tile format or compression
} catch (StorageException e) {
    // The storage failed; NotFoundException when the file is missing
} catch (IOException e) {
    // A damaged index, or a file ending before a part it declares
}
```

`StorageException` (in `io.tileverse.storage`) is unchecked.

## Performance tips

1. **Reuse readers.** Opening a reader reads the header, the metadata, and the block index.
2. **Cache remote reads.** Wrap the `RangeReader` of a remote archive in a `CachingRangeReader`, as shown in [Open an archive](#open-an-archive).
3. **Read neighboring tiles together.** The first tile read from a block loads the tile index of the block. The following tiles of that block reuse it from the cache.
