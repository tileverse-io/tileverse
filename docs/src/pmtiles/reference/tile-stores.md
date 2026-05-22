# Tile Stores Reference

`PMTilesReader.getTile(...)` returns raw on-disk tile bytes. The `*TileStore` classes wrap a `PMTilesReader` and expose a uniform, format-agnostic API parameterized on the decoded payload type. They live in the `tileverse-tilestore` module and are pulled in transitively when you depend on `tileverse-pmtiles`; no separate dependency is needed.

This page is the API surface reference. For task-oriented walkthroughs see [Reading PMTiles](../how-to/read-pmtiles.md).

## Component view

![tileverse-tilestore components](../../assets/images/storage/structurizr-TileStoreComponents.svg)

`TileStore<T>` is the format-agnostic top-level interface; `AbstractTileStore<T>` owns the `TileMatrixSet`. `VectorTileStore` and `RasterTileStore` are payload-specific abstract bases that PMTiles (and any future archive format) implements. The `TileJSON v3 Model` records expose per-layer metadata advertised by vector archives.

## Module layout

| Package | Type | Purpose |
| :--- | :--- | :--- |
| `io.tileverse.tiling.store` | `TileStore<T>` | Top-level interface: query by extent, by zoom, or one tile at a time. |
| `io.tileverse.tiling.store` | `AbstractTileStore<T>` | Base class that owns the `TileMatrixSet`. |
| `io.tileverse.tiling.store` | `TileData<T>` | Record pairing a `Tile` (spatial metadata) with its decoded payload. |
| `io.tileverse.vectortile.store` | `VectorTileStore` | Abstract base for stores whose payload is a decoded `VectorTile`. |
| `io.tileverse.rastertile.store` | `RasterTileStore` | Abstract base for stores whose payload is a decoded `RenderedImage`. |
| `io.tileverse.pmtiles.store` | `PMTilesVectorTileStore` | PMTiles archive whose `tileType == TILETYPE_MVT`. |
| `io.tileverse.pmtiles.store` | `PMTilesRasterTileStore` | PMTiles archive whose `tileType == TILETYPE_{PNG,JPEG,WEBP,AVIF}`. |
| `io.tileverse.jackson.databind.tilejson.v3` | `TileJSON`, `VectorLayer`, `TilesetType` | TileJSON 3.0.0 model exposed by `VectorTileStore.getVectorLayersMetadata()` and produced by serializers. |

## `TileStore<T>`

```java
public interface TileStore<T> {
    enum Strategy { SPEED, QUALITY }

    TileMatrixSet matrixSet();
    Optional<TileData<T>> loadTile(Tile tile);

    default Stream<TileData<T>> findTiles(List<BoundingBox2D> extents, int zoomLevel);
    default Stream<TileData<T>> findTiles(List<BoundingBox2D> extents, double resolution, Strategy strategy);
    default Optional<TileMatrix> tileMatrix(List<BoundingBox2D> extents, int zoomLevel);
    default int findBestZoomLevel(double resolution, Strategy strategy);
}
```

- `matrixSet()` returns the store's grid model. For PMTiles this is always `WebMercatorQuad`.
- `loadTile(Tile)` is the single primitive every subclass implements. Default methods compose it.
- `findTiles` resolves a list of extents to the intersecting tile range and streams decoded payloads.
- `Strategy` controls zoom selection when a target resolution falls between matrix levels. `SPEED` rounds to the coarser zoom (fewer reads), `QUALITY` rounds to the finer zoom (more detail).

## `TileData<T>`

```java
public record TileData<T>(Tile tile, T data) {}
```

`Tile` carries spatial metadata (extent in CRS units, `TileIndex`, the parent `TileMatrix`). `data` is the decoded payload of type `T`. Useful when a consumer needs the extent to position the tile in a mosaic without re-deriving it.

## `VectorTileStore`

```java
public abstract class VectorTileStore extends AbstractTileStore<VectorTile> {
    public abstract List<VectorLayer> getVectorLayersMetadata();
    public Optional<VectorLayer> getLayerMetadata(String layerId);
    public abstract BoundingBox2D getExtent();
}
```

- `getVectorLayersMetadata()` returns the TileJSON 3.0.0 `vector_layers` array advertised by the archive (layer id, field types, zoom range).
- `getExtent()` is the WGS84 bounding box of the archive, useful for clipping queries.

`PMTilesVectorTileStore` implements this on top of a `PMTilesReader`. It validates the archive's `tileType` at construction and throws `UnsupportedTileTypeException` if the archive is a raster archive.

## `RasterTileStore`

```java
public abstract class RasterTileStore extends AbstractTileStore<RenderedImage> {
    public abstract String mimeType();
    public abstract BoundingBox2D getExtent();
}
```

- `mimeType()` returns the on-disk MIME type of the encoded tiles (`image/png`, `image/jpeg`, `image/webp`, `image/avif`). Useful for HTTP `Content-Type` headers when proxying raw bytes, or for selecting an `ImageWriter` when re-encoding.
- The payload type is the `RenderedImage` interface (not the concrete `BufferedImage` that `ImageIO.read` produces), so the result flows directly into APIs like `GridCoverageFactory.create(String, RenderedImage, ReferencedEnvelope)` without intermediate copies.

`PMTilesRasterTileStore` implements this on top of a `PMTilesReader`. It decodes each tile by feeding the streaming `InputStream` returned by `PMTilesReader.getTile(long, IOFunction<InputStream, D>)` directly to `ImageIO.read`, so no intermediate `ByteBuffer` is allocated per tile.

WebP decoding is enabled out of the box by a bundled `ImageIO` plugin (`com.github.usefulness:webp-imageio`) registered via the JDK `ServiceLoader`. PNG and JPEG decoders ship with the JDK. AVIF is not bundled; add a JDK ImageIO plugin for AVIF if you need it.

## `PMTilesVectorTileStore` / `PMTilesRasterTileStore`

Both wrappers are constructed from an existing `PMTilesReader`. They borrow the reader; closing the wrapper is *not* required (no resources beyond the underlying reader), but closing the reader releases the storage handle.

```java
try (PMTilesReader reader = PMTilesReader.open(uri)) {
    PMTilesVectorTileStore vectorStore = new PMTilesVectorTileStore(reader);
    // or
    PMTilesRasterTileStore rasterStore = new PMTilesRasterTileStore(reader);
}
```

If the archive type doesn't match the wrapper, the constructor throws `UnsupportedTileTypeException`. Callers can branch on `PMTilesHeader.isRasterTileType()` to pick the right wrapper:

```java
PMTilesHeader header = reader.getHeader();
TileStore<?> store = header.isRasterTileType()
        ? new PMTilesRasterTileStore(reader)
        : new PMTilesVectorTileStore(reader);
```

Optional caching: both classes expose a fluent `cacheManager(CacheManager)` setter that attaches an `io.tileverse.cache.CacheManager` for memoizing decoded payloads at the store level. The default is no cache.

## TileJSON v3 model

The `io.tileverse.jackson.databind.tilejson.v3` package contains a Jackson-annotated record model of the [TileJSON 3.0.0 specification](https://github.com/mapbox/tilejson-spec/tree/master/3.0.0):

| Type | Role |
| :--- | :--- |
| `TileJSON` | Top-level record. Required fields: `tilejson`, `tiles`, `vector_layers`. Optional: `attribution`, `bounds`, `center`, `description`, etc. |
| `VectorLayer` | Per-layer descriptor: `id`, `fields`, `description`, `minzoom`, `maxzoom`. Returned by `VectorTileStore.getVectorLayersMetadata()`. |
| `TilesetType` | Enum: `OVERLAY` or `BASELAYER`, serialized as the JSON-spec strings. |

The records use Jackson annotations directly; no custom mapper is required. The package is consumed by `VectorTileStore` and is the format the PMTiles reader pulls per-layer metadata from when present in the archive's JSON `metadata` blob.
