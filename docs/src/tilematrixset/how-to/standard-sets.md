# Standard Tile Matrix Sets

`DefaultTileMatrixSets` exposes the most common tile matrix sets as immutable static constants. Each instance is fully populated with an OGC identifier, the `supportedCRS` URI, and, when applicable, the matching well-known scale set URI.

## WebMercatorQuad

The OGC standard set used by Google Maps, OpenStreetMap, Mapbox, Leaflet, OpenLayers and almost every web mapping client.

- **identifier**: `WebMercatorQuad`
- **CRS**: EPSG:3857 (Spherical Mercator, meters)
- **wellKnownScaleSet**: `http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible`
- **Origin**: top-left
- **Structure**: 1 tile at zoom 0, four child tiles at each subsequent level

```java
TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
TileMatrixSet tms512 = DefaultTileMatrixSets.WEB_MERCATOR_QUADx2; // 512 px tiles
```

## WorldCRS84Quad

OGC standard set for geographic (longitude/latitude) tile pyramids. Useful when you want to avoid reprojection to Web Mercator.

- **identifier**: `WorldCRS84Quad`
- **CRS**: EPSG:4326 (WGS84 lon/lat)
- **wellKnownScaleSet**: `http://www.opengis.net/def/wkss/OGC/1.0/GoogleCRS84Quad`
- **Origin**: top-left
- **Structure**: 2x1 tiles at zoom 0 covering `[-180, -90, 180, 90]`

```java
TileMatrixSet tms = DefaultTileMatrixSets.WORLD_CRS84_QUAD;
TileMatrixSet tms512 = DefaultTileMatrixSets.WORLD_CRS84_QUADx2; // 512 px tiles
```

## Legacy GeoWebCache-style sets

These match the GeoWebCache resolution arrays used by older deployments. They share the underlying CRS with the OGC sets above but do not carry a well-known scale set URI.

| Constant | Identifier | CRS | Tile size |
|----------|------------|-----|-----------|
| `WORLD_EPSG3857` | `WORLD_EPSG3857` | EPSG:3857 | 256 |
| `WORLD_EPSG3857x2` | `WORLD_EPSG3857x2` | EPSG:3857 | 512 |
| `WORLD_EPSG4326` | `WORLD_EPSG4326` | EPSG:4326 | 256 |
| `WORLD_EPSG4326x2` | `WORLD_EPSG4326x2` | EPSG:4326 | 512 |

## Common properties

Every standard set is thread-safe and immutable, and exposes:

- the canonical `identifier()` (mandatory in OGC TMS 2D),
- a `supportedCRS()` URI suitable for OGC API - Tiles consumption,
- precomputed `resolution(int z)` and matching `scaleDenominator()` values calculated from the standard 0.28 mm rendering pixel size,
- a `boundingBox()` covering the full CRS extent,
- a `tilePyramid()` exposing the per-level tile ranges.
