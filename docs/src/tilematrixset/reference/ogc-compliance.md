# OGC Compliance

`tileverse-tilematrixset` implements the data model and the JSON/XML encodings defined by [OGC Two Dimensional Tile Matrix Set 17-083r2](https://docs.ogc.org/is/17-083r2/17-083r2.html). The summary excerpt of the spec is kept alongside the source as `tileverse-tilematrixset/spec.md`.

## Data model mapping

| OGC element | Java class | Notes |
|-------------|------------|-------|
| `TileMatrixSet` | `io.tileverse.tiling.matrix.TileMatrixSet` | Interface; default implementation is `StandardTileMatrixSet` |
| `TileMatrix` | `io.tileverse.tiling.matrix.TileMatrix` | Java record |
| `TileMatrixLimits` | `io.tileverse.tiling.matrix.TileMatrixLimits` | clause 7.3 |
| `TileMatrixSetLimits` | `io.tileverse.tiling.matrix.TileMatrixSetLimits` | clause 7.3 |
| `TileMatrixSetLink` | `io.tileverse.tiling.matrix.TileMatrixSetLink` | clause 7.4 |
| `VariableMatrixWidth` | `io.tileverse.tiling.matrix.VariableMatrixWidth` | clause 7.2 |
| `BoundingBox` | `io.tileverse.tiling.common.BoundingBox2D` | |

### TileMatrixSet fields

| OGC field | Accessor | Required |
|-----------|----------|----------|
| identifier | `identifier()` | yes |
| title | `title()` (Optional) | no |
| abstract | `abstractDescription()` (Optional) | no |
| keywords | `keywords()` | no |
| supportedCRS | `supportedCRS()` (URI) | yes |
| wellKnownScaleSet | `wellKnownScaleSet()` (Optional URI) | no |
| boundingBox | `boundingBox()` | yes (kept for compatibility) |
| tileMatrix | `tileMatrices()` | yes |

### TileMatrix fields

| OGC field | Accessor | Notes |
|-----------|----------|-------|
| identifier | `identifier()` | derived from `zoomLevel()` |
| scaleDenominator | `scaleDenominator()` | derived via `CrsUnits` (0.28 mm pixel) |
| topLeftCorner | `topLeftCorner()` | derived from `boundingBox()` |
| cellSize / resolution | `resolution()` | map units per pixel |
| tileWidth, tileHeight | `tileWidth()`, `tileHeight()` | |
| matrixWidth, matrixHeight | `matrixWidth()`, `matrixHeight()` | |
| cornerOfOrigin | `cornerOfOrigin()` | `TOP_LEFT` by default |

### Scale algebra

`CrsUnits` provides the standard 0.28 mm rendering pixel conversions defined by OGC 17-083r2 clause 8 (Annex B):

```java
double scaleDenominator = CrsUnits.resolutionToScaleDenominator(resolution, "EPSG:3857");
double resolution       = CrsUnits.scaleDenominatorToResolution(scaleDenominator, "EPSG:4326");
```

It recognises the CRSs of the bundled standard sets (EPSG:3857, EPSG:4326, CRS:84) and defaults to a meters-per-unit coefficient of 1 for unknown CRSs.

## JSON encoding (Annex C)

`TileMatrixSetIO` round-trips through the Jackson records in `io.tileverse.jackson.databind.tms.v1`, using the field names from the JSON Schema:

```java
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.matrix.TileMatrixSetIO;

TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
String json = TileMatrixSetIO.toJSON(tms);

// Or stream-based:
TileMatrixSetIO.writeJSON(tms, System.out);
TileMatrixSet parsed = TileMatrixSetIO.readJSON(json);
```

The serialised output follows the OGC structure:

```json
{
  "id": "WebMercatorQuad",
  "crs": "http://www.opengis.net/def/crs/EPSG/0/3857",
  "wellKnownScaleSet": "http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible",
  "boundingBox": {
    "lowerLeft": [-20037508.3427892, -20037508.3427892],
    "upperRight": [20037508.3427892, 20037508.3427892],
    "crs": "http://www.opengis.net/def/crs/EPSG/0/3857"
  },
  "tileMatrices": [
    {
      "id": "0",
      "scaleDenominator": 559082264.0287178,
      "cellSize": 156543.0339280410,
      "pointOfOrigin": [-20037508.3427892, 20037508.3427892],
      "tileWidth": 256,
      "tileHeight": 256,
      "matrixWidth": 1,
      "matrixHeight": 1
    }
  ]
}
```

The reader accepts JSON that declares either `cellSize`, `scaleDenominator`, or both, and normalizes a URI-form CRS such as `http://www.opengis.net/def/crs/EPSG/0/3857` back to `EPSG:3857` while preserving the original URI in `supportedCRS()`.

## XML encoding (Annex D)

XML support uses StAX, requiring no extra dependencies:

```java
import io.tileverse.tiling.matrix.TileMatrixSetIO;

TileMatrixSet tms = TileMatrixSetIO.readXML(inputStream);
TileMatrixSetIO.writeXML(tms, outputStream);
```

The writer emits a single default namespace (`http://www.opengis.net/tms/1.0`). The reader is namespace-agnostic and also accepts the historical WMTS 1.0 / OWS 1.1 element layout.
