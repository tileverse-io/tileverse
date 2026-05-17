# OGC Two Dimensional Tile Matrix Set Specification
**Category:** OGC Implementation Standard (Version 1.0).
**Reference:** https://docs.ogc.org/is/17-083r2/17-083r2.html

**Purpose:** Defines rules and requirements for a tile matrix set as a way to index space based on regular grids defining a domain for a limited list of scales in a Coordinate Reference System (CRS).

## 1. Core Data Models

### 1.1 TileMatrixSet
A tiling scheme composed of a collection of tile matrices defined at different scales, covering approximately the same area and sharing a common coordinate reference system.
*   **identifier** (`String`): Mandatory identifier, which must be unique for each TileMatrixSet of a server.
*   **title** (`List<String>`): Optional title, normally used for human display.
*   **abstract** (`List<String>`): Optional brief narrative description.
*   **keywords** (`List<String>`): Optional unordered list of words or phrases describing the dataset.
*   **boundingBox** (`BoundingBox`): Optional, but deprecated; it should not be used to calculate the position of tiles in the CRS space.
*   **supportedCRS** (`URI`): Mandatory reference to one coordinate reference system.
*   **wellKnownScaleSet** (`URI`): Optional reference to a well-known scale set, which must be consistent with the `supportedCRS`.
*   **tileMatrix** (`List<TileMatrix>`): Mandatory list containing one or more elements, where each `TileMatrix` must have a unique `scaleDenominator`.

### 1.2 TileMatrix
A grid tiling scheme that defines how space is partitioned into a set of conterminous tiles at a fixed scale.
*   **identifier** (`String`): Mandatory identifier, which must be unique within the context of the parent TileMatrixSet.
*   **title** (`List<String>`): Optional title.
*   **abstract** (`List<String>`): Optional narrative description.
*   **keywords** (`List<String>`): Optional keywords.
*   **scaleDenominator** (`double`): Mandatory scale denominator level for the tile matrix.
*   **topLeftCorner** (`double[]`): Mandatory ordered sequence of double values representing the position in CRS coordinates of the top-left corner.
*   **tileWidth** (`int`): Mandatory positive integer representing the width of each tile in pixels.
*   **tileHeight** (`int`): Mandatory positive integer representing the height of each tile in pixels.
*   **matrixWidth** (`int`): Mandatory positive integer representing the width of the matrix in number of tiles.
*   **matrixHeight** (`int`): Mandatory positive integer representing the height of the matrix in number of tiles.

### 1.3 TileMatrixSetLimits & TileMatrixLimits
This structure declares a limited coverage of a tile matrix set to prevent invalidating previously cached tiles when bounding boxes extend.
*   **TileMatrixSetLimits** contains **tileMatrixLimits** (`List<TileMatrixLimits>`): Mandatory list of one or more limit constraints. Each `tileMatrix` identifier shall be mentioned only once within this set.
*   **TileMatrixLimits** attributes:
    *   **tileMatrix** (`String`): Mandatory reference to a `tileMatrix` identifier.
    *   **minTileRow** (`int`): Mandatory non-negative integer ranging from 0 to `maxTileRow`.
    *   **maxTileRow** (`int`): Mandatory non-negative integer ranging from `minTileRow` to `matrixHeight - 1`.
    *   **minTileCol** (`int`): Mandatory non-negative integer ranging from 0 to `maxTileCol`.
    *   **maxTileCol** (`int`): Mandatory non-negative integer ranging from `minTileCol` to `matrixWidth - 1`.

### 1.4 TileMatrixSetLink
This allows a dataset to declare the use of a tile matrix set defined elsewhere, mapping limits onto it.
*   **tileMatrixSet** (`URI`): Mandatory reference to a tile matrix set identifier.
*   **tileMatrixSetLimits** (`List<TileMatrixSetLimits>`): Optional definition of limits. The absence of this parameter means that tile row and column indices are only limited by 0 and the corresponding `matrixWidth` and `matrixHeight`.

### 1.5 VariableMatrixWidth
This allows compensation for spatial distortion (such as Equirectangular projections at poles) by reducing the number of tiles in high-latitude rows.
*   **coalesce** (`int`): Mandatory positive integer that must be greater than 1.
*   **minTileRow** (`int`): Mandatory non-negative integer ranging from 0 to `maxTileRow`.
*   **maxTileRow** (`int`): Mandatory non-negative integer ranging from `minTileRow` to `matrixHeight - 1`.
*   **Constraint:** Only tile rows with a coalesce factor different from 1 shall be encoded. If a row is not mentioned, a coalesce factor of 1 shall be considered default for that row.

## 2. Mathematical Algorithms & Coordinate Calculations

### 2.1 Standard Rendering Pixel & Unit Conversions
Calculations utilize a standardized rendering pixel size defined as 0.28 mm x 0.28 mm.
*   **metersPerUnit(crs)**: A coefficient to convert the CRS units into meters. If the CRS uses meters, `metersPerUnit` equals 1; if it uses degrees, it equals `2 * PI * a / 360`, where `a` is the Earth's maximum ellipsoid radius.

### 2.2 Tile Bounding Box Computation
Given the top-left point of the tile matrix `(tileMatrixMinX, tileMatrixMaxY)` originating from `topLeftCorner`:
*   **pixelSpan** = `scaleDenominator * (0.28 * 10^-3) / metersPerUnit(crs)`.
*   **tileSpanX** = `tileWidth * pixelSpan`.
*   **tileSpanY** = `tileHeight * pixelSpan`.
*   **tileMatrixMaxX** = `tileMatrixMinX + tileSpanX * matrixWidth`.
*   **tileMatrixMinY** = `tileMatrixMaxY - tileSpanY * matrixHeight`.

### 2.3 Tile Indexing logic
*   **Standard Indexing:** Each tile is identified by a `tileCol` and `tileRow` index, starting with `0,0` at the top-left corner of the matrix, increasing towards the right and bottom respectively.
*   **Coalescence Offset (Variable Matrix):** When `coalesce` factors are applied, the `tileRow` indexing mathematically skips linearly. For example, if `coalesce` is 4, the indices of the first three tiles map to the same row, but logically act as if the original grid bounds still apply.
*   **Grid cell positioning:** When projecting absolute screen pixels onto this space (extrapolated device grid), cell indices can become very large integers, potentially requiring 64-bit numerical representations (e.g., `long` in Java) for the absolute `{i', j'}` grid coordinates.
