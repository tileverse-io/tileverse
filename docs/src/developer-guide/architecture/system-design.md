# System Design

## Data Flow Architecture

Tileverse allows applications to stream tiled data from remote storage directly to the client (or processing engine) with minimal memory overhead.

### 1. Request Lifecycle

When a `PMTilesReader` requests a tile `(z, x, y)`:

1.  **Spatial Index Lookup**: The reader calculates the Hilbert ID for the requested tile.
2.  **Directory Fetch**: It checks if the relevant directory section is in memory. If not, it issues a byte-range request to the `RangeReader`.
3.  **Offset Calculation**: Using the directory data, it finds the exact byte offset and length of the tile data in the archive.
4.  **Data Fetch**: It issues a second byte-range request to the `RangeReader` for the actual tile body, exposed to callers as a streaming `InputStream`.
5.  **Optional Decoding**: `PMTilesReader.getTile(...)` itself just returns the raw bytes. Higher-level wrappers run a decoder against the same stream: `PMTilesVectorTileStore` feeds it to `VectorTileCodec` to produce a `VectorTile`; `PMTilesRasterTileStore` feeds it to `javax.imageio.ImageIO` to produce a `RenderedImage`. Decoding the tile body never materializes an intermediate `ByteBuffer` because the streaming `IOFunction<InputStream, D>` overload runs the mapper directly on the channel-backed stream.

### 2. Caching Strategy

To avoid network latency, the `CachingRangeReader` implements a two-tier cache:

*   **Tier 1 (Metadata)**: PMTiles directories and headers are highly cacheable and small. They are prioritized in the "Meta" cache.
*   **Tier 2 (Data)**: Actual tile data is cached in a separate "Data" cache (optional), useful for hot areas of the map.

### 3. Extensibility

The system is designed for extension via interfaces:

*   Implement `RangeReader` to support a new storage backend (e.g., Hadoop HDFS, FTP).
*   Implement `TileMatrixSet` to support exotic non-Earth coordinate systems.