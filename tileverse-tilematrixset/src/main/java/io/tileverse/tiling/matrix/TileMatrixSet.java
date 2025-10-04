/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.tiling.matrix;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.List;
import java.util.Optional;

/**
 * A Tile Matrix Set defines the relationship between a tile pyramid's discrete grid
 * and a continuous coordinate reference system (CRS). It bridges the gap between
 * tile space (integer coordinates) and map space (real-world coordinates).
 *
 * <p>This implementation is CRS-agnostic and can work with any coordinate system,
 * making it easy to integrate with various GIS libraries without dependencies.
 *
 * <p>Key concepts:
 * <ul>
 * <li><b>Tile Space</b>: Discrete grid coordinates (tile X, Y, Z)</li>
 * <li><b>Map Space</b>: Continuous CRS coordinates (longitude/latitude, projected coordinates, etc.)</li>
 * <li><b>Origin</b>: The map space coordinate corresponding to tile (0,0) at each zoom level</li>
 * <li><b>Resolution</b>: Map units per pixel at each zoom level</li>
 * <li><b>Tile Size</b>: Pixel dimensions of each tile (typically 256x256 or 512x512)</li>
 * </ul>
 *
 * @since 1.0
 */
public interface TileMatrixSet {

    /**
     * Returns the underlying tile pyramid that defines the discrete grid structure.
     *
     * @return the tile pyramid
     */
    TilePyramid tilePyramid();

    /**
     * Returns the identifier for the coordinate reference system used by this tile matrix set.
     * This is typically a CRS code like "EPSG:4326", "EPSG:3857", etc., but can be any
     * string that uniquely identifies the coordinate system.
     *
     * @return the CRS identifier
     */
    String crsId();

    /**
     * Returns the pixel width of tiles in this matrix set.
     * This is typically 256 or 512 pixels.
     *
     * @return the tile width in pixels
     */
    int tileWidth();

    /**
     * Returns the pixel height of tiles in this matrix set.
     * This is typically 256 or 512 pixels.
     *
     * @return the tile height in pixels
     */
    int tileHeight();

    /**
     * Returns the map space resolution (map units per pixel) at the specified zoom level.
     * Higher zoom levels typically have smaller (more detailed) resolutions.
     *
     * @param zoomLevel the zoom level
     * @return the resolution in map units per pixel
     * @throws IllegalArgumentException if the zoom level is not supported
     */
    double resolution(int zoomLevel);

    /**
     * Returns the map space extent covered by this tile matrix set.
     * This defines the bounding box in CRS coordinates that encompasses
     * all tiles at all zoom levels.
     *
     * @return the map space extent
     */
    BoundingBox2D boundingBox();

    default BoundingBox2D extent(TileIndex tileIndex) {
        return extent(tileIndex.x(), tileIndex.y(), 1, 1);
    }

    default BoundingBox2D extent(TileRange range) {
        return extent(range.first().x(), range.first().y(), range.spanX(), range.spanY());
    }

    default BoundingBox2D extent(long tileCol, long tileRow, long tileSpanX, long tileSpanY) {
        BoundingBox2D extent = boundingBox();

        double tileMatrixMinX = extent.minX();
        double tileMatrixMaxY = extent.maxY();

        // upper-left corner
        double leftX = tileCol * tileSpanX + tileMatrixMinX;
        double upperY = tileMatrixMaxY - tileRow * tileSpanY;

        // lower-right corner (rightX, lowerY) of the tile:
        double rightX = (tileCol + 1) * tileSpanX + tileMatrixMinX;
        double lowerY = tileMatrixMaxY - (tileRow + 1) * tileSpanY;

        return new BoundingBox2D(leftX, lowerY, rightX, upperY);
    }

    /**
     * Returns the map space origin coordinate for the all zoom levels.
     *
     * The origin depends on the tile pyramid's axis origin configuration.
     *
     * @return the origin coordinate in map space
     */
    Coordinate origin();

    /**
     * Converts a map space coordinate to the corresponding tile index at the specified zoom level.
     * If the coordinate falls outside the matrix set extent, returns the nearest edge tile.
     *
     * @param coordinate the map space coordinate
     * @param zoomLevel the target zoom level
     * @return the tile index containing the coordinate
     * @throws IllegalArgumentException if the zoom level is not supported
     */
    TileIndex coordinateToTile(Coordinate coordinate, int zoomLevel);

    /**
     * Returns all tile matrices in this set, ordered by zoom level.
     * Each TileMatrix combines a TileRange with its spatial properties.
     *
     * @return the list of tile matrices
     */
    List<TileMatrix> tileMatrices();

    /**
     * Returns the tile matrix for the specified zoom level.
     *
     * @param zoomLevel the zoom level
     * @return the tile matrix for the zoom level, or empty if not present
     */
    Optional<TileMatrix> tileMatrix(int zoomLevel);

    /**
     * Returns the tile matrix for the specified zoom level.
     * Throws an exception if the zoom level is not present.
     *
     * @param zoomLevel the zoom level
     * @return the tile matrix for the zoom level
     * @throws IllegalArgumentException if the zoom level is not supported
     */
    default TileMatrix getTileMatrix(int zoomLevel) {
        return tileMatrix(zoomLevel)
                .orElseThrow(() -> new IllegalArgumentException("Zoom level " + zoomLevel + " not found"));
    }

    default Optional<Tile> tile(TileIndex tileIndex) {
        return tileMatrix(tileIndex.z()).flatMap(m -> m.tile(tileIndex));
    }

    /**
     * Returns the minimum zoom level in this tile matrix set.
     *
     * @return the minimum zoom level
     */
    default int minZoomLevel() {
        return tilePyramid().minZoomLevel();
    }

    /**
     * Returns the maximum zoom level in this tile matrix set.
     *
     * @return the maximum zoom level
     */
    default int maxZoomLevel() {
        return tilePyramid().maxZoomLevel();
    }

    /**
     * Returns true if this tile matrix set contains the specified zoom level.
     *
     * @param zoomLevel the zoom level to check
     * @return true if the zoom level is present
     */
    default boolean hasZoomLevel(int zoomLevel) {
        return tileMatrix(zoomLevel).isPresent();
    }

    /**
     * Returns a new TileMatrixSet containing only the tiles that intersect with the given extent.
     * Each tile matrix in the result is filtered to include only tiles that intersect with the extent.
     * Empty matrices (with no intersecting tiles) are excluded from the result.
     *
     * @param mapExtent the map space extent to intersect with
     * @return a new TileMatrixSet containing only intersecting tiles, or empty if no tiles intersect at any zoom level
     */
    Optional<TileMatrixSet> intersection(BoundingBox2D mapExtent);

    /**
     * Returns a TileMatrix for the specified zoom level containing only tiles that intersect
     * with the given extent. This is a convenience method equivalent to calling
     * {@code getTileMatrix(zoomLevel).intersection(mapExtent)}.
     *
     * @param mapExtent the map space extent to intersect with
     * @param zoomLevel the zoom level for the tile matrix
     * @return a TileMatrix containing only intersecting tiles at the specified zoom level, or empty if no tiles intersect
     */
    default Optional<TileMatrix> intersection(BoundingBox2D mapExtent, int zoomLevel) {
        return getTileMatrix(zoomLevel).intersection(mapExtent);
    }

    /**
     * Returns a subset of this tile matrix set containing only the specified zoom level range.
     * This creates a view that delegates to the original matrix set.
     *
     * @param minZoomLevel the minimum zoom level (inclusive)
     * @param maxZoomLevel the maximum zoom level (inclusive)
     * @return a new TileMatrixSet containing only the specified zoom levels
     * @throws IllegalArgumentException if the zoom range is invalid or not supported
     */
    TileMatrixSet subset(int minZoomLevel, int maxZoomLevel);

    /**
     * Returns a subset of this tile matrix set containing tiles that intersect with the given extent
     * within the specified zoom level range. This combines spatial and zoom filtering.
     *
     * @param mapExtent the map space extent to intersect with
     * @param minZoomLevel the minimum zoom level (inclusive)
     * @param maxZoomLevel the maximum zoom level (inclusive)
     * @return a new TileMatrixSet containing only intersecting tiles in the zoom range
     * @throws IllegalArgumentException if the zoom range is invalid or not supported
     */
    default Optional<TileMatrixSet> intersection(BoundingBox2D mapExtent, int minZoomLevel, int maxZoomLevel) {
        return subset(minZoomLevel, maxZoomLevel).intersection(mapExtent);
    }

    /**
     * Creates a new builder for StandardTileMatrixSet.
     *
     * @return a new builder instance
     */
    static TileMatrixSetBuilder builder() {
        return new TileMatrixSetBuilder();
    }

    /**
     * Creates a builder pre-populated with this tile matrix set's configuration.
     * Useful for creating variants with different zoom level ranges.
     *
     * @return a builder initialized with this tile matrix set's values
     */
    default TileMatrixSetBuilder toBuilder() {
        return StandardTileMatrixSet.toBuilder(this);
    }
}
