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
     * Builder for creating StandardTileMatrixSet instances.
     */
    class Builder {
        private TilePyramid tilePyramid;
        private String crsId;
        private int tileWidth = 256;
        private int tileHeight = 256;
        private Extent extent;
        private double[] resolutions;
        private Coordinate[] origins;

        /**
         * Sets the tile pyramid that defines the discrete grid structure.
         *
         * @param tilePyramid the tile pyramid
         * @return this builder
         */
        public Builder tilePyramid(TilePyramid tilePyramid) {
            this.tilePyramid = tilePyramid;
            return this;
        }

        /**
         * Sets the coordinate reference system identifier.
         *
         * @param crsId the CRS identifier (e.g., "EPSG:4326", "EPSG:3857")
         * @return this builder
         */
        public Builder crs(String crsId) {
            this.crsId = crsId;
            return this;
        }

        /**
         * Sets the tile dimensions in pixels.
         *
         * @param width  the tile width in pixels
         * @param height the tile height in pixels
         * @return this builder
         */
        public Builder tileSize(int width, int height) {
            this.tileWidth = width;
            this.tileHeight = height;
            return this;
        }

        /**
         * Sets the map space extent covered by this tile matrix set.
         *
         * @param extent the map space extent
         * @return this builder
         */
        public Builder extent(Extent extent) {
            this.extent = extent;
            return this;
        }

        /**
         * Sets the resolutions for each zoom level. The array length must match the
         * number of zoom levels in the tile pyramid.
         *
         * @param resolutions the resolution array (map units per pixel)
         * @return this builder
         */
        public Builder resolutions(double... resolutions) {
            this.resolutions = resolutions.clone();
            return this;
        }

        /**
         * Sets the origin coordinates for each zoom level. The array length must match
         * the number of zoom levels in the tile pyramid.
         *
         * @param origins the origin coordinate array
         * @return this builder
         */
        public Builder origins(Coordinate... origins) {
            this.origins = origins.clone();
            return this;
        }

        /**
         * Sets the zoom level range by subsetting the tile pyramid and adjusting
         * resolutions/origins. This is a convenience method for creating tile matrix
         * sets with limited zoom ranges.
         *
         * @param minZoom the minimum zoom level (inclusive)
         * @param maxZoom the maximum zoom level (inclusive)
         * @return this builder
         * @throws IllegalStateException if tilePyramid, resolutions, or origins are not
         *                               set
         */
        public Builder zoomRange(int minZoom, int maxZoom) {
            if (tilePyramid == null) {
                throw new IllegalStateException("tilePyramid must be set before calling zoomRange()");
            }
            if (resolutions == null) {
                throw new IllegalStateException("resolutions must be set before calling zoomRange()");
            }
            if (origins == null) {
                throw new IllegalStateException("origins must be set before calling zoomRange()");
            }

            // Validate zoom range is within current arrays
            if (minZoom < 0 || maxZoom >= resolutions.length || maxZoom >= origins.length) {
                throw new IllegalArgumentException(
                        "Zoom range [" + minZoom + ", " + maxZoom + "] is outside available array bounds [0, "
                                + Math.min(resolutions.length - 1, origins.length - 1) + "]");
            }

            // Validate all zoom levels in range have valid values
            for (int z = minZoom; z <= maxZoom; z++) {
                if (resolutions[z] <= 0) {
                    throw new IllegalArgumentException("Invalid resolution at zoom level " + z + ": " + resolutions[z]);
                }
                if (origins[z] == null) {
                    throw new IllegalArgumentException("Missing origin at zoom level " + z);
                }
            }

            // Create new arrays that maintain direct indexing but sized to the max zoom needed
            double[] subsetResolutions = new double[maxZoom + 1];
            Coordinate[] subsetOrigins = new Coordinate[maxZoom + 1];

            // Copy relevant values maintaining direct indexing
            for (int z = minZoom; z <= maxZoom; z++) {
                subsetResolutions[z] = resolutions[z];
                subsetOrigins[z] = origins[z];
            }

            this.resolutions = subsetResolutions;
            this.origins = subsetOrigins;

            // Now subset the tile pyramid
            this.tilePyramid = tilePyramid.subset(minZoom, maxZoom);

            return this;
        }

        /**
         * Builds the StandardTileMatrixSet instance.
         *
         * @return the configured tile matrix set
         * @throws IllegalStateException if required properties are not set
         */
        public TileMatrixSet build() {
            if (tilePyramid == null) {
                throw new IllegalStateException("tilePyramid is required");
            }
            if (crsId == null) {
                throw new IllegalStateException("crsId is required");
            }
            if (extent == null) {
                throw new IllegalStateException("extent is required");
            }
            if (resolutions == null) {
                throw new IllegalStateException("resolutions are required");
            }
            if (origins == null) {
                throw new IllegalStateException("origins are required");
            }

            // Arrays must be large enough to directly index by zoom level
            int maxZoom = tilePyramid.maxZoomLevel();
            int minZoom = tilePyramid.minZoomLevel();

            if (resolutions.length <= maxZoom) {
                throw new IllegalStateException("resolutions array length (" + resolutions.length
                        + ") must be greater than max zoom level (" + maxZoom + ")");
            }
            if (origins.length <= maxZoom) {
                throw new IllegalStateException("origins array length (" + origins.length
                        + ") must be greater than max zoom level (" + maxZoom + ")");
            }

            // Validate that all required zoom levels have values
            for (int z = minZoom; z <= maxZoom; z++) {
                if (resolutions[z] <= 0) {
                    throw new IllegalStateException("Invalid resolution at zoom level " + z + ": " + resolutions[z]);
                }
                if (origins[z] == null) {
                    throw new IllegalStateException("Missing origin at zoom level " + z);
                }
            }

            return new StandardTileMatrixSet(tilePyramid, crsId, tileWidth, tileHeight, extent, resolutions, origins);
        }
    }

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
     * Returns the map space extent covered by this tile matrix set.
     * This defines the bounding box in CRS coordinates that encompasses
     * all tiles at all zoom levels.
     *
     * @return the map space extent
     */
    Extent extent();

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
     * Returns the map space origin coordinate for the specified zoom level.
     * This is the map space coordinate that corresponds to tile (0,0) at that zoom level.
     * The origin depends on the tile pyramid's axis origin configuration.
     *
     * @param zoomLevel the zoom level
     * @return the origin coordinate in map space
     * @throws IllegalArgumentException if the zoom level is not supported
     */
    Coordinate origin(int zoomLevel);

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
     * Converts a map space extent to the corresponding tile range at the specified zoom level.
     * The returned range covers all tiles that intersect with the given extent.
     *
     * @param extent the map space extent
     * @param zoomLevel the target zoom level
     * @return the tile range covering the extent
     * @throws IllegalArgumentException if the zoom level is not supported
     */
    TileRange extentToRange(Extent extent, int zoomLevel);

    // New TileMatrix-based API

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
    Optional<TileMatrixSet> intersection(Extent mapExtent);

    /**
     * Returns a TileMatrix for the specified zoom level containing only tiles that intersect
     * with the given extent. This is a convenience method equivalent to calling
     * {@code getTileMatrix(zoomLevel).intersection(mapExtent)}.
     *
     * @param mapExtent the map space extent to intersect with
     * @param zoomLevel the zoom level for the tile matrix
     * @return a TileMatrix containing only intersecting tiles at the specified zoom level, or empty if no tiles intersect
     */
    default Optional<TileMatrix> intersection(Extent mapExtent, int zoomLevel) {
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
    default Optional<TileMatrixSet> intersection(Extent mapExtent, int minZoomLevel, int maxZoomLevel) {
        return subset(minZoomLevel, maxZoomLevel).intersection(mapExtent);
    }

    /**
     * Creates a new builder for StandardTileMatrixSet.
     *
     * @return a new builder instance
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder pre-populated with this tile matrix set's configuration.
     * Useful for creating variants with different zoom level ranges.
     *
     * @return a builder initialized with this tile matrix set's values
     */
    default TileMatrixSet.Builder toBuilder() {
        return StandardTileMatrixSet.toBuilder(this);
    }
}
