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
import io.tileverse.tiling.pyramid.TilePyramid;

/**
 * Builder for creating StandardTileMatrixSet instances.
 */
public class TileMatrixSetBuilder {
    private TilePyramid tilePyramid;
    private String crsId;
    private int tileWidth = 256;
    private int tileHeight = 256;
    private BoundingBox2D extent;
    private double[] resolutions;

    /**
     * Sets the tile pyramid that defines the discrete grid structure.
     *
     * @param tilePyramid the tile pyramid
     * @return this builder
     */
    public TileMatrixSetBuilder tilePyramid(TilePyramid tilePyramid) {
        this.tilePyramid = tilePyramid;
        return this;
    }

    /**
     * Sets the coordinate reference system identifier.
     *
     * @param crsId the CRS identifier (e.g., "EPSG:4326", "EPSG:3857")
     * @return this builder
     */
    public TileMatrixSetBuilder crs(String crsId) {
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
    public TileMatrixSetBuilder tileSize(int width, int height) {
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
    public TileMatrixSetBuilder extent(BoundingBox2D extent) {
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
    public TileMatrixSetBuilder resolutions(double... resolutions) {
        this.resolutions = resolutions.clone();
        return this;
    }

    /**
     * Sets the zoom level range by subsetting the tile pyramid and adjusting
     * resolutions. This is a convenience method for creating tile matrix
     * sets with limited zoom ranges.
     *
     * @param minZoom the minimum zoom level (inclusive)
     * @param maxZoom the maximum zoom level (inclusive)
     * @return this builder
     * @throws IllegalStateException if tilePyramid, resolutions, or origins are not
     *                               set
     */
    public TileMatrixSetBuilder zoomRange(int minZoom, int maxZoom) {
        if (tilePyramid == null) {
            throw new IllegalStateException("tilePyramid must be set before calling zoomRange()");
        }
        if (resolutions == null) {
            throw new IllegalStateException("resolutions must be set before calling zoomRange()");
        }

        // Validate zoom range is within current arrays
        if (minZoom < 0 || maxZoom >= resolutions.length) {
            throw new IllegalArgumentException("Zoom range [" + minZoom + ", " + maxZoom
                    + "] is outside available array bounds [0, " + (resolutions.length - 1) + "]");
        }

        // Validate all zoom levels in range have valid values
        for (int z = minZoom; z <= maxZoom; z++) {
            if (resolutions[z] <= 0) {
                throw new IllegalArgumentException("Invalid resolution at zoom level " + z + ": " + resolutions[z]);
            }
        }

        // Create new arrays that maintain direct indexing but sized to the max zoom needed
        double[] subsetResolutions = new double[maxZoom + 1];

        // Copy relevant values maintaining direct indexing
        for (int z = minZoom; z <= maxZoom; z++) {
            subsetResolutions[z] = resolutions[z];
        }

        this.resolutions = subsetResolutions;

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
        // Arrays must be large enough to directly index by zoom level
        int maxZoom = tilePyramid.maxZoomLevel();
        int minZoom = tilePyramid.minZoomLevel();

        if (resolutions.length <= maxZoom) {
            throw new IllegalStateException("resolutions array length (" + resolutions.length
                    + ") must be greater than max zoom level (" + maxZoom + ")");
        }

        // Validate that all required zoom levels have values
        for (int z = minZoom; z <= maxZoom; z++) {
            if (resolutions[z] <= 0) {
                throw new IllegalStateException("Invalid resolution at zoom level " + z + ": " + resolutions[z]);
            }
        }

        return new StandardTileMatrixSet(tilePyramid, crsId, tileWidth, tileHeight, extent, resolutions);
    }
}
