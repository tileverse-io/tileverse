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
package io.tileverse.pmtiles.store;

import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrixSet;

/**
 * Factory methods for creating TileMatrixSet instances from PMTiles data.
 * Provides convenient methods to create tile matrix sets that match the
 * coverage and zoom levels of PMTiles files.
 *
 * @since 1.0
 */
public class PMTilesTileMatrixSet {

    /**
     * Creates a WebMercator TileMatrixSet from a PMTilesReader.
     * Convenience method that extracts the header and creates the tile matrix set.
     *
     * @param pmtilesReader the PMTiles reader
     * @return a TileMatrixSet covering the PMTiles zoom range
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromWebMercator(PMTilesReader pmtilesReader) {
        return fromWebMercator(pmtilesReader.getHeader());
    }

    /**
     * Creates a WebMercator TileMatrixSet that matches both the zoom level range
     * and geographic bounds defined in a PMTiles header. Uses the standard OGC
     * WebMercatorQuad tile matrix set as the base and creates a subset covering
     * only the tiles that intersect with the PMTiles bounding box.
     *
     * <p>This assumes the PMTiles uses WebMercator projection (EPSG:3857),
     * which is the most common case for PMTiles files.
     *
     * @param pmtilesHeader the PMTiles header containing zoom level and bounds information
     * @return a TileMatrixSet covering the PMTiles geographic and zoom extent
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromWebMercator(PMTilesHeader pmtilesHeader) {
        // Convert PMTiles lat/lon bounds to WebMercator extent using precise transformation
        BoundingBox2D webMercatorExtent =
                WebMercatorTransform.latLonToWebMercator(pmtilesHeader.geographicBoundingBox());

        // Get zoom-level subset and spatial intersection in one step
        TileMatrixSet baseTms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD.toBuilder()
                .zoomRange(pmtilesHeader.minZoom(), pmtilesHeader.maxZoom())
                .build();

        return baseTms.intersection(webMercatorExtent).orElse(baseTms);
    }

    /**
     * Creates a WebMercator TileMatrixSet with 512px tiles that matches both the
     * zoom level range and geographic bounds defined in a PMTiles header.
     * Uses the standard OGC WebMercatorQuad x2 tile matrix set as the base.
     *
     * @param pmtilesHeader the PMTiles header containing zoom level and bounds information
     * @return a TileMatrixSet with 512px tiles covering the PMTiles extent
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromWebMercator512(PMTilesHeader pmtilesHeader) {
        // Convert PMTiles lat/lon bounds to WebMercator extent using precise transformation
        BoundingBox2D webMercatorExtent =
                WebMercatorTransform.latLonToWebMercator(pmtilesHeader.geographicBoundingBox());

        // Get zoom-level subset and spatial intersection
        TileMatrixSet baseTms = DefaultTileMatrixSets.WEB_MERCATOR_QUADx2.toBuilder()
                .zoomRange(pmtilesHeader.minZoom(), pmtilesHeader.maxZoom())
                .build();

        return baseTms.intersection(webMercatorExtent).orElse(baseTms);
    }

    /**
     * Creates a geographic (EPSG:4326) TileMatrixSet that matches both the zoom level range
     * and geographic bounds defined in a PMTiles header. Uses the standard WorldCRS84Quad
     * tile matrix set as the base.
     *
     * @param pmtilesHeader the PMTiles header containing zoom level and bounds information
     * @return a TileMatrixSet covering the PMTiles extent in EPSG:4326
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromCRS84(PMTilesHeader pmtilesHeader) {
        // PMTiles bounds are already in lat/lon, create extent directly
        BoundingBox2D latLonExtent = pmtilesHeader.geographicBoundingBox();

        // Get zoom-level subset and spatial intersection
        TileMatrixSet baseTms = DefaultTileMatrixSets.WORLD_CRS84_QUAD.toBuilder()
                .zoomRange(pmtilesHeader.minZoom(), pmtilesHeader.maxZoom())
                .build();

        return baseTms.intersection(latLonExtent).orElse(baseTms);
    }

    /**
     * Creates a geographic (EPSG:4326) TileMatrixSet from a PMTilesReader.
     * Convenience method that extracts the header and creates the tile matrix set.
     *
     * @param pmtilesReader the PMTiles reader
     * @return a TileMatrixSet covering the PMTiles zoom range in EPSG:4326
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromCRS84(PMTilesReader pmtilesReader) {
        return fromCRS84(pmtilesReader.getHeader());
    }
}
