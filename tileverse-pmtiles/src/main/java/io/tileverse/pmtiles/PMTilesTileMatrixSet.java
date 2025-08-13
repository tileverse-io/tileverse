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
package io.tileverse.pmtiles;

import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.Extent;
import io.tileverse.tiling.matrix.TileMatrixSet;

/**
 * Factory methods for creating TileMatrixSet instances from PMTiles data.
 * Provides convenient methods to create tile matrix sets that match the
 * coverage and zoom levels of PMTiles files.
 *
 * @since 1.0
 */
public class PMTilesTileMatrixSet {

    // Constants for WebMercator coordinate transformation
    private static final double EARTH_RADIUS = 6378137.0; // WGS84 semi-major axis
    private static final double ORIGIN_SHIFT = Math.PI * EARTH_RADIUS;

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
        // Convert PMTiles lat/lon bounds to WebMercator extent
        Extent webMercatorExtent = latLonToWebMercator(
                pmtilesHeader.minLon(), pmtilesHeader.minLat(),
                pmtilesHeader.maxLon(), pmtilesHeader.maxLat());

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
        // Convert PMTiles lat/lon bounds to WebMercator extent
        Extent webMercatorExtent = latLonToWebMercator(
                pmtilesHeader.minLon(), pmtilesHeader.minLat(),
                pmtilesHeader.maxLon(), pmtilesHeader.maxLat());

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
        Extent latLonExtent = Extent.of(
                pmtilesHeader.minLon(), pmtilesHeader.minLat(),
                pmtilesHeader.maxLon(), pmtilesHeader.maxLat());

        // Get zoom-level subset and spatial intersection
        TileMatrixSet baseTms = DefaultTileMatrixSets.WORLD_CRS84_QUAD.toBuilder()
                .zoomRange(pmtilesHeader.minZoom(), pmtilesHeader.maxZoom())
                .build();

        return baseTms.intersection(latLonExtent).orElse(baseTms);
    }

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

    /**
     * Converts lat/lon coordinates to WebMercator (EPSG:3857) extent.
     * Uses the spherical Mercator projection formula.
     *
     * @param minLon minimum longitude in degrees
     * @param minLat minimum latitude in degrees
     * @param maxLon maximum longitude in degrees
     * @param maxLat maximum latitude in degrees
     * @return WebMercator extent in meters
     */
    private static Extent latLonToWebMercator(double minLon, double minLat, double maxLon, double maxLat) {
        // Clamp latitude to valid Mercator range
        minLat = Math.max(-85.0511, Math.min(85.0511, minLat));
        maxLat = Math.max(-85.0511, Math.min(85.0511, maxLat));

        // Convert longitude to WebMercator X
        double minX = minLon * ORIGIN_SHIFT / 180.0;
        double maxX = maxLon * ORIGIN_SHIFT / 180.0;

        // Convert latitude to WebMercator Y using spherical Mercator formula
        double minY = Math.log(Math.tan((90.0 + minLat) * Math.PI / 360.0)) * EARTH_RADIUS;
        double maxY = Math.log(Math.tan((90.0 + maxLat) * Math.PI / 360.0)) * EARTH_RADIUS;

        return Extent.of(minX, minY, maxX, maxY);
    }
}
