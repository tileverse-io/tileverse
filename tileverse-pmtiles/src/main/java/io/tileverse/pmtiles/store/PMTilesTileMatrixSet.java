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
import io.tileverse.vectortile.store.WebMercatorTransform;

/**
 * Factory for creating {@link TileMatrixSet} instances tailored to a PMTiles archive's metadata.
 *
 * <p>This utility class provides methods to derive a tiling scheme that matches the spatial extent and zoom level range
 * defined in a PMTiles header.
 *
 * @since 1.0
 */
public final class PMTilesTileMatrixSet {

    private PMTilesTileMatrixSet() {
        // utility class
    }

    /**
     * Creates a Web Mercator (EPSG:3857) tile matrix set from a {@link PMTilesReader}.
     *
     * @param pmtilesReader the reader for the PMTiles archive
     * @return a tile matrix set matching the archive's configuration
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromWebMercator(PMTilesReader pmtilesReader) {
        return fromWebMercator(pmtilesReader.getHeader());
    }

    /**
     * Creates a Web Mercator (EPSG:3857) tile matrix set from a {@link PMTilesHeader}.
     *
     * <p>The resulting set is a subset of the standard OGC WebMercatorQuad, restricted to the geographic extent and
     * zoom range specified in the header.
     *
     * @param pmtilesHeader the PMTiles archive header
     * @return a tile matrix set covering the archive's spatial and zoom extent
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
     * Creates a Web Mercator tile matrix set with 512px tiles from a {@link PMTilesHeader}.
     *
     * <p>Uses the standard OGC WebMercatorQuadx2 as the base.
     *
     * @param pmtilesHeader the PMTiles archive header
     * @return a 512px tile matrix set matching the archive's extent
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
     * Creates a geographic (EPSG:4326) tile matrix set from a {@link PMTilesHeader}.
     *
     * <p>Uses the standard WorldCRS84Quad as the base.
     *
     * @param pmtilesHeader the PMTiles archive header
     * @return a geographic tile matrix set matching the archive's extent
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
     * Creates a geographic (EPSG:4326) tile matrix set from a {@link PMTilesReader}.
     *
     * @param pmtilesReader the reader for the PMTiles archive
     * @return a geographic tile matrix set matching the archive's extent
     * @throws IllegalArgumentException if the zoom range is invalid
     */
    public static TileMatrixSet fromCRS84(PMTilesReader pmtilesReader) {
        return fromCRS84(pmtilesReader.getHeader());
    }
}
