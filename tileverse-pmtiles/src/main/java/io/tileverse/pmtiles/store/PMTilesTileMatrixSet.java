/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles.store;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.pmtiles.PMTilesHeader;
import io.tileverse.pmtiles.PMTilesReader;
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
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of WebMercatorQuad
     */
    public static TileMatrixSet fromWebMercator(PMTilesReader pmtilesReader) {
        return fromWebMercator(pmtilesReader.getHeader());
    }

    /**
     * Creates a Web Mercator (EPSG:3857) tile matrix set from a {@link PMTilesHeader}.
     *
     * <p>The resulting set is a subset of the standard OGC WebMercatorQuad, restricted to the geographic extent and
     * zoom range specified in the header. WebMercatorQuad ends at zoom level 24. The tiles of deeper levels stay
     * readable through {@link PMTilesReader}, outside the tile matrix set.
     *
     * @param pmtilesHeader the PMTiles archive header
     * @return a tile matrix set covering the archive's spatial and zoom extent
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of WebMercatorQuad
     */
    public static TileMatrixSet fromWebMercator(PMTilesHeader pmtilesHeader) {
        // Convert PMTiles lat/lon bounds to WebMercator extent using precise transformation
        BoundingBox2D webMercatorExtent =
                WebMercatorTransform.latLonToWebMercator(pmtilesHeader.geographicBoundingBox());

        return limitToArchive(DefaultTileMatrixSets.WEB_MERCATOR_QUAD, pmtilesHeader, webMercatorExtent);
    }

    /**
     * Creates a Web Mercator tile matrix set with 512px tiles from a {@link PMTilesHeader}.
     *
     * <p>Uses the standard OGC WebMercatorQuadx2 as the base.
     *
     * @param pmtilesHeader the PMTiles archive header
     * @return a 512px tile matrix set matching the archive's extent
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of WebMercatorQuadx2
     */
    public static TileMatrixSet fromWebMercator512(PMTilesHeader pmtilesHeader) {
        // Convert PMTiles lat/lon bounds to WebMercator extent using precise transformation
        BoundingBox2D webMercatorExtent =
                WebMercatorTransform.latLonToWebMercator(pmtilesHeader.geographicBoundingBox());

        return limitToArchive(DefaultTileMatrixSets.WEB_MERCATOR_QUADx2, pmtilesHeader, webMercatorExtent);
    }

    /**
     * Creates a geographic (CRS84) tile matrix set from a {@link PMTilesHeader}.
     *
     * <p>Uses the standard WorldCRS84Quad as the base.
     *
     * @param pmtilesHeader the PMTiles archive header
     * @return a geographic tile matrix set matching the archive's extent
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of WorldCRS84Quad
     */
    public static TileMatrixSet fromCRS84(PMTilesHeader pmtilesHeader) {
        // PMTiles bounds are already in lat/lon, create extent directly
        BoundingBox2D latLonExtent = pmtilesHeader.geographicBoundingBox();

        return limitToArchive(DefaultTileMatrixSets.WORLD_CRS84_QUAD, pmtilesHeader, latLonExtent);
    }

    /**
     * Creates a geographic (CRS84) tile matrix set from a {@link PMTilesReader}.
     *
     * @param pmtilesReader the reader for the PMTiles archive
     * @return a geographic tile matrix set matching the archive's extent
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of WorldCRS84Quad
     */
    public static TileMatrixSet fromCRS84(PMTilesReader pmtilesReader) {
        return fromCRS84(pmtilesReader.getHeader());
    }

    /**
     * Returns {@code quad} cut to the zoom levels of the archive, up to the last level of {@code quad}, and to
     * {@code extent}.
     *
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of {@code quad}
     */
    private static TileMatrixSet limitToArchive(TileMatrixSet quad, PMTilesHeader header, BoundingBox2D extent) {
        int maxZoom = Math.min(header.maxZoom(), quad.maxZoomLevel());
        if (header.minZoom() > maxZoom) {
            throw new IllegalArgumentException("The zoom levels %d to %d of the archive are deeper than level %d of %s"
                    .formatted(header.minZoom(), header.maxZoom(), quad.maxZoomLevel(), quad.identifier()));
        }
        TileMatrixSet base =
                quad.toBuilder().zoomRange(header.minZoom(), maxZoom).build();
        return base.intersection(extent).orElse(base);
    }
}
