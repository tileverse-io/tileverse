/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles.store;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.vectortile.store.WebMercatorTransform;
import io.tileverse.versatiles.VersatilesHeader;
import io.tileverse.versatiles.VersatilesReader;

/**
 * Builds the {@link TileMatrixSet} of a Versatiles archive.
 *
 * <p>Versatiles tiles follow the Web Mercator (EPSG:3857) tiling scheme of web maps. The tile matrix set is the
 * {@code WebMercatorQuad} set, or {@code WebMercatorQuadx2} for raster tiles of 512 pixels, limited to the zoom range
 * of the archive and to its bounding box. The header records the bounding box in WGS84 longitude and latitude, and the
 * intersection uses it converted to Web Mercator.
 *
 * <p>Both sets end at zoom level 24. The tiles of deeper levels stay readable through {@link VersatilesReader}, outside
 * the tile matrix set.
 */
public final class VersatilesTileMatrixSet {

    private VersatilesTileMatrixSet() {}

    /** Returns the tile matrix set of the archive read by {@code reader}. */
    public static TileMatrixSet fromWebMercator(VersatilesReader reader) {
        return fromWebMercator(reader.getHeader());
    }

    /**
     * Returns the tile matrix set described by {@code header}.
     *
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of {@code WebMercatorQuad}
     */
    public static TileMatrixSet fromWebMercator(VersatilesHeader header) {
        return limitToArchive(DefaultTileMatrixSets.WEB_MERCATOR_QUAD, header);
    }

    /**
     * Returns the tile matrix set described by {@code header}, for tiles of 512 x 512 pixels.
     *
     * @throws IllegalArgumentException if the archive has no zoom level within the levels of {@code WebMercatorQuadx2}
     */
    public static TileMatrixSet fromWebMercator512(VersatilesHeader header) {
        return limitToArchive(DefaultTileMatrixSets.WEB_MERCATOR_QUADx2, header);
    }

    private static TileMatrixSet limitToArchive(TileMatrixSet quad, VersatilesHeader header) {
        int maxZoom = Math.min(header.maxZoom(), quad.maxZoomLevel());
        if (header.minZoom() > maxZoom) {
            throw new IllegalArgumentException("The zoom levels %d to %d of the archive are deeper than level %d of %s"
                    .formatted(header.minZoom(), header.maxZoom(), quad.maxZoomLevel(), quad.identifier()));
        }
        BoundingBox2D webMercatorExtent = WebMercatorTransform.latLonToWebMercator(header.geographicBoundingBox());
        TileMatrixSet base =
                quad.toBuilder().zoomRange(header.minZoom(), maxZoom).build();
        return base.intersection(webMercatorExtent).orElse(base);
    }
}
