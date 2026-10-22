/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
 * {@code WebMercatorQuad} set, limited to the zoom range of the archive and to its bounding box. The header records the
 * bounding box in WGS84 longitude and latitude, and the intersection uses it converted to Web Mercator.
 *
 * <p>{@code WebMercatorQuad} ends at zoom level 24. The tiles of deeper levels stay readable through
 * {@link VersatilesReader}, outside the tile matrix set.
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
        TileMatrixSet quad = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;
        int maxZoom = Math.min(header.maxZoom(), quad.maxZoomLevel());
        if (header.minZoom() > maxZoom) {
            throw new IllegalArgumentException(
                    "The zoom levels %d to %d of the archive are deeper than level %d of WebMercatorQuad"
                            .formatted(header.minZoom(), header.maxZoom(), quad.maxZoomLevel()));
        }
        BoundingBox2D webMercatorExtent = WebMercatorTransform.latLonToWebMercator(header.geographicBoundingBox());
        TileMatrixSet base =
                quad.toBuilder().zoomRange(header.minZoom(), maxZoom).build();
        return base.intersection(webMercatorExtent).orElse(base);
    }
}
