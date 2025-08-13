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

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.pyramid.TilePyramid;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A view-based implementation of TileMatrixSet that delegates to another TileMatrixSet
 * while applying zoom level and/or spatial filtering. This allows for efficient chaining
 * of subset and intersection operations without creating new underlying data structures.
 *
 * <p>This implementation supports:
 * <ul>
 * <li>Zoom level filtering (subset operations)</li>
 * <li>Spatial filtering (intersection operations)</li>
 * <li>Chaining of multiple filters</li>
 * <li>Lazy evaluation of filtered matrices</li>
 * </ul>
 *
 * @since 1.0
 */
class TileMatrixSetView implements TileMatrixSet {

    private final TileMatrixSet delegate;
    private final int minZoomLevel;
    private final int maxZoomLevel;
    private final Extent filterExtent;

    // Lazy cached values
    private volatile List<TileMatrix> cachedMatrices;
    private volatile TilePyramid cachedPyramid;

    /**
     * Creates a view with zoom level filtering.
     */
    private TileMatrixSetView(TileMatrixSet delegate, int minZoomLevel, int maxZoomLevel, Extent filterExtent) {
        this.delegate = requireNonNull(delegate);
        this.minZoomLevel = minZoomLevel;
        this.maxZoomLevel = maxZoomLevel;
        this.filterExtent = requireNonNull(filterExtent);
    }

    /**
     * Creates a zoom-filtered view of the given tile matrix set.
     */
    static TileMatrixSetView subset(TileMatrixSet delegate, int minZoomLevel, int maxZoomLevel) {
        // Validate zoom range
        if (minZoomLevel > maxZoomLevel) {
            throw new IllegalArgumentException(
                    "minZoomLevel (" + minZoomLevel + ") cannot be greater than maxZoomLevel (" + maxZoomLevel + ")");
        }

        // If delegate is already a view, we can optimize by combining filters
        if (delegate instanceof TileMatrixSetView view) {
            int combinedMinZoom = Math.max(minZoomLevel, view.minZoomLevel);
            int combinedMaxZoom = Math.min(maxZoomLevel, view.maxZoomLevel);

            if (combinedMinZoom > combinedMaxZoom) {
                throw new IllegalArgumentException("Zoom range [" + minZoomLevel + ", " + maxZoomLevel
                        + "] does not intersect with existing filter");
            }

            return new TileMatrixSetView(view.delegate, combinedMinZoom, combinedMaxZoom, view.filterExtent);
        }

        return new TileMatrixSetView(delegate, minZoomLevel, maxZoomLevel, delegate.extent());
    }

    /**
     * Creates a spatially-filtered view of the given tile matrix set.
     */
    static Optional<TileMatrixSet> intersection(TileMatrixSet delegate, Extent mapExtent) {
        TileMatrixSet ret = null;
        int minZoom, maxZoom;

        // If delegate is already a view, we can combine filters but preserve zoom levels
        if (delegate instanceof TileMatrixSetView view) {
            mapExtent = view.filterExtent.intersection(mapExtent);
            minZoom = view.minZoomLevel;
            maxZoom = view.maxZoomLevel;
            delegate = view.delegate;
        } else {
            minZoom = delegate.minZoomLevel();
            maxZoom = delegate.maxZoomLevel();
        }

        if (delegate.extent().intersects(mapExtent)) {
            ret = new TileMatrixSetView(delegate, minZoom, maxZoom, mapExtent);
        }

        return Optional.ofNullable(ret);
    }

    @Override
    public TilePyramid tilePyramid() {
        if (cachedPyramid == null) {
            synchronized (this) {
                if (cachedPyramid == null) {
                    TilePyramid basePyramid = delegate.tilePyramid();

                    // Apply zoom filtering if different from delegate's range
                    int delegateMin = delegate.minZoomLevel();
                    int delegateMax = delegate.maxZoomLevel();

                    if (minZoomLevel != delegateMin || maxZoomLevel != delegateMax) {
                        int actualMin = Math.max(minZoomLevel, basePyramid.minZoomLevel());
                        int actualMax = Math.min(maxZoomLevel, basePyramid.maxZoomLevel());

                        if (actualMin <= actualMax) {
                            basePyramid = basePyramid.subset(actualMin, actualMax);
                        } else {
                            // Empty pyramid
                            basePyramid = TilePyramid.builder()
                                    .axisOrigin(basePyramid.axisOrigin())
                                    .build();
                        }
                    }

                    // Apply spatial filtering if different from delegate's extent
                    if (!filterExtent.equals(delegate.extent())) {
                        TilePyramid.Builder pyramidBuilder =
                                TilePyramid.builder().axisOrigin(basePyramid.axisOrigin());

                        for (var range : basePyramid.levels()) {
                            // Get the tile matrix for this zoom level and apply spatial filter
                            Optional<TileMatrix> matrix = delegate.tileMatrix(range.zoomLevel());
                            if (matrix.isPresent()) {
                                TileMatrix filtered =
                                        matrix.get().intersection(filterExtent).orElse(null);
                                if (filtered != null) {
                                    pyramidBuilder.level(filtered.tileRange());
                                }
                            }
                        }

                        basePyramid = pyramidBuilder.build();
                    }

                    cachedPyramid = basePyramid;
                }
            }
        }
        return cachedPyramid;
    }

    @Override
    public String crsId() {
        return delegate.crsId();
    }

    @Override
    public int tileWidth() {
        return delegate.tileWidth();
    }

    @Override
    public int tileHeight() {
        return delegate.tileHeight();
    }

    @Override
    public Extent extent() {
        return delegate.extent();
    }

    @Override
    public double resolution(int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return delegate.resolution(zoomLevel);
    }

    @Override
    public Coordinate origin(int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return delegate.origin(zoomLevel);
    }

    @Override
    public List<TileMatrix> tileMatrices() {
        if (cachedMatrices == null) {
            synchronized (this) {
                if (cachedMatrices == null) {
                    cachedMatrices = tilePyramid().levels().stream()
                            .map(range -> {
                                Optional<TileMatrix> matrix = delegate.tileMatrix(range.zoomLevel());
                                if (matrix.isPresent()) {
                                    TileMatrix result = matrix.get();
                                    // Apply spatial filter if different from delegate's extent
                                    if (!filterExtent.equals(delegate.extent())) {
                                        result = result.intersection(filterExtent)
                                                .orElse(null);
                                    }
                                    // The zoom filtering is already handled by tilePyramid()
                                    return result;
                                }
                                return null;
                            })
                            .filter(Objects::nonNull)
                            .toList();
                }
            }
        }
        return cachedMatrices;
    }

    @Override
    public Optional<TileMatrix> tileMatrix(int zoomLevel) {
        if (!isZoomLevelInRange(zoomLevel)) {
            return Optional.empty();
        }

        Optional<TileMatrix> matrix = delegate.tileMatrix(zoomLevel);
        if (matrix.isPresent() && !filterExtent.equals(delegate.extent())) {
            return matrix.get().intersection(filterExtent);
        }
        return matrix;
    }

    @Override
    public Optional<TileMatrixSet> intersection(Extent mapExtent) {
        return TileMatrixSetView.intersection(this, mapExtent);
    }

    @Override
    public TileMatrixSet subset(int minZoomLevel, int maxZoomLevel) {
        return TileMatrixSetView.subset(this, minZoomLevel, maxZoomLevel);
    }

    // Delegate abstract methods from TileMatrixSet

    @Override
    public io.tileverse.tiling.pyramid.TileIndex coordinateToTile(Coordinate coordinate, int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return delegate.coordinateToTile(coordinate, zoomLevel);
    }

    @Override
    public io.tileverse.tiling.pyramid.TileRange extentToRange(Extent extent, int zoomLevel) {
        validateZoomLevel(zoomLevel);
        return delegate.extentToRange(extent, zoomLevel);
    }

    private void validateZoomLevel(int zoomLevel) {
        if (!isZoomLevelInRange(zoomLevel)) {
            throw new IllegalArgumentException(
                    "Zoom level " + zoomLevel + " is not supported by this tile matrix set view");
        }
        if (!delegate.hasZoomLevel(zoomLevel)) {
            throw new IllegalArgumentException(
                    "Zoom level " + zoomLevel + " is not supported by the underlying tile matrix set");
        }
    }

    private boolean isZoomLevelInRange(int zoomLevel) {
        return zoomLevel >= minZoomLevel && zoomLevel <= maxZoomLevel;
    }
}
