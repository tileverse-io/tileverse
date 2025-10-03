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
package io.tileverse.tiling.store;

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrix;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.pyramid.TileIndex;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface TileStore<T> {

    enum Strategy {
        /**
         * Selects the closest lower quality zoom level based on the provided resolution
         */
        SPEED,
        /**
         * Selects the closest higher quality zoom level based on the provided resolution
         */
        QUALITY
    }

    TileMatrixSet matrixSet();

    default Stream<TileData<T>> findTiles(List<BoundingBox2D> extents, double resolution, Strategy strategy) {
        int bestZoomLevel = findBestZoomLevel(resolution, strategy);
        return findTiles(extents, bestZoomLevel);
    }

    default Stream<TileData<T>> findTiles(List<BoundingBox2D> extents, int zoomLevel) {
        Optional<TileMatrix> filteredTileMatrix = tileMatrix(extents, zoomLevel);
        Stream<Tile> tiles = filteredTileMatrix.map(TileMatrix::tiles).orElseGet(Stream::empty);
        return tiles.map(this::loadTile).filter(Optional::isPresent).map(Optional::orElseThrow);
    }

    default Optional<TileMatrix> tileMatrix(List<BoundingBox2D> extents, int zoomLevel) {
        requireNonNull(extents, "extents is null");

        Optional<TileMatrix> tileMatrix = matrixSet().tileMatrix(zoomLevel);
        if (tileMatrix.isEmpty() || extents.isEmpty()) {
            return tileMatrix;
        }

        final TileMatrix fullMatrix = tileMatrix.orElseThrow();

        if (extents.size() == 1) {
            return fullMatrix.intersection(extents.get(0));
        }

        List<TileMatrix> intersecting = extents.stream()
                .map(extent -> fullMatrix.intersection(extent))
                .filter(Optional::isPresent)
                .map(Optional::orElseThrow)
                .toList();

        if (intersecting.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(TileMatrix.union(intersecting));
    }

    default Optional<TileData<T>> findTile(TileIndex tileIndex) {
        return matrixSet().tile(requireNonNull(tileIndex)).flatMap(this::loadTile);
    }

    Optional<TileData<T>> loadTile(Tile tile);

    /**
     * Finds the best zoom level for the provided resolution, based on the strategy chosen
     * and each {@link TileMatrixSet#resolution(int) TileMatrix resolution}.
     *
     * @param resolution
     * @param strategy
     * @return
     */
    /**
     * {@inheritDoc}
     */
    default int findBestZoomLevel(final double resolution, Strategy strategy) {
        final int minZoomLevel = matrixSet().minZoomLevel();
        final int maxZoomLevel = matrixSet().maxZoomLevel();

        return findBestZoomLevel(resolution, strategy, minZoomLevel, maxZoomLevel);
    }

    /**
     * Finds the best zoom level for the provided resolution between {@code minZoomLevel} and {@code maxZoomLevel}
     * @param resolution
     * @param strategy
     * @param minZoomLevel
     * @param maxZoomLevel
     * @return
     */
    default int findBestZoomLevel(
            final double resolution, Strategy strategy, final int minZoomLevel, final int maxZoomLevel) {
        if (minZoomLevel > maxZoomLevel) {
            throw new IllegalArgumentException("minZoomLevel>maxZoomLevel");
        }
        // Find the zoom level with the closest resolution
        int closestZoom = minZoomLevel;
        double closestDiff = Double.MAX_VALUE;

        for (int zoom = minZoomLevel; zoom <= maxZoomLevel; zoom++) {
            double zoomResolution = matrixSet().resolution(zoom);
            double diff = Math.abs(resolution - zoomResolution);
            if (diff < closestDiff) {
                closestDiff = diff;
                closestZoom = zoom;
            }
        }

        // Apply strategy to choose between closest and adjacent zoom levels
        final double closestResolution = matrixSet().resolution(closestZoom);

        int bestZoomLevel;
        if (closestResolution == resolution) {
            // Exact match, return it regardless of strategy
            bestZoomLevel = closestZoom;
        } else {

            bestZoomLevel = switch (strategy) {
                case SPEED:
                    // Prefer lower quality (higher resolution values, lower zoom levels)
                    if (closestResolution < resolution && closestZoom > minZoomLevel) {
                        // Current zoom is higher quality than needed, try lower zoom
                        yield closestZoom - 1;
                    }
                    yield closestZoom;

                case QUALITY:
                    // Prefer higher quality (lower resolution values, higher zoom levels)
                    if (closestResolution > resolution && closestZoom < maxZoomLevel) {
                        // Current zoom is lower quality than needed, try higher zoom
                        yield closestZoom + 1;
                    }
                    yield closestZoom;

                default:
                    yield closestZoom;};
        }
        return bestZoomLevel;
    }
}
