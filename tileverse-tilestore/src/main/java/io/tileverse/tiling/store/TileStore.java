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

/**
 * Interface for tile stores that provide access to tile data.
 *
 * <p>A {@code TileStore} provides a mechanism to retrieve tile data based on spatial queries, zoom levels, or specific
 * tile coordinates. It operates within a defined {@link TileMatrixSet}.
 *
 * @param <T> the type of data associated with tiles (e.g., {@code VectorTile} for vector stores, or a byte array/image
 *     for raster stores)
 */
public interface TileStore<T> {

    /** Strategy for selecting zoom levels based on a requested resolution. */
    enum Strategy {
        /**
         * On ties or when no exact resolution match exists, prefers a coarser zoom level (higher units-per-pixel, fewer
         * tiles). Optimizes for fewer tile reads and faster processing at the cost of detail.
         */
        SPEED,
        /**
         * On ties or when no exact resolution match exists, prefers a finer zoom level (lower units-per-pixel, more
         * tiles). Optimizes for detail at the cost of additional tile reads.
         */
        QUALITY
    }

    /**
     * Returns the tile matrix set defining the tiling scheme of this store.
     *
     * @return the {@link TileMatrixSet}
     */
    TileMatrixSet matrixSet();

    /**
     * Finds tiles matching the given extents and target resolution.
     *
     * <p>The zoom level is automatically determined based on the provided resolution and selection strategy.
     *
     * @param extents list of bounding boxes in the store's CRS to query
     * @param resolution the desired resolution (units per pixel)
     * @param strategy the strategy for selecting the appropriate zoom level
     * @return a stream of tile data matching the criteria
     */
    default Stream<TileData<T>> findTiles(List<BoundingBox2D> extents, double resolution, Strategy strategy) {
        int bestZoomLevel = findBestZoomLevel(resolution, strategy);
        return findTiles(extents, bestZoomLevel);
    }

    /**
     * Finds tiles matching the given extents at a specific zoom level.
     *
     * @param extents list of bounding boxes in the store's CRS to query
     * @param zoomLevel the zoom level to query
     * @return a stream of tile data at the specified zoom level
     */
    default Stream<TileData<T>> findTiles(List<BoundingBox2D> extents, int zoomLevel) {
        Optional<TileMatrix> filteredTileMatrix = tileMatrix(extents, zoomLevel);
        Stream<Tile> tiles = filteredTileMatrix.map(TileMatrix::tiles).orElseGet(Stream::empty);
        return tiles.map(this::loadTile).filter(Optional::isPresent).map(Optional::orElseThrow);
    }

    /**
     * Computes the subset of the tile matrix that covers the given extents at a specific zoom level.
     *
     * <p>When {@code extents} is empty, the full tile matrix at {@code zoomLevel} is returned.
     *
     * @param extents list of bounding boxes to query; empty means no spatial filter
     * @param zoomLevel the zoom level
     * @return optional tile matrix covering the intersection of the extents and the full matrix; the full matrix when
     *     {@code extents} is empty; empty when {@code zoomLevel} is outside the matrix set or no extent intersects the
     *     matrix
     */
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
                .map(fullMatrix::intersection)
                .filter(Optional::isPresent)
                .map(Optional::orElseThrow)
                .toList();

        if (intersecting.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(TileMatrix.union(intersecting));
    }

    /**
     * Retrieves a single tile by its coordinate index.
     *
     * @param tileIndex the unique identifier of the tile
     * @return an optional containing the tile data if it exists, empty otherwise
     */
    default Optional<TileData<T>> findTile(TileIndex tileIndex) {
        return matrixSet().tile(requireNonNull(tileIndex)).flatMap(this::loadTile);
    }

    /**
     * Loads the actual data for the specified tile metadata.
     *
     * <p>Implementations should handle the low-level retrieval and decoding of the tile content.
     *
     * @param tile the tile metadata for which to load data
     * @return an optional containing the tile data, or empty if it could not be loaded
     */
    Optional<TileData<T>> loadTile(Tile tile);

    /**
     * Finds the best zoom level for the provided resolution across all available levels.
     *
     * @param resolution the target resolution
     * @param strategy the selection strategy ({@link Strategy#SPEED} or {@link Strategy#QUALITY})
     * @return the best matching zoom level
     */
    default int findBestZoomLevel(final double resolution, Strategy strategy) {
        final int minZoomLevel = matrixSet().minZoomLevel();
        final int maxZoomLevel = matrixSet().maxZoomLevel();

        return findBestZoomLevel(resolution, strategy, minZoomLevel, maxZoomLevel);
    }

    /**
     * Finds the best zoom level for the provided resolution within a specific range.
     *
     * @param resolution the target resolution
     * @param strategy the selection strategy ({@link Strategy#SPEED} or {@link Strategy#QUALITY})
     * @param minZoomLevel the minimum zoom level to consider
     * @param maxZoomLevel the maximum zoom level to consider
     * @return the best matching zoom level within the specified range
     * @throws IllegalArgumentException if minZoomLevel is greater than maxZoomLevel
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
                    yield closestZoom;
            };
        }
        return bestZoomLevel;
    }
}
