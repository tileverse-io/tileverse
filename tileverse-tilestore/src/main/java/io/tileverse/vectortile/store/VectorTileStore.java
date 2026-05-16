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
package io.tileverse.vectortile.store;

import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.AbstractTileStore;
import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Abstract base class for {@link io.tileverse.tiling.store.TileStore} implementations that provide {@link VectorTile}
 * data.
 *
 * <p>This class extends {@link AbstractTileStore} to provide vector-specific functionality, such as retrieving layer
 * metadata and querying individual features across multiple tiles.
 */
public abstract class VectorTileStore extends AbstractTileStore<VectorTile> {

    /**
     * Initializes the store with the specified tile matrix set.
     *
     * @param matrixSet the tile matrix set to use
     */
    protected VectorTileStore(TileMatrixSet matrixSet) {
        super(matrixSet);
    }

    /**
     * Returns the metadata for all vector layers available in this store.
     *
     * @return a list of {@link VectorLayer} metadata objects
     */
    public abstract List<VectorLayer> getVectorLayersMetadata();

    /**
     * Returns the metadata for a specific layer by its identifier.
     *
     * @param layerId the identifier of the layer
     * @return an optional containing the layer metadata, or empty if not found
     */
    public Optional<VectorLayer> getLayerMetadata(String layerId) {
        return getVectorLayersMetadata().stream()
                .filter(l -> layerId.equals(l.id()))
                .findFirst();
    }

    /**
     * Returns the overall spatial extent of the data in this store.
     *
     * @return the extent in the {@link TileMatrixSet#crsId() TileMatrixSet CRS}
     */
    public abstract BoundingBox2D getExtent();

    /**
     * Retrieves a stream of features matching the specified query.
     *
     * <p>This method handles finding the appropriate tiles, loading them, and extracting the features according to the
     * query parameters (e.g., layer filters, spatial filters).
     *
     * @param query the query parameters
     * @return a stream of {@link Feature} objects
     * @throws java.io.UncheckedIOException if a tile load fails during stream consumption
     */
    public Stream<Feature> getFeatures(VectorTilesQuery query) {
        // find matching tiles
        Stream<TileData<VectorTile>> tilesInExtentAndZoomLevel = findTiles(query);

        Stream<VectorTileReader> tileReaders = tilesInExtentAndZoomLevel.map(tile -> new VectorTileReader(tile, query));

        return tileReaders.flatMap(VectorTileReader::getFeatures);
    }

    /**
     * Finds the tiles that intersect the query extent at the appropriate zoom level.
     *
     * @param query the query parameters
     * @return a stream of tile data
     */
    protected Stream<TileData<VectorTile>> findTiles(VectorTilesQuery query) {
        final int zoomLevel = determineZoomLevel(query);
        final List<BoundingBox2D> queryExtent = query.extent();
        return findTiles(queryExtent, zoomLevel);
    }

    /**
     * Determines the most appropriate zoom level for the given query.
     *
     * <p>The zoom level is selected based on (in order of precedence):
     *
     * <ol>
     *   <li>Explicit zoom level in the query.
     *   <li>Best zoom level for the explicit resolution in the query, applying the query's strategy.
     *   <li>If the query's strategy is {@link Strategy#SPEED SPEED}, the store's minimum zoom level.
     *   <li>Otherwise, the maximum {@code minZoom} across all vector layers, falling back to the store's minimum zoom
     *       level when no layers report a {@code minZoom}.
     * </ol>
     *
     * @param query the query parameters
     * @return the determined zoom level
     */
    protected int determineZoomLevel(VectorTilesQuery query) {
        if (query.zoomLevel().isPresent()) {
            return query.zoomLevel().getAsInt();
        }
        if (query.resolution().isPresent()) {
            return findBestZoomLevel(query.resolution().getAsDouble(), query.strategy());
        }
        if (query.strategy() == Strategy.SPEED) {
            return matrixSet().minZoomLevel();
        }

        return getVectorLayersMetadata().stream()
                .mapToInt(VectorLayer::minZoom)
                .max()
                .orElseGet(matrixSet()::minZoomLevel);
    }
}
