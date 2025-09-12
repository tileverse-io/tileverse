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

public abstract class VectorTileStore extends AbstractTileStore<VectorTile> {

    public VectorTileStore(TileMatrixSet matrixSet) {
        super(matrixSet);
    }

    public abstract List<VectorLayer> getVectorLayersMetadata();

    public Optional<VectorLayer> getLayerMetadata(String layerId) {
        return getVectorLayersMetadata().stream()
                .filter(l -> layerId.equals(l.id()))
                .findFirst();
    }

    /**
     * @return the extent in the {@link TileMatrixSet#crsId() TileMatrixSet CRS}
     */
    public abstract BoundingBox2D getExtent();

    public Stream<Feature> getFeatures(VectorTilesQuery query) {
        // find matching tiles
        Stream<TileData<VectorTile>> tilesInExtentAndZoomLevel = findTiles(query);

        Stream<VectorTileReader> tileReaders = tilesInExtentAndZoomLevel.map(tile -> new VectorTileReader(tile, query));

        return tileReaders.flatMap(VectorTileReader::getFeatures);
    }

    protected Stream<TileData<VectorTile>> findTiles(VectorTilesQuery query) {
        final int zoomLevel = determineZoomLevel(query);
        final List<BoundingBox2D> queryExtent = query.extent();
        Stream<TileData<VectorTile>> matchingTiles = findTiles(queryExtent, zoomLevel);
        return matchingTiles;
    }

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

        int maxMinZoom = getVectorLayersMetadata().stream()
                .mapToInt(VectorLayer::minZoom)
                .max()
                .orElseGet(matrixSet()::minZoomLevel);
        return maxMinZoom;
    }
}
