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
package io.tileverse.vectortile.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.tiling.matrix.DefaultTileMatrixSets;
import io.tileverse.tiling.matrix.Tile;
import io.tileverse.tiling.matrix.TileMatrixSet;
import io.tileverse.tiling.store.TileData;
import io.tileverse.tiling.store.TileStore.Strategy;
import io.tileverse.vectortile.model.VectorTile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VectorTileStoreTest {

    private static final TileMatrixSet ZOOMS_2_TO_14 = DefaultTileMatrixSets.WORLD_EPSG3857.subset(2, 14);

    // A layer without a minzoom is valid TileJSON
    @ParameterizedTest
    @MethodSource("layerMinZooms")
    void qualityZoomIsHighestMinZoom(List<Integer> minZooms, int expectedZoom) {
        VectorTileStore store = new StoreWithLayers(layersWithMinZooms(minZooms));
        VectorTilesQuery query = new VectorTilesQuery().strategy(Strategy.QUALITY);

        assertThat(store.determineZoomLevel(query)).isEqualTo(expectedZoom);
    }

    static Stream<Arguments> layerMinZooms() {
        return Stream.of(
                Arguments.of(List.of(3, 5), 5),
                Arguments.of(Arrays.asList(3, null, 5), 5),
                Arguments.of(Arrays.asList(null, null), 2),
                Arguments.of(List.of(), 2));
    }

    private static List<VectorLayer> layersWithMinZooms(List<Integer> minZooms) {
        return minZooms.stream()
                .map(minZoom -> VectorLayer.of("layer", Map.of(), null, minZoom, null))
                .toList();
    }

    private static final class StoreWithLayers extends VectorTileStore {

        private final List<VectorLayer> layers;

        StoreWithLayers(List<VectorLayer> layers) {
            super(ZOOMS_2_TO_14);
            this.layers = layers;
        }

        @Override
        public List<VectorLayer> getVectorLayersMetadata() {
            return layers;
        }

        @Override
        public BoundingBox2D getExtent() {
            return ZOOMS_2_TO_14.boundingBox();
        }

        @Override
        public Optional<TileData<VectorTile>> loadTile(Tile tile) {
            return Optional.empty();
        }
    }
}
