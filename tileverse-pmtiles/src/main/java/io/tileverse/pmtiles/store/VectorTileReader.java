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

import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

record VectorTileReader(TileData<VectorTile> tileData, VectorTilesQuery query) {

    public Stream<VectorTile.Layer.Feature> getFeatures() {

        Stream<VectorTileLayerReader> layers = extractLayers(tileData, query);

        return layers.flatMap(VectorTileLayerReader::getFeatures);
    }

    private Stream<VectorTileLayerReader> extractLayers(TileData<VectorTile> vectortile, VectorTilesQuery query) {

        Stream<Layer> layers;

        final boolean allLayers = query.layers().isEmpty();
        final VectorTile vectorTile = vectortile.data();
        if (allLayers) {
            layers = vectorTile.getLayers().stream();
        } else {
            List<String> layerNames = query.layers().orElseThrow();
            layers = layerNames.stream().map(vectorTile::getLayer).flatMap(Optional::stream);
        }

        return layers.map(layer -> new VectorTileLayerReader(layer, tileData, query));
    }
}
