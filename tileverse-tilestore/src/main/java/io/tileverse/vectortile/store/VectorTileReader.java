/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.store;

import io.tileverse.tiling.store.TileData;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Internal reader that extracts features from a single {@link VectorTile} based on a {@link VectorTilesQuery}.
 *
 * @param tileData the tile metadata and its decoded content
 * @param query the query parameters defining which layers and features to extract
 */
record VectorTileReader(TileData<VectorTile> tileData, VectorTilesQuery query) {

    /**
     * Extracts all matching features from all matching layers in the tile.
     *
     * @return a stream of features
     */
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
