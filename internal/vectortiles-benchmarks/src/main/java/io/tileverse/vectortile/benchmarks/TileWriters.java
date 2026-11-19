/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import io.tileverse.vectortile.benchmarks.TileFeatures.InputFeature;
import io.tileverse.vectortile.benchmarks.TileFeatures.LayerFeatures;
import io.tileverse.vectortile.mvt.LayerBuilder;
import io.tileverse.vectortile.mvt.VectorTileBuilder;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * Writes the features of a tile with either library: the layers in order, and the features of a layer with their ids,
 * attributes and geometries. The parity test and the write benchmarks write the tiles with these methods.
 */
public final class TileWriters {

    private TileWriters() {}

    /**
     * Writes the features with the tileverse builder, one layer at a time.
     *
     * @param features the features of the tile
     * @param builder the builder, cleared once the tile is written
     * @return the encoded tile
     */
    public static byte[] writeTileverse(TileFeatures features, VectorTileBuilder builder) {
        for (LayerFeatures layer : features.layers()) {
            LayerBuilder layerBuilder = builder.layer().name(layer.name());
            for (InputFeature feature : layer.features()) {
                layerBuilder.feature(feature.attributes(), feature.geometry(), feature.id());
            }
            layerBuilder.build();
        }
        return builder.toByteArray();
    }

    /**
     * Adds the features to the java-vector-tile encoder, then encodes the tile.
     *
     * @param features the features of the tile
     * @param encoder a new encoder: an encoder keeps the features added to it
     * @return the encoded tile
     */
    public static byte[] writeJavaVectorTile(TileFeatures features, VectorTileEncoder encoder) {
        for (LayerFeatures layer : features.layers()) {
            for (InputFeature feature : layer.features()) {
                encoder.addFeature(layer.name(), feature.attributes(), feature.geometry(), feature.id());
            }
        }
        return encoder.encode();
    }
}
