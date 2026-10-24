/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static java.util.Objects.requireNonNull;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

record MvtLayer(MvtTile tile, VectorTileProto.Tile.Layer layerProto) implements VectorTile.Layer {

    @Override
    public VectorTile getTile() {
        return tile;
    }

    @Override
    public String getName() {
        return layerProto.getName();
    }

    @Override
    public int getExtent() {
        if (!layerProto.hasExtent()) {
            return 4096;
        }
        return layerProto.getExtent();
    }

    @Override
    public Set<String> getAttributeNames() {
        return Set.copyOf(layerProto.getKeysList());
    }

    @Override
    public int count() {
        return layerProto.getFeaturesCount();
    }

    /**
     * Returns a stream of features in this layer.
     *
     * <p>If the features don't have ids, they'll be assigned an id based on their position in the list
     *
     * @return a sequential-only stream that reuses Feature instances for memory efficiency
     * @see MvtFeature#copy() for creating independent Feature copies
     */
    @Override
    public Stream<Feature> getFeatures() {
        return getFeatures(f -> true);
    }

    @Override
    public Stream<Feature> getFeatures(Predicate<Feature> filter) {
        return getFeatures(filter, new GeometryDecoder());
    }

    @Override
    public Stream<Feature> getFeatures(Predicate<Feature> filter, GeometryReader decoder) {
        requireNonNull(filter, "filter");
        requireNonNull(decoder, "decoder");
        return layerProto.getFeaturesList().stream()
                .map(f -> createFeature(f, decoder))
                .filter(filter);
    }

    private Feature createFeature(VectorTileProto.Tile.Feature feature, GeometryReader decoder) {
        return new MvtFeature(this, feature, decoder);
    }

    @Override
    public String toString() {
        return "%s[name=%s, extent=%d, features=%d]"
                .formatted(getClass().getSimpleName(), getName(), getExtent(), count());
    }
}
