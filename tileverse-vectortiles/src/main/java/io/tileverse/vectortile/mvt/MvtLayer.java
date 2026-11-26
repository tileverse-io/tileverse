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
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A layer of a decoded tile, as a view over the tile and the index of the layer. */
record MvtLayer(MvtTile tile, LayerIndex index) implements VectorTile.Layer {

    @Override
    public VectorTile getTile() {
        return tile;
    }

    @Override
    public String getName() {
        return index.name();
    }

    @Override
    public int getExtent() {
        return index.extent();
    }

    @Override
    public Set<String> getAttributeNames() {
        return index.attributeNames();
    }

    @Override
    public int count() {
        return index.featureCount();
    }

    /** Returns a stream of new features, each one read from the tile bytes when the stream reaches it. */
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
        return IntStream.range(0, count())
                .mapToObj(feature -> createFeature(feature, decoder))
                .filter(filter);
    }

    private Feature createFeature(int feature, GeometryReader decoder) {
        return new MvtFeature(this, feature, decoder);
    }

    @Override
    public String toString() {
        return "%s[name=%s, extent=%d, features=%d]"
                .formatted(getClass().getSimpleName(), getName(), getExtent(), count());
    }
}
