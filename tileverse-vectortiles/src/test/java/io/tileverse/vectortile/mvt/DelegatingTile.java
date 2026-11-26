/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * A tile of another {@link VectorTile} implementation, for the tests: its layers delegate to those of a decoded tile,
 * and none of them is a decoded tile or layer. Its features are wrappers too, and the codec and the builders read the
 * tile through the model only. The layers of a tile from {@link #withDecodedFeatures} hand out the decoded features as
 * they are instead, as a layer filtering the features of a decoded tile does.
 *
 * @param wrapsFeatures whether the layers hand out wrappers of the decoded features, or the decoded features
 */
record DelegatingTile(VectorTile tile, boolean wrapsFeatures) implements VectorTile {

    /** A tile whose layers and features are all wrappers. */
    DelegatingTile(VectorTile tile) {
        this(tile, true);
    }

    /** A tile whose layers are wrappers, handing out the decoded features of the tile as they are. */
    static DelegatingTile withDecodedFeatures(VectorTile tile) {
        return new DelegatingTile(tile, false);
    }

    @Override
    public Optional<Envelope> boundingBox() {
        return tile.boundingBox();
    }

    @Override
    public VectorTile withBoundingBox(Envelope bounds) {
        return new DelegatingTile(tile.withBoundingBox(bounds), wrapsFeatures);
    }

    @Override
    public Set<String> getLayerNames() {
        return tile.getLayerNames();
    }

    @Override
    public List<Layer> getLayers() {
        return tile.getLayers().stream().map(this::delegatingLayer).toList();
    }

    @Override
    public Optional<Layer> getLayer(String name) {
        return tile.getLayer(name).map(this::delegatingLayer);
    }

    // The bytes of the decoded tile are not the bytes of this tile
    @Override
    public OptionalInt serializedSize() {
        return OptionalInt.empty();
    }

    private Layer delegatingLayer(Layer layer) {
        return new DelegatingLayer(this, layer);
    }

    private record DelegatingLayer(DelegatingTile tile, Layer layer) implements Layer {

        @Override
        public VectorTile getTile() {
            return tile;
        }

        @Override
        public String getName() {
            return layer.getName();
        }

        @Override
        public int getExtent() {
            return layer.getExtent();
        }

        @Override
        public Set<String> getAttributeNames() {
            return layer.getAttributeNames();
        }

        @Override
        public int count() {
            return layer.count();
        }

        @Override
        public Stream<Feature> getFeatures() {
            return layer.getFeatures().map(this::delegatingFeature);
        }

        @Override
        public Stream<Feature> getFeatures(Predicate<Feature> filter) {
            return getFeatures().filter(filter);
        }

        @Override
        public Stream<Feature> getFeatures(Predicate<Feature> filter, GeometryReader decoder) {
            Stream<Feature> features = layer.getFeatures(feature -> true, decoder);
            return features.map(this::delegatingFeature).filter(filter);
        }

        private Feature delegatingFeature(Feature feature) {
            if (!tile.wrapsFeatures()) {
                return feature;
            }
            return new DelegatingFeature(this, feature);
        }
    }

    private record DelegatingFeature(DelegatingLayer layer, Feature feature) implements Feature {

        @Override
        public Layer getLayer() {
            return layer;
        }

        @Override
        public long getId() {
            return feature.getId();
        }

        @Override
        public Geometry getGeometry() {
            return feature.getGeometry();
        }

        @Override
        public Object getAttribute(String attributeName) {
            return feature.getAttribute(attributeName);
        }

        @Override
        public Map<String, Object> getAttributes() {
            return feature.getAttributes();
        }
    }
}
