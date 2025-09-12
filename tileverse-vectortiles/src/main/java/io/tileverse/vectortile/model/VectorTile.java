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
package io.tileverse.vectortile.model;

import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * A vector tile containing multiple named layers.
 * <p>
 * Represents the root container for vector tile data, organizing
 * features into logical layers. Each layer can have different
 * coordinate extents and feature types.
 */
public interface VectorTile {

    Optional<Envelope> boundingBox();

    VectorTile withBoundingBox(Envelope bounds);

    /**
     * Returns all layer names in this tile.
     */
    Set<String> getLayerNames();

    /**
     * Returns all layers in this tile.
     */
    List<Layer> getLayers();

    /**
     * Returns the layer with the specified name.
     *
     * @param name the layer name
     * @return the layer, or empty if no layer exists with that name
     */
    Optional<Layer> getLayer(String name);

    /**
     * Returns a stream of features from the specified layer.
     *
     * @param layerName the name of the layer to get features from
     * @return a sequential-only stream of features from the specified layer
     * @see Layer#getFeatures()
     * @see Feature#copy()
     */
    default Stream<Feature> getFeatures(String layerName) {
        return getLayer(layerName).map(Layer::getFeatures).orElseGet(Stream::empty);
    }

    default Stream<Feature> getFeatures(String layerName, Predicate<Feature> filter, GeometryReader decoder) {
        return getLayer(layerName).map(l -> l.getFeatures(filter, decoder)).orElseGet(Stream::empty);
    }

    /**
     * Returns a stream of all features from all layers in this tile.
     *
     * @return a stream of features from all layers
     * @see Layer#getFeatures()
     */
    default Stream<Feature> getFeatures() {
        return getLayers().stream().flatMap(Layer::getFeatures);
    }

    /**
     * A named collection of vector features with shared coordinate space.
     * <p>
     * Layers group related features and define the coordinate extent.
     * All features within a layer share the same coordinate precision
     * and valid coordinate range [0, extent-1].
     */
    public static interface Layer {

        VectorTile getTile();

        /**
         * Returns the layer name.
         */
        String getName();

        /**
         * Returns the coordinate extent for this layer.
         * <p>
         * Defines the valid coordinate range [0, extent-1] and precision.
         * Defaults to 4096 in MVT. Common values are 256, 512, 1024, 2048, or 4096.
         */
        int getExtent();

        /**
         * Returns all attribute names present in this layer.
         * <p>
         * Represents the union of all attribute names across all features.
         */
        Set<String> getAttributeNames();

        /**
         * Returns the number of features in this layer.
         */
        int count();

        /**
         * Returns a stream of features in this layer.
         * <p>
         * Geometries are returned in their raw extent space (0 to extent-1) as stored in the MVT data.
         * <p>
         * If the features don't have ids, they'll be assigned an id based on their position in the list
         * @return a sequential-only stream that reuses Feature instances for memory
         *         efficiency
         * @see MvtFeature#copy() for creating independent Feature copies
         */
        Stream<Feature> getFeatures();

        Stream<Feature> getFeatures(Predicate<Feature> filter);

        Stream<Feature> getFeatures(Predicate<Feature> filter, GeometryReader decoder);

        /**
         * A vector tile feature with geometry and attributes.
         * <p>
         * Features represent individual geographic objects within a tile layer.
         * Each feature has a geometry, optional attributes, and an identifier.
         * Coordinates are in tile extent space [0, extent-1].
         */
        public static interface Feature {

            /**
             * Returns the layer containing this feature.
             */
            Layer getLayer();

            /**
             * Returns the feature identifier.
             * <p>
             * Features may have explicit IDs or default to 0.
             */
            long getId();

            /**
             * Returns the feature's geometry in tile extent coordinates.
             * <p>
             * Coordinates are preserved exactly as stored in the MVT data,
             * typically in the range [0, extent-1] where extent is the layer's extent value.
             */
            Geometry getGeometry();

            /**
             * Returns a single attribute value by name.
             *
             * @param attributeName the attribute name
             * @return the attribute value, or null if the attribute doesn't exist
             */
            Object getAttribute(String attributeName);

            /**
             * Returns all feature attributes as a map.
             * <p>
             * The returned map reflects the feature's current attribute state.
             * Missing attributes return null when accessed individually.
             */
            Map<String, Object> getAttributes();
        }
    }
}
