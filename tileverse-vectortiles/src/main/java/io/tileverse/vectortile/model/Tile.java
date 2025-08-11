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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A vector tile containing multiple named layers.
 * <p>
 * Represents the root container for vector tile data, organizing
 * features into logical layers. Each layer can have different
 * coordinate extents and feature types.
 */
public interface Tile {

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

    /**
     * Returns a stream of all features from all layers in this tile.
     *
     * @return a stream of features from all layers
     * @see Layer#getFeatures()
     */
    default Stream<Feature> getFeatures() {
        return getLayers().stream().flatMap(Layer::getFeatures);
    }
}
