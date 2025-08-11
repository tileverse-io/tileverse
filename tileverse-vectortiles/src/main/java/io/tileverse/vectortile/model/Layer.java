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

import java.util.Set;
import java.util.stream.Stream;

/**
 * A named collection of vector features with shared coordinate space.
 * <p>
 * Layers group related features and define the coordinate extent.
 * All features within a layer share the same coordinate precision
 * and valid coordinate range [0, extent-1].
 */
public interface Layer {

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
}
