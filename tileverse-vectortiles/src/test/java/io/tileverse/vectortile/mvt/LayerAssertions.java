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
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

/**
 * AssertJ-style fluent assertions for MVT layers.
 *
 * <p>Usage example:
 * <pre>{@code
 * Layer layer = ...;
 * LayerAssertions.assertThat(layer)
 *     .hasName("roads")
 *     .hasExtent(4096)
 *     .hasFeatureCount(10)
 *     .hasAttribute("highway")
 *     .hasAttribute("name");
 * }</pre>
 */
class LayerAssertions extends AbstractAssert<LayerAssertions, Layer> {

    private final TileAssertions parentTile;

    public LayerAssertions(Layer actual) {
        super(actual, LayerAssertions.class);
        this.parentTile = null;
    }

    LayerAssertions(Layer actual, TileAssertions parentTile) {
        super(actual, LayerAssertions.class);
        this.parentTile = parentTile;
    }

    /**
     * Entry point for fluent assertions on MVT layers.
     */
    public static LayerAssertions assertThat(Layer actual) {
        return new LayerAssertions(actual);
    }

    /**
     * Assert that the layer has the specified name.
     */
    public LayerAssertions hasName(String expectedName) {
        isNotNull();
        Assertions.assertThat(actual.getName())
                .overridingErrorMessage("Expected layer name to be <%s> but was <%s>", expectedName, actual.getName())
                .isEqualTo(expectedName);
        return this;
    }

    /**
     * Assert that the layer has the specified extent.
     */
    public LayerAssertions hasExtent(int expectedExtent) {
        isNotNull();
        Assertions.assertThat(actual.getExtent())
                .overridingErrorMessage(
                        "Expected layer extent to be <%s> but was <%s>", expectedExtent, actual.getExtent())
                .isEqualTo(expectedExtent);
        return this;
    }

    /**
     * Assert that the layer has the specified number of features.
     */
    public LayerAssertions hasFeatureCount(int expectedCount) {
        isNotNull();
        Assertions.assertThat(actual.count())
                .overridingErrorMessage(
                        "Expected layer to have <%s> features but had <%s>", expectedCount, actual.count())
                .isEqualTo(expectedCount);
        return this;
    }

    /**
     * Assert that the layer has an attribute with the specified name.
     */
    public LayerAssertions hasAttribute(String attributeName) {
        isNotNull();
        Assertions.assertThat(actual.getAttributeNames())
                .overridingErrorMessage(
                        "Expected layer to have attribute <%s> but attributes were <%s>",
                        attributeName, actual.getAttributeNames())
                .contains(attributeName);
        return this;
    }

    /**
     * Assert that the layer has attributes with the specified names (in any order).
     */
    public LayerAssertions hasAttributes(String... attributeNames) {
        isNotNull();
        Assertions.assertThat(actual.getAttributeNames())
                .overridingErrorMessage(
                        "Expected layer to have attributes <%s> but attributes were <%s>",
                        Arrays.toString(attributeNames), actual.getAttributeNames())
                .contains(attributeNames);
        return this;
    }

    /**
     * Assert that the layer has exactly the specified attributes (in any order).
     */
    public LayerAssertions hasExactAttributes(String... attributeNames) {
        isNotNull();
        Assertions.assertThat(actual.getAttributeNames())
                .overridingErrorMessage(
                        "Expected layer to have exactly attributes <%s> but attributes were <%s>",
                        Arrays.toString(attributeNames), actual.getAttributeNames())
                .containsExactlyInAnyOrder(attributeNames);
        return this;
    }

    /**
     * Navigate to feature assertions for the first feature in this layer.
     */
    public FeatureAssertions firstFeature() {
        return feature(0);
    }

    /**
     * Navigate to feature assertions for the feature at the specified index.
     * This preserves the layer context for attribute assertions.
     */
    public FeatureAssertions feature(int index) {
        isNotNull();
        List<Feature> features = actual.getFeatures().toList();
        Assertions.assertThat(features)
                .overridingErrorMessage(
                        "Expected layer to have at least <%s> features but had <%s>", index + 1, features.size())
                .hasSizeGreaterThan(index);

        Feature feature = features.get(index);
        return new FeatureAssertions(feature, actual, this);
    }

    /**
     * Navigate back to the parent tile assertions.
     */
    public TileAssertions tile() {
        if (parentTile == null) {
            throw new IllegalStateException("No parent tile context available");
        }
        return parentTile;
    }
}
