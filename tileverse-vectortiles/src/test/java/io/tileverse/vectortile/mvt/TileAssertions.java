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

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import java.util.Arrays;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

/**
 * AssertJ-style fluent assertions for MVT tiles.
 *
 * <p>Usage example:
 * <pre>{@code
 * Tile tile = ...;
 * TileAssertions.assertThat(tile)
 *     .hasLayerCount(2)
 *     .hasLayer("roads")
 *     .hasLayer("buildings");
 * }</pre>
 */
class TileAssertions extends AbstractAssert<TileAssertions, VectorTile> {

    public TileAssertions(VectorTile actual) {
        super(actual, TileAssertions.class);
    }

    /**
     * Entry point for fluent assertions on MVT tiles.
     */
    public static TileAssertions assertThat(VectorTile actual) {
        return new TileAssertions(actual);
    }

    /**
     * Assert that the tile has the specified number of layers.
     */
    public TileAssertions hasLayerCount(int expectedCount) {
        isNotNull();
        Assertions.assertThat(actual.getLayers())
                .overridingErrorMessage(
                        "Expected tile to have <%s> layers but had <%s>",
                        expectedCount, actual.getLayers().size())
                .hasSize(expectedCount);
        return this;
    }

    /**
     * Assert that the tile has a layer with the specified name.
     */
    public TileAssertions hasLayer(String layerName) {
        isNotNull();
        Assertions.assertThat(actual.getLayerNames())
                .overridingErrorMessage(
                        "Expected tile to contain layer <%s> but layers were <%s>", layerName, actual.getLayerNames())
                .contains(layerName);
        return this;
    }

    /**
     * Assert that the tile has layers with the specified names (in any order).
     */
    public TileAssertions hasLayers(String... layerNames) {
        isNotNull();
        Assertions.assertThat(actual.getLayerNames())
                .overridingErrorMessage(
                        "Expected tile to contain layers <%s> but layers were <%s>",
                        Arrays.toString(layerNames), actual.getLayerNames())
                .contains(layerNames);
        return this;
    }

    /**
     * Assert that the tile has exactly the specified layers (in any order).
     */
    public TileAssertions hasExactLayers(String... layerNames) {
        isNotNull();
        Assertions.assertThat(actual.getLayerNames())
                .overridingErrorMessage(
                        "Expected tile to have exactly layers <%s> but layers were <%s>",
                        Arrays.toString(layerNames), actual.getLayerNames())
                .containsExactlyInAnyOrder(layerNames);
        return this;
    }

    /**
     * Navigate to layer assertions for the specified layer name.
     * This preserves the tile context for downstream assertions.
     */
    public LayerAssertions layer(String layerName) {
        Layer layer = actual.getLayer(layerName).orElse(null);
        Assertions.assertThat(layer)
                .overridingErrorMessage(
                        "Expected tile to contain layer <%s> but layers were <%s>", layerName, actual.getLayerNames())
                .isNotNull();
        return new LayerAssertions(layer, this);
    }
}
