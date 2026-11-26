/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.locationtech.jts.geom.Geometry;

/**
 * Compares the features of a tile written again from the model with those of its source tile. The writer leaves out
 * repeated points and orients the rings: both tiles are compared without repeated points, normalized.
 */
final class RoundTripAssertions {

    private RoundTripAssertions() {
        // static methods only
    }

    /**
     * Checks the features of the rebuilt tile against those of the source with a geometry, in order. A failure names
     * the first feature that differs, or the features past the end of the other tile.
     */
    static void assertSameFeatures(VectorTile rebuilt, VectorTile source) {
        List<FeatureContent> actual = contents(rebuilt);
        List<FeatureContent> expected =
                contents(source).stream().map(FeatureContent::withWrittenId).toList();
        int compared = Math.min(actual.size(), expected.size());
        for (int i = 0; i < compared; i++) {
            Assertions.assertThat(actual.get(i)).as("feature %d", i).isEqualTo(expected.get(i));
        }
        Assertions.assertThat(actual.subList(compared, actual.size()))
                .as("features not in the source")
                .isEmpty();
        Assertions.assertThat(expected.subList(compared, expected.size()))
                .as("features left out")
                .isEmpty();
    }

    /** The features of the tile with a non-empty geometry, layer after layer. */
    private static List<FeatureContent> contents(VectorTile tile) {
        List<FeatureContent> contents = new ArrayList<>();
        for (Layer layer : tile.getLayers()) {
            layer.getFeatures()
                    .filter(feature -> !feature.getGeometry().isEmpty())
                    .forEach(feature -> contents.add(FeatureContent.of(layer, feature)));
        }
        return contents;
    }

    /**
     * A feature as compared by the round trip, with the name and the extent of its layer, and its geometry without
     * repeated points and normalized. The geometries compare with {@code equalsExact}, as {@link Geometry#equals} does.
     */
    private record FeatureContent(
            String layer, int extent, long id, Map<String, Object> attributes, Geometry geometry) {

        static FeatureContent of(Layer layer, Feature feature) {
            Geometry canonical =
                    Geometries.withoutRepeatedPoints(feature.getGeometry()).norm();
            return new FeatureContent(
                    layer.getName(), layer.getExtent(), feature.getId(), feature.getAttributes(), canonical);
        }

        /**
         * The feature with its id as written from the model. A negative id means no id, and the decoder reads no id as
         * 0. The decoder returns an id of 2^63 or above as a negative long, and the source tiles hold such ids.
         */
        FeatureContent withWrittenId() {
            if (id >= 0) {
                return this;
            }
            return new FeatureContent(layer, extent, 0, attributes, geometry);
        }
    }
}
