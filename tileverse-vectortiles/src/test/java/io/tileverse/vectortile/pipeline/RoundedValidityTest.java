/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import static io.tileverse.vectortile.mvt.Geometries.wkt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.VectorTileCodec;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.GeometryEditor;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

class RoundedValidityTest {

    private static final PrecisionModel INTEGER_GRID = new PrecisionModel(1.0);

    /** A hole 0.25 units off the corner of the shell: 4 tile units off at the scale 16, on the corner at scale 1. */
    private static final String HOLE_NEAR_SHELL =
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.25 0.25, 0.25 2, 2 2, 2 0.25, 0.25 0.25))";

    private static final List<String> COMMITTED_TILES =
            List.of("14-8801-5371.vector.pbf", "bigtile.vector.pbf", "cells-11-1058-568.mvt", "cells-11-1065-567.mvt");

    @ParameterizedTest
    @MethodSource("polygons")
    void agreesWithIsValidOnPolygons(String text, boolean expected) {
        Geometry polygon = wkt(text);

        boolean valid = new RoundedValidity().isValid((Polygonal) polygon);
        boolean reference = isValidOnceRounded(polygon);

        assertThat(reference).as("answer of JTS once rounded").isEqualTo(expected);
        assertThat(valid).as("answer of the check").isEqualTo(expected);
    }

    static Stream<Arguments> polygons() {
        return Stream.of(
                arguments("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", true),
                // bow-tie
                arguments("POLYGON((0 0, 10 10, 10 0, 0 10, 0 0))", false),
                // hole outside the shell
                arguments("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (20 20, 21 20, 21 21, 20 21, 20 20))", false),
                // nested holes
                arguments(
                        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 8 1, 8 8, 1 8, 1 1), (2 2, 3 2, 3 3, 2 3, 2 2))",
                        false),
                // interior disconnected
                arguments("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0 5, 5 10, 10 5, 5 0, 0 5))", false),
                // hole touching the shell at one point
                arguments("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 5 10, 8 2, 2 2))", true),
                // valid before rounding, invalid after
                arguments("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.4 0.4, 0.4 2, 2 2, 2 0.4, 0.4 0.4))", false),
                // invalid before rounding, valid after
                arguments("POLYGON((0 0, 10 0, 10 10, 4.6 5.1, 5.4 4.9, 0 10, 0 0))", true),
                // overlapping shells
                arguments("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 15 5, 15 15, 5 15, 5 5)))", false),
                arguments("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 0, 30 0, 30 10, 20 10, 20 0)))", true));
    }

    // The jittered copies have coordinates off the integer grid, and they round back to the decoded polygons
    @Test
    void agreesWithIsValidOnTileFixtures() throws IOException {
        RoundedValidity validity = new RoundedValidity();
        List<Geometry> disagreements = new ArrayList<>();
        Map<Boolean, Integer> answerCounts = new HashMap<>();

        for (Geometry polygon : tilePolygons()) {
            boolean valid = validity.isValid((Polygonal) polygon);
            if (valid != isValidOnceRounded(polygon)) {
                disagreements.add(polygon);
            }
            answerCounts.merge(valid, 1, Integer::sum);
        }

        assertThat(disagreements).isEmpty();
        assertThat(answerCounts).containsOnlyKeys(true, false);
    }

    // The check reuses the coordinates of earlier checks: the small polygon must not see the points of the large one
    @Test
    void keepsAnswerAfterLargerPolygon() {
        RoundedValidity validity = new RoundedValidity();
        Geometry large = wkt("POINT (5000 5000)").buffer(1000, 64);
        Geometry small = wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.4 0.4, 0.4 2, 2 2, 2 0.4, 0.4 0.4))");

        boolean largeValid = validity.isValid((Polygonal) large);
        boolean smallValid = validity.isValid((Polygonal) small);

        assertThat(largeValid).isTrue();
        assertThat(smallValid).isFalse();
    }

    // At scale 1, the hole rounds onto the shell
    @Test
    void checksAtScale() {
        Polygonal polygon = (Polygonal) wkt(HOLE_NEAR_SHELL);

        assertThat(new RoundedValidity().isValid(polygon)).isFalse();
        assertThat(new RoundedValidity(16).isValid(polygon)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(doubles = {0, -1, Double.NaN, Double.POSITIVE_INFINITY})
    void rejectsInvalidScale(double scale) {
        assertThatThrownBy(() -> new RoundedValidity(scale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The scale must be positive and finite: " + scale);
    }

    /** The answer of JTS on a copy of the polygon with each coordinate rounded to the closest integer. */
    private static boolean isValidOnceRounded(Geometry polygon) {
        return GeometryPrecisionReducer.reducePointwise(polygon, INTEGER_GRID).isValid();
    }

    /**
     * The polygons of the committed tiles as decoded, with the packed sequences of the decoder, followed by the same
     * polygons jittered.
     */
    private static List<Geometry> tilePolygons() throws IOException {
        List<Geometry> decoded = new ArrayList<>();
        for (String tile : COMMITTED_TILES) {
            decoded.addAll(polygonsOf(tile));
        }
        Jitter jitter = new Jitter(new SplittableRandom(42));
        List<Geometry> polygons = new ArrayList<>(decoded);
        for (Geometry polygon : decoded) {
            polygons.add(jitter.applyTo(polygon));
        }
        return polygons;
    }

    private static List<Geometry> polygonsOf(String resource) throws IOException {
        VectorTile tile = new VectorTileCodec().decode(resource(resource));
        List<Geometry> polygons = new ArrayList<>();
        for (Layer layer : tile.getLayers()) {
            layer.getFeatures()
                    .map(Feature::getGeometry)
                    .filter(geometry -> geometry instanceof Polygonal && !geometry.isEmpty())
                    .forEach(polygons::add);
        }
        return polygons;
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = Objects.requireNonNull(RoundedValidityTest.class.getResourceAsStream("/" + name))) {
            return in.readAllBytes();
        }
    }

    /**
     * Moves the points of the rings of polygons by offsets of at most 0.49 on both axes: a moved point rounds to its
     * place in a decoded tile. The closing point of a ring moves with its first point, and the ring stays closed.
     */
    private static final class Jitter extends GeometryEditor.CoordinateSequenceOperation {

        private static final double MAX_OFFSET = 0.49;

        private final SplittableRandom random;

        Jitter(SplittableRandom random) {
            this.random = random;
        }

        Geometry applyTo(Geometry polygon) {
            GeometryEditor editor = new GeometryEditor(polygon.getFactory());
            return editor.edit(polygon, this);
        }

        /** Returns a moved copy of the ring, a sequence of the same kind. */
        @Override
        public CoordinateSequence edit(CoordinateSequence ring, Geometry geometry) {
            CoordinateSequence moved = ring.copy();
            int closingPoint = ring.size() - 1;
            for (int i = 0; i < closingPoint; i++) {
                moved.setOrdinate(i, CoordinateSequence.X, ring.getX(i) + offset());
                moved.setOrdinate(i, CoordinateSequence.Y, ring.getY(i) + offset());
            }
            if (closingPoint > 0) {
                moved.setOrdinate(closingPoint, CoordinateSequence.X, moved.getX(0));
                moved.setOrdinate(closingPoint, CoordinateSequence.Y, moved.getY(0));
            }
            return moved;
        }

        private double offset() {
            return random.nextDouble(-MAX_OFFSET, MAX_OFFSET);
        }
    }
}
