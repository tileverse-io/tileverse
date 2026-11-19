/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import static io.tileverse.vectortile.mvt.Geometries.wkt;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

class SizeFilterStepTest {

    private final SizeFilterStep filter = new SizeFilterStep(1, 1);

    @Test
    void dropsSmallPolygon() {
        Geometry polygon = wkt("POLYGON ((0 0, 0.5 0, 0.5 0.5, 0 0.5, 0 0))");

        assertThat(filter.apply(polygon)).isNull();
    }

    @Test
    void keepsPolygonOfAreaOne() {
        Geometry polygon = wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");

        assertThat(filter.apply(polygon)).isSameAs(polygon);
    }

    @ParameterizedTest
    @MethodSource("shortLines")
    void dropsShortLine(String text) {
        Geometry line = wkt(text);

        assertThat(filter.apply(line)).isNull();
    }

    static Stream<String> shortLines() {
        return Stream.of("LINESTRING (0 0, 0.9 0)", "LINEARRING (0 0, 0.2 0, 0 0.2, 0 0)");
    }

    @Test
    void keepsLineOfLengthOne() {
        Geometry line = wkt("LINESTRING (0 0, 1 0)");

        assertThat(filter.apply(line)).isSameAs(line);
    }

    // java-vector-tile checks the length of a LineString only
    @Test
    void keepsMultiLineStringOfShortLines() {
        Geometry lines = wkt("MULTILINESTRING ((0 0, 0.1 0), (5 5, 5.1 5))");

        assertThat(filter.apply(lines)).isSameAs(lines);
    }

    @ParameterizedTest
    @MethodSource("points")
    void keepsPoints(String text) {
        Geometry points = wkt(text);

        assertThat(filter.apply(points)).isSameAs(points);
    }

    static Stream<String> points() {
        return Stream.of("POINT (1 1)", "MULTIPOINT ((1 1), (2 2))");
    }

    @Test
    void dropsSmallMultiPolygon() {
        Geometry polygons =
                wkt("MULTIPOLYGON (((0 0, 0.5 0, 0.5 0.5, 0 0.5, 0 0)), ((5 5, 5.5 5, 5.5 5.5, 5 5.5, 5 5)))");

        assertThat(filter.apply(polygons)).isNull();
    }
}
