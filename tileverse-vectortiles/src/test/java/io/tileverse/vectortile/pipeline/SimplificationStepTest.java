/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import static io.tileverse.vectortile.mvt.Geometries.wkt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

class SimplificationStepTest {

    @Test
    void simplifiesLineWithDouglasPeucker() {
        Geometry simplified = new SimplificationStep(0.5).apply(wkt("LINESTRING (0 0, 1 0.1, 2 0)"));

        Geometry expected = wkt("LINESTRING (0 0, 2 0)");
        assertThat(simplified.equalsExact(expected))
                .as("%s equals %s exactly", simplified, expected)
                .isTrue();
    }

    // The topology-preserving simplifier would keep the thin first polygon
    @Test
    void dropsCollapsedPolygonMember() {
        Geometry polygons = wkt("MULTIPOLYGON (((0 0, 10 0, 5 0.5, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))");

        Geometry simplified = new SimplificationStep(1).apply(polygons);

        Geometry expected = wkt("POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))");
        assertThat(simplified.equalsExact(expected))
                .as("%s equals %s exactly", simplified, expected)
                .isTrue();
    }

    // Douglas-Peucker collapses the shell of the first polygon and keeps its hole: it returns a ring and a polygon
    @Test
    void keepsMultiPolygonPolygonal() {
        Geometry polygons = wkt("""
                MULTIPOLYGON (((0 0, 50 -0.9, 100 0, 50 0.9, 0 0), (45 -0.6, 55 -0.6, 55 0.6, 45 0.6, 45 -0.6)),
                ((20 20, 25 20.5, 30 20, 30 30, 20 30, 20 20)))
                """);

        Geometry simplified = new SimplificationStep(1).apply(polygons);

        assertThat(simplified).isInstanceOf(Polygonal.class);
        assertThat(simplified.getNumGeometries()).isEqualTo(2);
        assertThat(simplified.getNumPoints()).isLessThan(polygons.getNumPoints());
    }

    @Test
    void leavesPointsAlone() {
        Geometry point = wkt("POINT (1 1)");

        assertThat(new SimplificationStep(1).apply(point)).isSameAs(point);
    }

    @ParameterizedTest
    @ValueSource(doubles = {0, -1, Double.NaN, Double.POSITIVE_INFINITY})
    void rejectsInvalidTolerance(double tolerance) {
        assertThatThrownBy(() -> new SimplificationStep(tolerance))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The distance tolerance must be positive and finite: " + tolerance);
    }

    @Test
    void describesTolerance() {
        assertThat(new SimplificationStep(2)).hasToString("SimplificationStep[distanceTolerance=2.0]");
    }
}
