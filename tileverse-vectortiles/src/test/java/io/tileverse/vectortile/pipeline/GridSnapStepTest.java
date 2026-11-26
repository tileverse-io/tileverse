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

class GridSnapStepTest {

    private final GridSnapStep snap = new GridSnapStep();

    @Test
    void snapsToIntegers() {
        Geometry snapped = snap.apply(wkt("POINT (4095.7 2048.3)"));

        Geometry expected = wkt("POINT (4096 2048)");
        assertThat(snapped.equalsExact(expected))
                .as("%s equals %s exactly", snapped, expected)
                .isTrue();
    }

    @Test
    void removesCollapsedParts() {
        Geometry snapped = snap.apply(wkt("MULTILINESTRING ((0.2 0.2, 0.3 0.3), (0 0, 5 5))"));

        Geometry expected = wkt("LINESTRING (0 0, 5 5)");
        assertThat(snapped.equalsExact(expected))
                .as("%s equals %s exactly", snapped, expected)
                .isTrue();
    }

    @Test
    void dropsCollapsedGeometry() {
        Geometry snapped = snap.apply(wkt("LINESTRING (0.2 0.2, 0.3 0.3)"));

        assertThat(snapped == null || snapped.isEmpty())
                .as("%s is null or empty", snapped)
                .isTrue();
    }

    // The reducer fails on this self-crossing polygon
    @Test
    void dropsPolygonWhenReductionFails() {
        Geometry bowtie = wkt("POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))");

        assertThat(snap.apply(bowtie)).isNull();
    }

    // At the scale 16, a tile unit is 0.0625 units of the geometry
    @Test
    void snapsToScaledGrid() {
        GridSnapStep scaled = new GridSnapStep(16);

        Geometry onIntegers = scaled.apply(wkt("POINT (1.03 2.97)"));
        Geometry onSixteenths = scaled.apply(wkt("POINT (1.04 2.96)"));

        Geometry integers = wkt("POINT (1 3)");
        Geometry sixteenths = wkt("POINT (1.0625 2.9375)");
        assertThat(onIntegers.equalsExact(integers))
                .as("%s equals %s exactly", onIntegers, integers)
                .isTrue();
        assertThat(onSixteenths.equalsExact(sixteenths))
                .as("%s equals %s exactly", onSixteenths, sixteenths)
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(doubles = {0, -1, Double.NaN, Double.POSITIVE_INFINITY})
    void rejectsInvalidScale(double scale) {
        assertThatThrownBy(() -> new GridSnapStep(scale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The scale must be positive and finite: " + scale);
    }

    @Test
    void describesStep() {
        assertThat(snap).hasToString("GridSnapStep[scale=1.0]");
    }
}
