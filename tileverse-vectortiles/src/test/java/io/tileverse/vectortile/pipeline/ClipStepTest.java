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

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

class ClipStepTest {

    private final ClipStep clip = new ClipStep(4096, 64);

    @Test
    void keepsPointInsideBuffer() {
        Geometry point = wkt("POINT (4120 2000)");

        assertThat(clip.apply(point)).isSameAs(point);
    }

    @Test
    void dropsPointOutsideBuffer() {
        Geometry point = wkt("POINT (5000 2000)");

        assertThat(clip.apply(point)).isNull();
    }

    // The envelope of the line is the clip envelope: its edges belong to the clip area
    @Test
    void keepsGeometryInsideEnvelopeAsIs() {
        Geometry line = wkt("LINESTRING (-64 -64, 4160 4160)");

        assertThat(clip.apply(line)).isSameAs(line);
    }

    @Test
    void clipsCrossingLine() {
        Geometry clipped = clip.apply(wkt("LINESTRING (3000 3000, 4200 4200)"));

        Geometry expected = wkt("LINESTRING (3000 3000, 4160 4160)");
        assertThat(clipped.equalsExact(expected))
                .as("%s equals %s exactly", clipped, expected)
                .isTrue();
    }

    @Test
    void dropsLineOutside() {
        Geometry clipped = clip.apply(wkt("LINESTRING (5000 5000, 6000 6000)"));

        assertThat(clipped == null || clipped.isEmpty())
                .as("%s is null or empty", clipped)
                .isTrue();
    }

    // The clip area ends at 4096 + 64 on both axes
    @Test
    void clipsPolygonToBuffer() {
        Geometry clipped = clip.apply(wkt("POLYGON ((4000 4000, 5000 4000, 5000 5000, 4000 5000, 4000 4000))"));

        assertThat(clipped.getArea()).isEqualTo(160.0 * 160.0);
    }

    // The overlay throws a TopologyException on this self-crossing polygon
    @Test
    void keepsInputWhenOverlayFails() {
        Geometry bowtie = wkt("POLYGON ((4000 4000, 5000 5000, 5000 4000, 4000 5000, 4000 4000))");

        assertThat(clip.apply(bowtie)).isSameAs(bowtie);
    }

    // The clip area ends at 256 + 2.5 on both axes
    @Test
    void clipsAtFractionalBounds() {
        ClipStep fractional = new ClipStep(256, 2.5);
        Geometry inside = wkt("POINT (258.4 0)");

        assertThat(fractional.apply(inside)).isSameAs(inside);
        assertThat(fractional.apply(wkt("POINT (258.6 0)"))).isNull();
    }

    @ParameterizedTest(name = "extent {0}, buffer {1}")
    @MethodSource("invalidBounds")
    void rejectsInvalidBounds(double extent, double buffer, String message) {
        assertThatThrownBy(() -> new ClipStep(extent, buffer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    static Stream<Arguments> invalidBounds() {
        return Stream.of(
                arguments(0.0, 64.0, "The extent must be positive and finite: 0.0"),
                arguments(Double.NaN, 64.0, "The extent must be positive and finite: NaN"),
                arguments(Double.POSITIVE_INFINITY, 64.0, "The extent must be positive and finite: Infinity"),
                arguments(4096.0, -1.0, "The buffer must be at least 0 and finite: -1.0"),
                arguments(4096.0, Double.NaN, "The buffer must be at least 0 and finite: NaN"));
    }

    @Test
    void describesParameters() {
        assertThat(clip.extent()).isEqualTo(4096.0);
        assertThat(clip.buffer()).isEqualTo(64.0);
        assertThat(clip).hasToString("ClipStep[extent=4096.0, buffer=64.0]");
    }
}
