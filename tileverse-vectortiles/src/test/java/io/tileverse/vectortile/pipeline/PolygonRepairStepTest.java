/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import static io.tileverse.vectortile.mvt.Geometries.wkt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

class PolygonRepairStepTest {

    // Once rounded, the hole shares two of its edges with the shell
    private static final String HOLE_ON_SHELL_ONCE_ROUNDED =
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.4 0.4, 0.4 2, 2 2, 2 0.4, 0.4 0.4))";

    /** A hole 0.25 units off the corner of the shell: 4 tile units off at the scale 16, on the corner at scale 1. */
    private static final String HOLE_NEAR_SHELL =
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.25 0.25, 0.25 2, 2 2, 2 0.25, 0.25 0.25))";

    private final PolygonRepairStep repair = new PolygonRepairStep();

    @Test
    void keepsValidPolygon() {
        Geometry polygon = wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 5 10, 8 2, 2 2))");

        assertThat(repair.apply(polygon)).isSameAs(polygon);
    }

    @ParameterizedTest
    @MethodSource("invalidOnceRounded")
    void repairsPolygonInvalidOnceRounded(String text) {
        Geometry repaired = repair.apply(wkt(text));

        assertThat(repaired).isInstanceOf(Polygonal.class);
        assertThat(new RoundedValidity().isValid((Polygonal) repaired)).isTrue();
    }

    static Stream<String> invalidOnceRounded() {
        return Stream.of(
                HOLE_ON_SHELL_ONCE_ROUNDED,
                // a bow-tie, rejected by the reducer and fixed as given
                "POLYGON((0 0, 10 10, 10 0, 0 10, 0 0))");
    }

    // The reducer returns the nested holes as given, and the fixer removes the inner hole
    @Test
    void fixesPolygonLeftInvalidBySnap() {
        Geometry nestedHoles =
                wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 8 1, 8 8, 1 8, 1 1), (2 2, 3 2, 3 3, 2 3, 2 2))");

        Geometry repaired = repair.apply(nestedHoles);

        Geometry expected = wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 8 1, 8 8, 1 8, 1 1))");
        assertThat(repaired.equalsNorm(expected))
                .as("%s equals %s once normalized", repaired, expected)
                .isTrue();
    }

    @Test
    void dropsPolygonCollapsedBySnap() {
        Geometry tiny = wkt("POLYGON((0.1 0.1, 0.3 0.1, 0.3 0.3, 0.1 0.3, 0.1 0.1))");

        assertThat(repair.apply(tiny)).isNull();
    }

    // A line crossing itself is valid
    @Test
    void leavesLinesAlone() {
        Geometry line = wkt("LINESTRING (0 0, 10 10, 10 0, 0 10, 0 0)");

        assertThat(repair.apply(line)).isSameAs(line);
    }

    @Test
    void repairsInSeveralThreads() throws InterruptedException, ExecutionException {
        List<Geometry> polygons = List.of(
                wkt(HOLE_ON_SHELL_ONCE_ROUNDED),
                wkt("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((10.4 0, 20 0, 20 10, 10.4 10, 10.4 0)))"),
                wkt("POLYGON((0 0, 10 0, 10 10, 5.6 10, 5.4 0.4, 5.2 10, 0 10, 0 0))"));
        List<Geometry> expected = polygons.stream().map(repair::apply).toList();
        Callable<List<Geometry>> repeatedRepairs = () -> differingRepairs(polygons, expected);

        List<Geometry> differing = new ArrayList<>();
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try {
            for (Future<List<Geometry>> result : threads.invokeAll(Collections.nCopies(8, repeatedRepairs))) {
                differing.addAll(result.get());
            }
        } finally {
            threads.shutdownNow();
        }

        assertThat(expected).doesNotContainNull();
        assertThat(differing).isEmpty();
    }

    @Test
    void keepsPolygonValidAtScale() {
        Geometry polygon = wkt(HOLE_NEAR_SHELL);

        assertThat(new PolygonRepairStep(16).apply(polygon)).isSameAs(polygon);
        assertThat(new PolygonRepairStep().apply(polygon)).isNotSameAs(polygon);
    }

    // Once multiplied by 16 and rounded, the corner of the hole is on the corner of the shell
    @Test
    void repairsOnScaledGrid() {
        Geometry polygon = wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.01 0.01, 0.01 2, 2 2, 2 0.01, 0.01 0.01))");

        Geometry repaired = new PolygonRepairStep(16).apply(polygon);

        assertThat(repaired).isInstanceOf(Polygonal.class);
        assertThat(new RoundedValidity(16).isValid((Polygonal) repaired)).isTrue();
        assertThat(repaired.getCoordinates()).allSatisfy(point -> {
            assertThat(point.x * 16).isEqualTo(Math.rint(point.x * 16));
            assertThat(point.y * 16).isEqualTo(Math.rint(point.y * 16));
        });
    }

    @ParameterizedTest
    @ValueSource(doubles = {0, -1, Double.NaN, Double.POSITIVE_INFINITY})
    void rejectsInvalidScale(double scale) {
        assertThatThrownBy(() -> new PolygonRepairStep(scale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The scale must be positive and finite: " + scale);
    }

    @Test
    void describesStep() {
        assertThat(repair).hasToString("PolygonRepairStep[scale=1.0]");
    }

    /** Repairs each polygon 200 times, and returns the results other than the expected result of the polygon. */
    private List<Geometry> differingRepairs(List<Geometry> polygons, List<Geometry> expected) {
        List<Geometry> differing = new ArrayList<>();
        for (int round = 0; round < 200; round++) {
            for (int i = 0; i < polygons.size(); i++) {
                Geometry repaired = repair.apply(polygons.get(i));
                if (repaired == null || !repaired.equalsExact(expected.get(i))) {
                    differing.add(repaired);
                }
            }
        }
        return differing;
    }
}
