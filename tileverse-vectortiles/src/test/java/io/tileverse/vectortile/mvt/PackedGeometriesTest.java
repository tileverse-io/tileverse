/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static io.tileverse.vectortile.mvt.PackedGeometries.STARTS_POLYGON;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PackedGeometriesTest {

    /**
     * Three features: a line of 2 points, a feature without parts, and a polygon of two rings of 4 points. The second
     * ring starts a polygon.
     */
    private static final int[] PART_ENDS = {2, 6 | STARTS_POLYGON, 10 | STARTS_POLYGON};

    private static final int[] FIRST_PARTS = {0, 1, 1, 3};

    private static final short[] SHORT_XY = {0, 0, 10, 10, 0, 0, 5, 0, 5, 5, 0, 0, 20, 20, 25, 20, 25, 25, 20, 20};

    @Test
    void locatesPartsOfFeatures() {
        PackedGeometries geometries = new PackedGeometries(SHORT_XY, PART_ENDS, FIRST_PARTS);

        assertThat(geometries.featureCount()).isEqualTo(3);
        assertThat(geometries.firstPart(0)).isZero();
        assertThat(geometries.partCount(0)).isEqualTo(1);
        assertThat(geometries.partCount(1)).isZero();
        assertThat(geometries.firstPart(2)).isEqualTo(1);
        assertThat(geometries.partCount(2)).isEqualTo(2);
        assertThat(geometries.partStart(0)).isZero();
        assertThat(geometries.partEnd(0)).isEqualTo(2);
        assertThat(geometries.partStart(2)).isEqualTo(6);
        assertThat(geometries.partEnd(2)).isEqualTo(10);
    }

    @Test
    void flagsPartsStartingPolygons() {
        PackedGeometries geometries = new PackedGeometries(SHORT_XY, PART_ENDS, FIRST_PARTS);

        assertThat(geometries.startsPolygon(0)).isFalse();
        assertThat(geometries.startsPolygon(1)).isTrue();
        assertThat(geometries.startsPolygon(2)).isTrue();
    }

    @Test
    void readsShortCoordinates() {
        PackedGeometries geometries = new PackedGeometries(SHORT_XY, PART_ENDS, FIRST_PARTS);
        double[] ring = new double[8];

        geometries.copyCoordinates(6, 10, ring);

        assertThat(ring).containsExactly(20, 20, 25, 20, 25, 25, 20, 20);
        assertThat(geometries.x(1)).isEqualTo(10);
        assertThat(geometries.y(4)).isEqualTo(5);
        assertThat(geometries.hasIntCoordinates()).isFalse();
    }

    @Test
    void readsIntCoordinates() {
        int[] xy = {-40000, 5, 40000, 70000};
        PackedGeometries geometries = new PackedGeometries(xy, new int[] {2}, new int[] {0, 1});
        double[] points = new double[4];

        geometries.copyCoordinates(0, 2, points);

        assertThat(points).containsExactly(-40000, 5, 40000, 70000);
        assertThat(geometries.x(1)).isEqualTo(40000);
        assertThat(geometries.y(1)).isEqualTo(70000);
        assertThat(geometries.hasIntCoordinates()).isTrue();
    }

    // The object, its coordinates object, and the three arrays
    @Test
    void sumsHeapSizeOfArrays() {
        PackedGeometries shorts = new PackedGeometries(SHORT_XY, PART_ENDS, FIRST_PARTS);
        PackedGeometries ints = new PackedGeometries(new int[20], PART_ENDS, FIRST_PARTS);

        long common = HeapSize.object(3 * HeapSize.REFERENCE)
                + HeapSize.object(HeapSize.REFERENCE)
                + HeapSize.array(Integer.BYTES, 3)
                + HeapSize.array(Integer.BYTES, 4);
        assertThat(shorts.heapSize()).isEqualTo(common + HeapSize.array(Short.BYTES, 20));
        assertThat(ints.heapSize()).isEqualTo(common + HeapSize.array(Integer.BYTES, 20));
    }
}
