/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

/**
 * The geometries of the features of a layer, decoded once when the tile is decoded. The geometry of a feature is a list
 * of parts: one part holding the points of a point feature, the lines of a line feature, or the rings of a polygon
 * feature. The parts of the layer follow one another in feature order, and their coordinates follow the same order.
 *
 * <p>Three arrays of exact size hold the geometries:
 *
 * <ul>
 *   <li>the absolute coordinates of the layer, x then y: 16-bit integers when each of them fits, 32-bit integers
 *       otherwise;
 *   <li>the end of each part, as the index of the coordinate after its last one, with the sign bit set on the rings
 *       starting a polygon;
 *   <li>the first part of each feature, and the number of parts of the layer after them.
 * </ul>
 *
 * <p>With 16-bit coordinates, a coordinate takes 4 bytes, a part 4 bytes and a feature 4 bytes. The arrays are written
 * before the instance is created, and only read afterwards: the tile can be shared between threads. A reader copies the
 * coordinates into geometries of its own, and callers may modify them.
 *
 * @see GeometryPacker
 */
final class PackedGeometries {

    /** The bit set in the end of a ring starting a polygon. A part ends below 2^31, and the sign bit is free. */
    static final int STARTS_POLYGON = Integer.MIN_VALUE;

    /**
     * The bytes of the fields below, for the heap size: three references (coordinates, partEnds, firstParts). A new
     * field must be counted here.
     */
    private static final int FIELD_BYTES = 3 * HeapSize.REFERENCE;

    private final Coordinates coordinates;
    private final int[] partEnds;
    private final int[] firstParts;

    /** Packs the geometries over 16-bit coordinates. The instance keeps the arrays: the caller must not modify them. */
    PackedGeometries(short[] xy, int[] partEnds, int[] firstParts) {
        this(new ShortCoordinates(xy), partEnds, firstParts);
    }

    /** Packs the geometries over 32-bit coordinates. The instance keeps the arrays: the caller must not modify them. */
    PackedGeometries(int[] xy, int[] partEnds, int[] firstParts) {
        this(new IntCoordinates(xy), partEnds, firstParts);
    }

    private PackedGeometries(Coordinates coordinates, int[] partEnds, int[] firstParts) {
        this.coordinates = coordinates;
        this.partEnds = partEnds;
        this.firstParts = firstParts;
    }

    int featureCount() {
        return firstParts.length - 1;
    }

    /** The index of the first part of the feature. A feature without parts has the first part of the next one. */
    int firstPart(int feature) {
        return firstParts[feature];
    }

    int partCount(int feature) {
        return firstParts[feature + 1] - firstParts[feature];
    }

    /** The index of the first coordinate of the part: the end of the part before it. */
    int partStart(int part) {
        return part == 0 ? 0 : partEnd(part - 1);
    }

    /** The index of the coordinate after the last one of the part. */
    int partEnd(int part) {
        return partEnds[part] & ~STARTS_POLYGON;
    }

    /**
     * Whether the part is a ring starting a polygon. The rings after it, up to the next one starting a polygon, are the
     * holes of the polygon.
     */
    boolean startsPolygon(int part) {
        return partEnds[part] < 0;
    }

    int x(int coordinate) {
        return coordinates.ordinate(2 * coordinate);
    }

    int y(int coordinate) {
        return coordinates.ordinate(2 * coordinate + 1);
    }

    /** Copies the coordinates from {@code start} inclusive to {@code end} exclusive into the target, x then y. */
    void copyCoordinates(int start, int end, double[] target) {
        coordinates.copy(2 * start, 2 * end, target);
    }

    /** Whether a coordinate of the layer needs more than 16 bits, and the layer stores its coordinates in 32 bits. */
    boolean hasIntCoordinates() {
        return coordinates instanceof IntCoordinates;
    }

    /**
     * Estimates the heap memory held by this instance, in bytes: the instance, its coordinates and its other arrays.
     */
    long heapSize() {
        return HeapSize.object(FIELD_BYTES)
                + coordinates.heapSize()
                + HeapSize.array(Integer.BYTES, partEnds.length)
                + HeapSize.array(Integer.BYTES, firstParts.length);
    }

    /** The absolute coordinates of a layer, x then y, as integers of one width. */
    private abstract static class Coordinates {

        abstract int ordinate(int index);

        /** Copies the ordinates from {@code start} inclusive to {@code end} exclusive to the start of the target. */
        abstract void copy(int start, int end, double[] target);

        /** The size of this object and of its array. */
        abstract long heapSize();
    }

    private static final class ShortCoordinates extends Coordinates {

        private final short[] ordinates;

        ShortCoordinates(short[] ordinates) {
            this.ordinates = ordinates;
        }

        @Override
        int ordinate(int index) {
            return ordinates[index];
        }

        @Override
        void copy(int start, int end, double[] target) {
            for (int i = start; i < end; i++) {
                target[i - start] = ordinates[i];
            }
        }

        @Override
        long heapSize() {
            return HeapSize.object(HeapSize.REFERENCE) + HeapSize.array(Short.BYTES, ordinates.length);
        }
    }

    private static final class IntCoordinates extends Coordinates {

        private final int[] ordinates;

        IntCoordinates(int[] ordinates) {
            this.ordinates = ordinates;
        }

        @Override
        int ordinate(int index) {
            return ordinates[index];
        }

        @Override
        void copy(int start, int end, double[] target) {
            for (int i = start; i < end; i++) {
                target[i - start] = ordinates[i];
            }
        }

        @Override
        long heapSize() {
            return HeapSize.object(HeapSize.REFERENCE) + HeapSize.array(Integer.BYTES, ordinates.length);
        }
    }
}
