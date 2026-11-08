/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import java.util.Arrays;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.operation.valid.IsValidOp;

/**
 * Tells whether a polygon is valid once its coordinates are rounded to integers, as a vector tile writer rounds them.
 * The answers are those of {@link IsValidOp} on a rounded copy of the polygon. For a polygon in coordinates multiplied
 * by a scale when they are rounded, the check multiplies them by the scale before rounding them, as the writer does.
 *
 * <p>{@code IsValidOp} reads the points of each ring as {@code Coordinate} objects with {@code getCoordinates()}, twice
 * or more for a valid polygon. An array sequence returns its own array. Other sequences create a {@code Coordinate} for
 * each point: a packed sequence at the first call, as it keeps the array, and a view over a part of a sequence at each
 * call. This class rounds the points into reused {@code Coordinate} objects, and builds the rounded copy over them with
 * array sequences. Once the reused objects are enough for the polygon, a check creates none for its points, and the
 * allocations left are those of {@code IsValidOp}, per ring and per monotone chain.
 *
 * <p>An instance keeps one {@code Coordinate} for each point of the largest polygon checked. It is not thread-safe.
 */
public final class RoundedValidity {

    /** Wraps the given arrays in array sequences, without copying: the rounded rings read the reused points. */
    private static final GeometryFactory ARRAY_FACTORY = new GeometryFactory(CoordinateArraySequenceFactory.instance());

    /** The rounded points of the polygon under check, ring after ring, followed by those left from larger polygons. */
    private Coordinate[] points;

    /** The number of points of the polygon under check rounded so far. */
    private int roundedCount;

    /** The number of tile units in one unit of the polygons. */
    private final double scale;

    /**
     * Creates a check of polygons in tile coordinates, without coordinates to reuse: a check creates those missing for
     * its polygon.
     */
    public RoundedValidity() {
        this(1);
    }

    /**
     * Creates a check of polygons in coordinates multiplied by the scale when they are rounded, without coordinates to
     * reuse.
     *
     * @param scale the number of tile units in one unit of the polygons
     * @throws IllegalArgumentException if the scale is not positive and finite
     */
    public RoundedValidity(double scale) {
        this.scale = Parameters.requirePositiveAndFinite(scale, "scale");
        points = new Coordinate[0];
    }

    /**
     * Tells whether the polygon or the multipolygon is valid once each coordinate is multiplied by the scale, then
     * rounded with {@link Math#round(double)}.
     *
     * @param polygonal a {@code Polygon} or a {@code MultiPolygon}
     * @return the answer of {@link IsValidOp} on the rounded copy
     */
    public boolean isValid(Polygonal polygonal) {
        Geometry geometry = (Geometry) polygonal;
        growPoints(geometry.getNumPoints());
        roundedCount = 0;
        Geometry rounded = roundedCopy(geometry);
        return IsValidOp.isValid(rounded);
    }

    /** Adds coordinates up to the point count, and keeps those created by earlier checks. */
    private void growPoints(int pointCount) {
        if (points.length >= pointCount) {
            return;
        }
        int reusedCount = points.length;
        points = Arrays.copyOf(points, pointCount);
        for (int i = reusedCount; i < pointCount; i++) {
            points[i] = new Coordinate();
        }
    }

    private Geometry roundedCopy(Geometry polygonal) {
        if (polygonal instanceof Polygon polygon) {
            return roundedPolygon(polygon);
        }
        return roundedMultiPolygon((MultiPolygon) polygonal);
    }

    private MultiPolygon roundedMultiPolygon(MultiPolygon multiPolygon) {
        Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
        for (int i = 0; i < polygons.length; i++) {
            polygons[i] = roundedPolygon((Polygon) multiPolygon.getGeometryN(i));
        }
        return ARRAY_FACTORY.createMultiPolygon(polygons);
    }

    private Polygon roundedPolygon(Polygon polygon) {
        LinearRing shell = roundedRing(polygon.getExteriorRing());
        LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = roundedRing(polygon.getInteriorRingN(i));
        }
        return ARRAY_FACTORY.createPolygon(shell, holes);
    }

    /**
     * Returns the ring rounded into the next reused coordinates. The rounded ring keeps the point count of the ring and
     * stays closed: its construction cannot fail for a ring accepted by JTS.
     */
    private LinearRing roundedRing(LinearRing ring) {
        CoordinateSequence sequence = ring.getCoordinateSequence();
        int start = roundedCount;
        roundPoints(sequence, start);
        roundedCount = start + sequence.size();
        Coordinate[] ringPoints = Arrays.copyOfRange(points, start, roundedCount);
        return ARRAY_FACTORY.createLinearRing(ringPoints);
    }

    private void roundPoints(CoordinateSequence sequence, int start) {
        int size = sequence.size();
        for (int i = 0; i < size; i++) {
            Coordinate point = points[start + i];
            point.setX((double) Math.round(sequence.getOrdinate(i, CoordinateSequence.X) * scale));
            point.setY((double) Math.round(sequence.getOrdinate(i, CoordinateSequence.Y) * scale));
        }
    }
}
