/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.math.BigInteger;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

/**
 * Writes the geometry commands of a feature: MoveTo, LineTo and ClosePath, with the points as zigzag-encoded deltas
 * from a cursor. The cursor starts at 0,0 for each geometry.
 *
 * <p>The commands follow the specification, on coordinates multiplied by the scale of the geometry, then rounded with
 * {@link Math#round(double)}:
 *
 * <ul>
 *   <li>The points of a {@code Point} or a {@code MultiPoint} are one MoveTo, repeated points included.
 *   <li>Points equal to the cursor are left out of a LineTo. A line collapsed to its first point is dropped. A
 *       {@code LinearRing} is written as a line, with its closing point.
 *   <li>The rings of a polygon are written without their closing point and without trailing points equal to their first
 *       point.
 *   <li>An exterior ring is written with a positive area by the surveyor's formula, and a hole with a negative one. A
 *       ring with the wrong sign is walked backwards from its first point.
 *   <li>A ring of zero area is dropped, with the holes of a dropped exterior ring.
 * </ul>
 *
 * <p>An instance keeps its arrays from one geometry to the next. It is not thread-safe.
 *
 * @see <a href="https://github.com/mapbox/vector-tile-spec/tree/master/2.1#43-geometry-encoding">Geometry encoding of
 *     the vector tile specification 2.1</a>
 */
final class CommandEncoder {

    private static final int MOVE_TO_ONE_POINT = GeometryCommand.packCommandAndLength(GeometryCommand.MoveTo, 1);

    private static final int CLOSE_PATH = GeometryCommand.packCommandAndLength(GeometryCommand.ClosePath, 1);

    private final IntArray commands = new IntArray();

    private final Cursor cursor = new Cursor();

    private GeomType type = GeomType.UNKNOWN;

    /** The number of tile units in one unit of the geometry being written. */
    private double scale = 1;

    /**
     * Writes the commands of a geometry in tile coordinates, as {@link #encode(Geometry, double)} does at scale 1.
     *
     * @return whether commands were written
     * @throws IllegalArgumentException as {@link #encode(Geometry, double)} does
     */
    boolean encode(Geometry geometry) {
        return encode(geometry, 1);
    }

    /**
     * Writes the commands of the geometry, in place of those of the previous call. Each ordinate is multiplied by the
     * scale, then rounded.
     *
     * @param geometry the geometry to write
     * @param scale the number of tile units in one unit of the geometry
     * @return whether commands were written: false for an empty geometry, and for a geometry with no part left once
     *     rounded
     * @throws IllegalArgumentException for a geometry other than points, lines and polygons, such as a
     *     {@code GeometryCollection}, for an ordinate that is NaN or rounds outside the range of an int once multiplied
     *     by the scale, and for a point more than 2^31 - 1 tile units from the cursor on one axis
     */
    boolean encode(Geometry geometry, double scale) {
        this.scale = scale;
        commands.clear();
        cursor.moveTo(0, 0);
        if (geometry instanceof Puntal) {
            type = GeomType.POINT;
            writePoints(geometry);
        } else if (geometry instanceof Lineal) {
            type = GeomType.LINESTRING;
            writeLines(geometry);
        } else if (geometry instanceof Polygonal) {
            type = GeomType.POLYGON;
            writePolygons(geometry);
        } else {
            throw new IllegalArgumentException(
                    "Cannot write a " + geometry.getGeometryType() + ": a feature holds points, lines or polygons");
        }
        return !commands.isEmpty();
    }

    /** The type of the last geometry: {@code POINT}, {@code LINESTRING} or {@code POLYGON}. */
    GeomType type() {
        return type;
    }

    /** The commands of the last geometry. The next call to {@link #encode} replaces them. */
    IntArray commands() {
        return commands;
    }

    /**
     * One MoveTo with the points, repeated points included: the rule against a zero delta applies to LineTo only.
     * Nothing is written for an empty geometry: the count of a MoveTo must be above 0.
     */
    private void writePoints(Geometry puntal) {
        if (puntal.isEmpty()) {
            return;
        }
        int moveTo = addPlaceholder();
        for (int i = 0; i < puntal.getNumGeometries(); i++) {
            Point point = (Point) puntal.getGeometryN(i);
            if (!point.isEmpty()) {
                addPoint(point.getCoordinateSequence(), 0);
            }
        }
        completeCommand(moveTo, GeometryCommand.MoveTo);
    }

    private void writeLines(Geometry lineal) {
        for (int i = 0; i < lineal.getNumGeometries(); i++) {
            LineString line = (LineString) lineal.getGeometryN(i);
            if (!line.isEmpty()) {
                writeLine(line.getCoordinateSequence());
            }
        }
    }

    /**
     * MoveTo the first point, then LineTo the points after it. A line collapsed to its first point once rounded is
     * removed, and the cursor goes back to its position before the line: a LineTo needs a point.
     */
    private void writeLine(CoordinateSequence line) {
        int start = commands.size();
        int startX = cursor.x;
        int startY = cursor.y;
        addMoveTo(line, 0);
        int lineTo = addPlaceholder();
        for (int i = 1; i < line.size(); i++) {
            addPointUnlessRepeated(line, i);
        }
        if (pointCountAfter(lineTo) > 0) {
            completeCommand(lineTo, GeometryCommand.LineTo);
        } else {
            commands.truncate(start);
            cursor.moveTo(startX, startY);
        }
    }

    private void writePolygons(Geometry polygonal) {
        for (int i = 0; i < polygonal.getNumGeometries(); i++) {
            writePolygon((Polygon) polygonal.getGeometryN(i));
        }
    }

    /**
     * The exterior ring with a positive surveyor's sum, then the holes with a negative one. A ring of zero area once
     * rounded is left out. When the exterior ring is left out, its holes are too. The exterior ring of an empty polygon
     * has no area.
     */
    private void writePolygon(Polygon polygon) {
        CoordinateSequence exterior = polygon.getExteriorRing().getCoordinateSequence();
        int exteriorSign = areaSign(exterior);
        if (exteriorSign == 0) {
            return;
        }
        writeRing(exterior, exteriorSign < 0);
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            CoordinateSequence hole = polygon.getInteriorRingN(i).getCoordinateSequence();
            writeHole(hole);
        }
    }

    private void writeHole(CoordinateSequence hole) {
        int sign = areaSign(hole);
        if (sign != 0) {
            writeRing(hole, sign > 0);
        }
    }

    /**
     * The sign of the surveyor's sum over the rounded points of the ring: the sign of its area. The sum of a ring
     * spanning most of the range of an int can exceed a long, and such a ring is summed again with a BigInteger.
     */
    private int areaSign(CoordinateSequence ring) {
        try {
            return Long.signum(surveyorSum(ring));
        } catch (ArithmeticException e) {
            return exactSurveyorSum(ring).signum();
        }
    }

    /**
     * The sum of the surveyor's formula over the rounded points of the ring: twice its signed area.
     *
     * @throws ArithmeticException if the sum overflows a long
     */
    private long surveyorSum(CoordinateSequence ring) {
        if (ring.size() == 0) {
            return 0;
        }
        int previousX = roundedX(ring, 0);
        int previousY = roundedY(ring, 0);
        long sum = 0;
        for (int i = 1; i < ring.size(); i++) {
            int x = roundedX(ring, i);
            int y = roundedY(ring, i);
            sum = Math.addExact(sum, edgeTerm(previousX, previousY, x, y));
            previousX = x;
            previousY = y;
        }
        return sum;
    }

    /** The sum of {@link #surveyorSum} in a BigInteger, for a ring with a sum beyond the range of a long. */
    private BigInteger exactSurveyorSum(CoordinateSequence ring) {
        int previousX = roundedX(ring, 0);
        int previousY = roundedY(ring, 0);
        BigInteger sum = BigInteger.ZERO;
        for (int i = 1; i < ring.size(); i++) {
            int x = roundedX(ring, i);
            int y = roundedY(ring, i);
            long term = edgeTerm(previousX, previousY, x, y);
            sum = sum.add(BigInteger.valueOf(term));
            previousX = x;
            previousY = y;
        }
        return sum;
    }

    /**
     * The term of the surveyor's formula for the edge from one point to the next. The products of two ints need a long,
     * and their difference fits in one.
     */
    private static long edgeTerm(int x1, int y1, int x2, int y2) {
        return (long) x1 * y2 - (long) x2 * y1;
    }

    /**
     * MoveTo the first point, LineTo the points before the closing one, then ClosePath. The trailing points equal to
     * the first point are left out: the specification forbids the cursor to sit on the first point before ClosePath. A
     * ring of non-zero area has points off the first one, and the walk ends at the last of them.
     *
     * @param backwards whether to walk the points in reverse from the first one, to flip the sign of the surveyor's sum
     */
    private void writeRing(CoordinateSequence ring, boolean backwards) {
        addMoveTo(ring, 0);
        int lineTo = addPlaceholder();
        if (backwards) {
            addPointsBackwards(ring);
        } else {
            addPointsForwards(ring);
        }
        completeCommand(lineTo, GeometryCommand.LineTo);
        commands.add(CLOSE_PATH);
    }

    /**
     * LineTo the points after the first one, up to the last point off the cursor. The cursor sits on the first point.
     */
    private void addPointsForwards(CoordinateSequence ring) {
        int last = ring.size() - 2;
        while (roundsToCursor(ring, last)) {
            last--;
        }
        for (int i = 1; i <= last; i++) {
            addPointUnlessRepeated(ring, i);
        }
    }

    /**
     * LineTo the points before the closing one, in reverse, up to the last point off the cursor. The cursor sits on the
     * first point.
     */
    private void addPointsBackwards(CoordinateSequence ring) {
        int last = 1;
        while (roundsToCursor(ring, last)) {
            last++;
        }
        for (int i = ring.size() - 2; i >= last; i--) {
            addPointUnlessRepeated(ring, i);
        }
    }

    /** MoveTo(1) the point at the index: the start of a line or a ring. */
    private void addMoveTo(CoordinateSequence sequence, int index) {
        commands.add(MOVE_TO_ONE_POINT);
        addPoint(sequence, index);
    }

    /**
     * Adds the point at the index unless it rounds to the cursor: the specification forbids a zero delta in a LineTo.
     */
    private void addPointUnlessRepeated(CoordinateSequence sequence, int index) {
        if (!roundsToCursor(sequence, index)) {
            addNextPoint();
        }
    }

    private void addPoint(CoordinateSequence sequence, int index) {
        cursor.read(sequence, index);
        addNextPoint();
    }

    /** Reads the point at the index as the next point of the cursor, and tells whether it rounds to the cursor. */
    private boolean roundsToCursor(CoordinateSequence sequence, int index) {
        cursor.read(sequence, index);
        return cursor.isOnNextPoint();
    }

    /** Adds the delta from the cursor to the next point, and moves the cursor there. */
    private void addNextPoint() {
        commands.add(zigZag(delta(cursor.x, cursor.nextX)));
        commands.add(zigZag(delta(cursor.y, cursor.nextY)));
        cursor.moveTo(cursor.nextX, cursor.nextY);
    }

    /**
     * The difference between two rounded ordinates, written as a parameter. The specification supports parameters from
     * -(2^31 - 1) to 2^31 - 1, and two ints can differ by more.
     *
     * @throws IllegalArgumentException if the difference is outside the range of a parameter
     */
    private static int delta(int from, int to) {
        long delta = (long) to - from;
        if (delta < -Integer.MAX_VALUE || delta > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot write the delta " + delta + " from " + from + " to " + to
                    + ": the specification supports parameters from -(2^31 - 1) to 2^31 - 1");
        }
        return (int) delta;
    }

    /** Adds a placeholder for a command of unknown count, and returns its index. */
    private int addPlaceholder() {
        int index = commands.size();
        commands.add(0);
        return index;
    }

    /** Sets the command at the index, with the number of points added after it as its count. */
    private void completeCommand(int index, int command) {
        int count = pointCountAfter(index);
        commands.set(index, GeometryCommand.packCommandAndLength(command, count));
    }

    /** The number of points added after the command at the index: two parameters each. */
    private int pointCountAfter(int index) {
        return (commands.size() - index - 1) / 2;
    }

    private int roundedX(CoordinateSequence sequence, int index) {
        return round(sequence.getOrdinate(index, CoordinateSequence.X));
    }

    private int roundedY(CoordinateSequence sequence, int index) {
        return round(sequence.getOrdinate(index, CoordinateSequence.Y));
    }

    /**
     * Multiplies an ordinate by the scale of the geometry, and rounds the product to the grid of the tile.
     *
     * @throws IllegalArgumentException if the product is NaN or rounds outside the range of an int
     */
    private int round(double ordinate) {
        double scaled = ordinate * scale;
        long rounded = Math.round(scaled);
        if (Double.isNaN(scaled) || rounded < Integer.MIN_VALUE || rounded > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(roundingFailure(ordinate));
        }
        return (int) rounded;
    }

    /** The message of an ordinate that cannot be rounded: the ordinate as given, and the scale when it is not 1. */
    private String roundingFailure(double ordinate) {
        if (Double.compare(scale, 1) == 0) {
            return "Cannot round the ordinate " + ordinate + " to an int";
        }
        return "Cannot round the ordinate " + ordinate + " multiplied by the scale " + scale + " to an int";
    }

    // A parameter holds the zigzag encoding of a delta: deltas of either sign near zero take few bytes
    private static int zigZag(int delta) {
        return (delta << 1) ^ (delta >> 31);
    }

    /**
     * The cursor of the specification, on the last point written, with the next point once rounded: a point is compared
     * with the cursor before it is written. The cursor rounds the points at the scale of the geometry being written.
     */
    private final class Cursor {
        private int x;
        private int y;
        private int nextX;
        private int nextY;

        void moveTo(int x, int y) {
            this.x = x;
            this.y = y;
        }

        void read(CoordinateSequence sequence, int index) {
            nextX = roundedX(sequence, index);
            nextY = roundedY(sequence, index);
        }

        boolean isOnNextPoint() {
            return nextX == x && nextY == y;
        }
    }
}
