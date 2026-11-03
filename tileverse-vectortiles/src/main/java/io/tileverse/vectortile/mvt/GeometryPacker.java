/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static io.tileverse.vectortile.mvt.PackedGeometries.STARTS_POLYGON;

import java.util.Arrays;

/**
 * Decodes the geometry commands of the features of a layer into {@link PackedGeometries}, one feature at a time.
 * Decoding a tile runs one packer over its layers, and the packer reuses its buffers from one layer to the next.
 *
 * <p>The packer reads the commands with the rules of earlier releases, except for the rings of zero area:
 *
 * <ul>
 *   <li>A geometry starts with MoveTo. Each MoveTo starts a part with its points. LineTo adds its points to the part.
 *       ClosePath adds the last point of the last MoveTo again: one point, whatever the count of the ClosePath.
 *   <li>The points of a point feature form one part. A line feature keeps its parts of 2 points or more. A polygon
 *       feature keeps its rings of 3 points or more, except the rings of zero area: the MVT specification says that
 *       such a ring signals anomalous points. A feature of unknown type keeps no part.
 *   <li>The first ring kept in a polygon feature starts a polygon. A later ring wound as the first one starts another
 *       polygon, and the other rings are holes of the polygon before them. The winding comes from the sign of the area
 *       of the ring.
 * </ul>
 *
 * <p>A geometry is malformed when it starts with a command other than MoveTo, when it has an unknown command, when a
 * command announces more points than the parameters left, or when a ring of 3 points or more does not end on its first
 * point, whatever its area. The packer then keeps no part of the feature, and throws a
 * {@link MalformedGeometryException}. No buffer is sized from a command count: the coordinate buffer grows with the
 * bytes of the commands, and the part buffer with the parts read.
 */
final class GeometryPacker {

    // A smaller part is skipped
    private static final int MIN_LINE_POINTS = 2;
    private static final int MIN_RING_POINTS = 3;

    /** The start of the current part before the first MoveTo of a geometry. */
    private static final int NO_PART = -1;

    private static final int MIN_BUFFER_COORDINATES = 32;
    private static final int MIN_BUFFER_PARTS = 64;

    private final VarintCursor commands = new VarintCursor(new byte[0], 0, 0);

    // The coordinates of the layer, x then y, in buffers longer than needed: 16-bit values while each coordinate fits,
    // and 32-bit values from the first coordinate needing more, for the rest of the layer. The buffers have an even
    // length.
    private short[] narrowXy;
    private int[] wideXy;
    private int coordinateCount;

    // The ends of the parts of the layer, in a buffer longer than needed
    private int[] partEnds;
    private int partCount;

    // The first part of each feature of the layer, then the number of parts of the layer
    private int[] firstParts;
    private int featuresPacked;

    // The geometry being packed
    private GeomType type;
    private int cursorX;
    private int cursorY;
    private int moveToX;
    private int moveToY;
    private int partStart;
    private int geometryStart;
    private int geometryFirstPart;
    private boolean firstRingClockwise;
    private boolean openRing;

    /**
     * Creates a packer with small buffers. The buffers grow when a layer holds more. The length of a tile tells little
     * about its geometry: the attributes take most of the bytes of some tiles.
     */
    GeometryPacker() {
        narrowXy = new short[2 * MIN_BUFFER_COORDINATES];
        partEnds = new int[MIN_BUFFER_PARTS];
    }

    /** Starts a layer of {@code featureCount} features. Each of them must then be packed, in layer order. */
    void startLayer(int featureCount) {
        firstParts = new int[featureCount + 1];
        featuresPacked = 0;
        coordinateCount = 0;
        partCount = 0;
    }

    /**
     * Packs the geometry of the next feature of the layer, from its geometry commands between {@code start} inclusive
     * and {@code end} exclusive. The range must end on a complete varint, as {@link VarintCursor} requires.
     *
     * @throws MalformedGeometryException if the geometry is malformed: the feature is packed without parts, and the
     *     next feature can be packed
     */
    void pack(GeomType type, byte[] bytes, int start, int end) throws MalformedGeometryException {
        commands.reset(bytes, start, end);
        requireCoordinateCapacity(end - start);
        startGeometry(type);
        try {
            packCommands();
        } catch (MalformedGeometryException e) {
            discardGeometry();
            throw e;
        } finally {
            featuresPacked++;
            firstParts[featuresPacked] = partCount;
        }
    }

    // Each coordinate takes one byte of the commands at least: the varint of a ClosePath, or the two parameters of a
    // point
    private void requireCoordinateCapacity(int commandBytes) {
        int ordinates = 2 * (coordinateCount + commandBytes);
        if (wideXy != null) {
            if (ordinates > wideXy.length) {
                wideXy = Arrays.copyOf(wideXy, Math.max(ordinates, 2 * wideXy.length));
            }
        } else if (ordinates > narrowXy.length) {
            narrowXy = Arrays.copyOf(narrowXy, Math.max(ordinates, 2 * narrowXy.length));
        }
    }

    private void startGeometry(GeomType geometryType) {
        type = geometryType;
        cursorX = 0;
        cursorY = 0;
        moveToX = 0;
        moveToY = 0;
        partStart = NO_PART;
        geometryStart = coordinateCount;
        geometryFirstPart = partCount;
        openRing = false;
    }

    private void packCommands() throws MalformedGeometryException {
        while (commands.hasNext()) {
            int command = commands.next();
            int id = GeometryCommand.extractCommand(command);
            int count = GeometryCommand.extractCount(command);
            if (id == GeometryCommand.MoveTo) {
                endPart();
                moveTo(count);
            } else if (partStart == NO_PART) {
                throw new MalformedGeometryException("the geometry starts with " + name(id) + " instead of MoveTo");
            } else if (id == GeometryCommand.LineTo) {
                addPoints(GeometryCommand.LineTo, count);
            } else if (id == GeometryCommand.ClosePath) {
                addCoordinate(moveToX, moveToY);
            } else {
                throw new MalformedGeometryException("the geometry has the unknown command " + id);
            }
        }
        endPart();
        endGeometry();
    }

    private static String name(int command) {
        return switch (command) {
            case GeometryCommand.LineTo -> "LineTo";
            case GeometryCommand.ClosePath -> "ClosePath";
            default -> "the unknown command " + command;
        };
    }

    private void moveTo(int count) throws MalformedGeometryException {
        partStart = coordinateCount;
        addPoints(GeometryCommand.MoveTo, count);
        if (count > 0) {
            moveToX = cursorX;
            moveToY = cursorY;
        }
    }

    /**
     * Adds the points of a MoveTo or a LineTo, and moves the cursor to the last one. Each parameter is the zigzag
     * encoding of a delta from the cursor.
     */
    private void addPoints(int command, int count) throws MalformedGeometryException {
        // The cursor stays in local variables while the points are read: the JIT keeps them in registers
        int x = cursorX;
        int y = cursorY;
        for (int k = 0; k < count; k++) {
            x += decodeZigZag(nextParameter(command, count));
            y += decodeZigZag(nextParameter(command, count));
            addCoordinate(x, y);
        }
        cursorX = x;
        cursorY = y;
    }

    // A command reads its points while parameters are left: a malformed count stops at the end of the commands
    private int nextParameter(int command, int count) throws MalformedGeometryException {
        if (!commands.hasNext()) {
            String name = command == GeometryCommand.MoveTo ? "MoveTo" : "LineTo";
            throw new MalformedGeometryException(
                    "a " + name + " announces " + count + " points, more than the parameters left");
        }
        return commands.next();
    }

    /** Decodes a parameter of a MoveTo or a LineTo: the zigzag encoding of a delta. */
    static int decodeZigZag(int encoded) {
        return (encoded >>> 1) ^ -(encoded & 1);
    }

    // The buffer has room for the coordinates of the geometry, from requireCoordinateCapacity
    private void addCoordinate(int x, int y) {
        int index = 2 * coordinateCount;
        if (wideXy == null && x == (short) x && y == (short) y) {
            narrowXy[index] = (short) x;
            narrowXy[index + 1] = (short) y;
        } else {
            addWideCoordinate(index, x, y);
        }
        coordinateCount++;
    }

    /** Adds a coordinate in 32 bits. The first one needing more than 16 bits moves the layer to 32-bit values. */
    private void addWideCoordinate(int index, int x, int y) {
        if (wideXy == null) {
            wideXy = new int[narrowXy.length];
            for (int i = 0; i < index; i++) {
                wideXy[i] = narrowXy[i];
            }
        }
        wideXy[index] = x;
        wideXy[index + 1] = y;
    }

    /**
     * Keeps the part ended by a MoveTo or by the end of the commands, or skips it. The points of a point feature stay
     * in the buffer, for the part of {@link #endGeometry}.
     */
    private void endPart() {
        if (partStart == NO_PART || type == GeomType.POINT) {
            return;
        }
        int length = coordinateCount - partStart;
        if (type == GeomType.LINESTRING && length >= MIN_LINE_POINTS) {
            addPart(false);
        } else if (type == GeomType.POLYGON && length >= MIN_RING_POINTS) {
            endRing(length);
        } else {
            skipPart();
        }
    }

    /**
     * Keeps a closed ring with an area, and skips a closed ring of zero area. A ring that does not end on its first
     * point makes the geometry malformed, whatever its area. An open ring is reported once the commands are read, as
     * JTS rejected it after the commands in earlier releases.
     */
    private void endRing(int length) {
        if (!isClosed(partStart, coordinateCount - 1)) {
            openRing = true;
            skipPart();
            return;
        }
        double area = signedArea(partStart, length);
        boolean clockwise = area > 0;
        boolean counterclockwise = area < 0;
        if (clockwise || counterclockwise) {
            addRing(clockwise);
        } else {
            skipPart();
        }
    }

    private void skipPart() {
        coordinateCount = partStart;
    }

    private void addRing(boolean clockwise) {
        boolean firstRing = partCount == geometryFirstPart;
        if (firstRing) {
            firstRingClockwise = clockwise;
        }
        addPart(firstRing || clockwise == firstRingClockwise);
    }

    private boolean isClosed(int first, int last) {
        return ordinate(2 * first) == ordinate(2 * last) && ordinate(2 * first + 1) == ordinate(2 * last + 1);
    }

    /**
     * The signed area of a ring: the Shoelace formula, as in {@code org.locationtech.jts.algorithm.Area#ofRingSigned},
     * over the coordinates in the buffer. The area is positive for a ring wound clockwise, and negative for a ring
     * wound counterclockwise.
     *
     * <p>This code is copied under JTS EDL 1.0 license (BSD-3)
     */
    private double signedArea(int start, int length) {
        double p0Y;
        double p1x = ordinate(2 * start);
        double p1y = ordinate(2 * start + 1);
        double p2x = ordinate(2 * start + 2);
        double p2y = ordinate(2 * start + 3);

        double x0 = p1x;
        p2x -= x0;
        double sum = 0.0;
        for (int i = 1; i < length - 1; i++) {
            p0Y = p1y;
            p1x = p2x;
            p1y = p2y;
            p2x = ordinate(2 * (start + i + 1));
            p2y = ordinate(2 * (start + i + 1) + 1);
            p2x -= x0;
            sum += p1x * (p0Y - p2y);
        }
        return sum / 2.0;
    }

    private int ordinate(int index) {
        return wideXy == null ? narrowXy[index] : wideXy[index];
    }

    private void addPart(boolean startsPolygon) {
        if (partCount == partEnds.length) {
            partEnds = Arrays.copyOf(partEnds, 2 * partEnds.length);
        }
        partEnds[partCount] = startsPolygon ? coordinateCount | STARTS_POLYGON : coordinateCount;
        partCount++;
    }

    private void endGeometry() throws MalformedGeometryException {
        if (openRing) {
            throw new MalformedGeometryException("a polygon ring does not end on its first point");
        }
        if (type == GeomType.POINT && coordinateCount > geometryStart) {
            addPart(false);
        }
    }

    private void discardGeometry() {
        coordinateCount = geometryStart;
        partCount = geometryFirstPart;
    }

    /**
     * Returns the packed geometries of the layer, in arrays of exact size. The coordinates take 16 bits when each of
     * them fits, and 32 bits otherwise. The next layer starts with 16-bit coordinates.
     */
    PackedGeometries finishLayer() {
        int ordinates = 2 * coordinateCount;
        int[] exactPartEnds = Arrays.copyOf(partEnds, partCount);
        PackedGeometries layer = wideXy == null
                ? new PackedGeometries(Arrays.copyOf(narrowXy, ordinates), exactPartEnds, firstParts)
                : packWide(ordinates, exactPartEnds);
        wideXy = null;
        return layer;
    }

    // The coordinates needing more than 16 bits may belong to skipped parts and malformed geometries alone
    private PackedGeometries packWide(int ordinates, int[] exactPartEnds) {
        if (fitInShorts(wideXy, ordinates)) {
            return new PackedGeometries(toShorts(wideXy, ordinates), exactPartEnds, firstParts);
        }
        return new PackedGeometries(Arrays.copyOf(wideXy, ordinates), exactPartEnds, firstParts);
    }

    private static boolean fitInShorts(int[] values, int length) {
        for (int i = 0; i < length; i++) {
            if (values[i] != (short) values[i]) {
                return false;
            }
        }
        return true;
    }

    private static short[] toShorts(int[] values, int length) {
        short[] shorts = new short[length];
        for (int i = 0; i < length; i++) {
            shorts[i] = (short) values[i];
        }
        return shorts;
    }

    /** A malformed geometry, packed without parts. The message names the problem. */
    @SuppressWarnings("serial")
    static final class MalformedGeometryException extends Exception {

        // Decoding a damaged tile may throw many of these, and a stack trace would only show the packer
        MalformedGeometryException(String problem) {
            super(problem, null, false, false);
        }
    }
}
