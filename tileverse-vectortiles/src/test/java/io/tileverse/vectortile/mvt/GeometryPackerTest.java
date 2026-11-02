/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.tileverse.vectortile.mvt.GeometryPacker.MalformedGeometryException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class GeometryPackerTest {

    private final GeometryPacker packer = new GeometryPacker();

    @Test
    void packsLineOfFeature() throws MalformedGeometryException {
        PackedGeometries line = packOne(GeomType.LINESTRING, moveTo(1), deltas(2, 3), lineTo(2), deltas(1, 1, 1, 1));

        assertThat(line.partCount(0)).isEqualTo(1);
        assertThat(part(line, 0)).isEqualTo("2 3, 3 4, 4 5");
        assertThat(line.startsPolygon(0)).isFalse();
    }

    @Test
    void restartsCursorAtEachFeature() throws MalformedGeometryException {
        packer.startLayer(2);
        pack(GeomType.LINESTRING, moveTo(1), deltas(2, 3), lineTo(1), deltas(1, 1));
        pack(GeomType.LINESTRING, moveTo(1), deltas(2, 3), lineTo(1), deltas(1, 1));
        PackedGeometries lines = packer.finishLayer();

        assertThat(lines.firstPart(1)).isEqualTo(1);
        assertThat(part(lines, 0)).isEqualTo("2 3, 3 4");
        assertThat(part(lines, 1)).isEqualTo("2 3, 3 4");
    }

    // The points of the three MoveTo commands form one part
    @Test
    void packsPointsAsOnePart() throws MalformedGeometryException {
        PackedGeometries points =
                packOne(GeomType.POINT, moveTo(2), deltas(5, 7, -2, -5), moveTo(1), deltas(1, 1), moveTo(0));

        assertThat(points.partCount(0)).isEqualTo(1);
        assertThat(part(points, 0)).isEqualTo("5 7, 3 2, 4 3");
    }

    // The count of a ClosePath is ignored: it adds one point
    @Test
    void closePathRepeatsMoveTo() throws MalformedGeometryException {
        PackedGeometries polygon =
                packOne(GeomType.POLYGON, moveTo(1), deltas(1, 1), lineTo(2), deltas(4, 0, 0, 4), closePath(3));

        assertThat(part(polygon, 0)).isEqualTo("1 1, 5 1, 5 5, 1 1");
    }

    // A MoveTo without points keeps the start of the previous one for the next ClosePath
    @Test
    void moveToZeroKeepsLastStart() throws MalformedGeometryException {
        PackedGeometries points = packOne(GeomType.POINT, moveTo(1), deltas(3, 4), moveTo(0), closePath(1));

        assertThat(part(points, 0)).isEqualTo("3 4, 3 4");
    }

    // The cursor moves through the skipped part, and the next part starts from its point
    @Test
    void skipsLinesOfOnePoint() throws MalformedGeometryException {
        PackedGeometries line =
                packOne(GeomType.LINESTRING, moveTo(1), deltas(5, 5), moveTo(1), deltas(1, 0), lineTo(1), deltas(0, 4));

        assertThat(line.partCount(0)).isEqualTo(1);
        assertThat(part(line, 0)).isEqualTo("6 5, 6 9");
    }

    @Test
    void skipsRingsOfTwoPoints() throws MalformedGeometryException {
        PackedGeometries polygon = packOne(
                GeomType.POLYGON,
                moveTo(1),
                deltas(20, 10),
                closePath(1),
                moveTo(1),
                deltas(-20, -10),
                lineTo(2),
                deltas(4, 0, 0, 4),
                closePath(1));

        assertThat(polygon.partCount(0)).isEqualTo(1);
        assertThat(part(polygon, 0)).isEqualTo("0 0, 4 0, 4 4, 0 0");
    }

    // The hole is wound against the shells. The first ring starts a polygon, and the third one, wound like it, another.
    @Test
    void flagsRingsWoundLikeFirst() throws MalformedGeometryException {
        PackedGeometries polygons = packOne(GeomType.POLYGON, concat(ring(0, 0, 10), hole(2, 2, 6), ring(20, 20, 10)));

        assertThat(polygons.partCount(0)).isEqualTo(3);
        assertThat(polygons.startsPolygon(0)).isTrue();
        assertThat(polygons.startsPolygon(1)).isFalse();
        assertThat(polygons.startsPolygon(2)).isTrue();
    }

    // The first ring of a feature starts a polygon, whatever its winding
    @Test
    void firstRingSetsShellWinding() throws MalformedGeometryException {
        PackedGeometries polygons = packOne(GeomType.POLYGON, concat(hole(0, 0, 10), ring(2, 2, 6), hole(20, 20, 10)));

        assertThat(polygons.startsPolygon(0)).isTrue();
        assertThat(polygons.startsPolygon(1)).isFalse();
        assertThat(polygons.startsPolygon(2)).isTrue();
    }

    // Only a closed ring of zero area is skipped
    @Test
    void rejectsOpenRingOfZeroArea() {
        int[] openFlatRing = concat(moveTo(1), deltas(30, 30), lineTo(2), deltas(5, 0, 5, 0));

        assertThatThrownBy(() -> packOne(GeomType.POLYGON, concat(ring(0, 0, 10), openFlatRing)))
                .isInstanceOf(MalformedGeometryException.class)
                .hasMessage("a polygon ring does not end on its first point");
    }

    @Test
    void unknownTypeKeepsNoPart() throws MalformedGeometryException {
        PackedGeometries unknown = packOne(GeomType.UNKNOWN, ring(0, 0, 10));

        assertThat(unknown.partCount(0)).isZero();
        assertThat(unknown.heapSize()).isEqualTo(emptyLayer().heapSize());
    }

    @Test
    void checksCommandsOfUnknownType() {
        assertThatThrownBy(() -> packOne(GeomType.UNKNOWN, lineTo(1), deltas(3, 4)))
                .isInstanceOf(MalformedGeometryException.class);
    }

    @Test
    void rejectsLineToFirst() {
        assertThatThrownBy(() -> packOne(GeomType.LINESTRING, lineTo(1), deltas(3, 4)))
                .isInstanceOf(MalformedGeometryException.class)
                .hasMessage("the geometry starts with LineTo instead of MoveTo");
    }

    @Test
    void rejectsUnknownCommand() {
        assertThatThrownBy(() -> packOne(GeomType.POINT, moveTo(1), deltas(3, 4), new int[] {3}))
                .isInstanceOf(MalformedGeometryException.class)
                .hasMessage("the geometry has the unknown command 3");
    }

    @Test
    void rejectsCountPastEnd() {
        int[][] commands = {moveTo(1), deltas(0, 0), lineTo(536_870_911), deltas(6, 10)};

        assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> assertThatThrownBy(() -> packOne(GeomType.LINESTRING, commands))
                        .isInstanceOf(MalformedGeometryException.class)
                        .hasMessage("a LineTo announces 536870911 points, more than the parameters left"));
    }

    @Test
    void rejectsUnclosedRing() {
        assertThatThrownBy(() -> packOne(GeomType.POLYGON, moveTo(1), deltas(0, 0), lineTo(2), deltas(4, 0, 0, 4)))
                .isInstanceOf(MalformedGeometryException.class)
                .hasMessage("a polygon ring does not end on its first point");
    }

    // The second feature fails after its first part, and keeps no part
    @Test
    void discardsMalformedGeometry() throws MalformedGeometryException {
        packer.startLayer(3);
        pack(GeomType.LINESTRING, moveTo(1), deltas(2, 3), lineTo(1), deltas(1, 1));
        assertThatThrownBy(() -> pack(GeomType.LINESTRING, moveTo(1), deltas(9, 9), lineTo(1), deltas(1, 1), lineTo(2)))
                .isInstanceOf(MalformedGeometryException.class);
        pack(GeomType.LINESTRING, moveTo(1), deltas(7, 7), lineTo(1), deltas(1, 1));
        PackedGeometries lines = packer.finishLayer();

        assertThat(lines.partCount(1)).isZero();
        assertThat(lines.firstPart(2)).isEqualTo(1);
        assertThat(part(lines, 1)).isEqualTo("7 7, 8 8");
        assertThat(lines.partStart(1)).isEqualTo(2);
    }

    @Test
    void storesShortsWithinRange() throws MalformedGeometryException {
        PackedGeometries points = packOne(GeomType.POINT, moveTo(2), deltas(32767, -32768, -65535, 65535));

        assertThat(part(points, 0)).isEqualTo("32767 -32768, -32768 32767");
        assertThat(points.hasIntCoordinates()).isFalse();
    }

    @Test
    void storesIntsPastShortRange() throws MalformedGeometryException {
        PackedGeometries points = packOne(GeomType.POINT, moveTo(2), deltas(0, 0, 32768, -5));

        assertThat(part(points, 0)).isEqualTo("0 0, 32768 -5");
        assertThat(points.hasIntCoordinates()).isTrue();
    }

    // The point of the skipped part is past the range of a short, and the kept part is not
    @Test
    void checksRangeOfKeptCoordinates() throws MalformedGeometryException {
        PackedGeometries line = packOne(
                GeomType.LINESTRING,
                moveTo(1),
                deltas(40000, 0),
                moveTo(1),
                deltas(-39990, 0),
                lineTo(1),
                deltas(5, 5));

        assertThat(part(line, 0)).isEqualTo("10 0, 15 5");
        assertThat(line.hasIntCoordinates()).isFalse();
    }

    // The second point of the malformed feature is past the range of a short, and the point of the next one is not
    @Test
    void ignoresRangeOfMalformedGeometry() throws MalformedGeometryException {
        int[][] malformedLine = {moveTo(1), deltas(1, 1), lineTo(1), deltas(40000, 0), lineTo(2)};
        packer.startLayer(2);
        assertThatThrownBy(() -> pack(GeomType.LINESTRING, malformedLine))
                .isInstanceOf(MalformedGeometryException.class);
        pack(GeomType.POINT, moveTo(1), deltas(3, 4));
        PackedGeometries layer = packer.finishLayer();

        assertThat(part(layer, 0)).isEqualTo("3 4");
        assertThat(layer.hasIntCoordinates()).isFalse();
    }

    // The second feature needs 32 bits, after the first one was packed in 16 bits
    @Test
    void widensEarlierCoordinates() throws MalformedGeometryException {
        packer.startLayer(2);
        pack(GeomType.POINT, moveTo(1), deltas(3, -4));
        pack(GeomType.POINT, moveTo(1), deltas(40000, -70000));
        PackedGeometries points = packer.finishLayer();

        assertThat(points.hasIntCoordinates()).isTrue();
        assertThat(part(points, 0)).isEqualTo("3 -4");
        assertThat(part(points, 1)).isEqualTo("40000 -70000");
    }

    @Test
    void nextLayerStartsNarrow() throws MalformedGeometryException {
        packer.startLayer(1);
        pack(GeomType.POINT, moveTo(1), deltas(40000, 0));
        packer.finishLayer();

        PackedGeometries points = packOne(GeomType.POINT, moveTo(1), deltas(3, 4));

        assertThat(points.hasIntCoordinates()).isFalse();
        assertThat(part(points, 0)).isEqualTo("3 4");
    }

    @Test
    void classifiesWideRings() throws MalformedGeometryException {
        PackedGeometries polygons =
                packOne(GeomType.POLYGON, concat(ring(40000, 0, 10), hole(2, 2, 6), ring(20, 20, 10)));

        assertThat(polygons.hasIntCoordinates()).isTrue();
        assertThat(polygons.partCount(0)).isEqualTo(3);
        assertThat(polygons.startsPolygon(1)).isFalse();
        assertThat(polygons.startsPolygon(2)).isTrue();
        assertThat(part(polygons, 0)).isEqualTo("40000 0, 40010 0, 40010 10, 40000 10, 40000 0");
    }

    @Test
    void growsBuffersPastFirstSize() throws MalformedGeometryException {
        int[] steps = new int[2000];
        Arrays.fill(steps, 1);
        packer.startLayer(300);
        for (int feature = 0; feature < 300; feature++) {
            pack(GeomType.LINESTRING, moveTo(1), deltas(0, 0), lineTo(1000), deltas(steps));
        }
        PackedGeometries lines = packer.finishLayer();

        assertThat(lines.partCount(299)).isEqualTo(1);
        assertThat(lines.partEnd(299)).isEqualTo(300 * 1001);
        assertThat(lines.x(300 * 1001 - 1)).isEqualTo(1000);
    }

    private PackedGeometries packOne(GeomType type, int[]... commands) throws MalformedGeometryException {
        packer.startLayer(1);
        pack(type, commands);
        return packer.finishLayer();
    }

    private void pack(GeomType type, int[]... commands) throws MalformedGeometryException {
        byte[] bytes = varints(concat(commands));
        packer.pack(type, bytes, 0, bytes.length);
    }

    private PackedGeometries emptyLayer() {
        packer.startLayer(1);
        try {
            packer.pack(GeomType.UNKNOWN, new byte[0], 0, 0);
        } catch (MalformedGeometryException e) {
            throw new AssertionError(e);
        }
        return packer.finishLayer();
    }

    /** The coordinates of a part, as "x y, x y". */
    private static String part(PackedGeometries geometries, int part) {
        List<String> coordinates = new ArrayList<>();
        for (int i = geometries.partStart(part); i < geometries.partEnd(part); i++) {
            coordinates.add(geometries.x(i) + " " + geometries.y(i));
        }
        return String.join(", ", coordinates);
    }

    /**
     * A square ring of the size, from a MoveTo of (dx, dy), wound as the shells of the tests. ClosePath leaves the
     * cursor at the last point of the LineTo.
     */
    private static int[] ring(int dx, int dy, int size) {
        return concat(moveTo(1), deltas(dx, dy), lineTo(3), deltas(size, 0, 0, size, -size, 0), closePath(1));
    }

    /** A square ring wound against {@link #ring}. */
    private static int[] hole(int dx, int dy, int size) {
        return concat(moveTo(1), deltas(dx, dy), lineTo(3), deltas(0, size, size, 0, 0, -size), closePath(1));
    }

    private static int[] moveTo(int count) {
        return new int[] {(count << 3) | 1};
    }

    private static int[] lineTo(int count) {
        return new int[] {(count << 3) | 2};
    }

    private static int[] closePath(int count) {
        return new int[] {(count << 3) | 7};
    }

    private static int[] deltas(int... deltas) {
        return Arrays.stream(deltas).map(delta -> (delta << 1) ^ (delta >> 31)).toArray();
    }

    private static int[] concat(int[]... parts) {
        return Arrays.stream(parts).flatMapToInt(Arrays::stream).toArray();
    }

    // As in protobuf, a uint32 takes five bytes at most, even as a negative Java int
    private static byte[] varints(int[] values) {
        ProtoWriter writer = new ProtoWriter();
        for (int value : values) {
            writer.varint(Integer.toUnsignedLong(value));
        }
        return writer.toByteArray();
    }
}
