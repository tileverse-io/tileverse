/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static io.tileverse.vectortile.mvt.GeometryDecoder.DEFAULT_GEOMETRY_FACTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.mvt.GeometryPacker.MalformedGeometryException;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.util.AffineTransformation;

class GeometryDecoderTest {

    private final GeometryDecoder decoder = new GeometryDecoder();

    @Test
    void decodesPolygonWithHole() {
        Geometry geometry = decode(GeomType.POLYGON, polygonWithHole());

        GeometryAssertions.assertThat(geometry)
                .equalsExact("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))");
    }

    @Test
    void decodesMultiPoint() {
        Geometry geometry = decode(GeomType.POINT, moveTo(2), deltas(5, 7, -2, -5));

        GeometryAssertions.assertThat(geometry).equalsExact("MULTIPOINT ((5 7), (3 2))");
    }

    // The third ring is wound as the first one, and starts a polygon. Its MoveTo counts from 8 2, the end of the hole.
    @Test
    void groupsRingsIntoPolygons() {
        int[] secondPolygon = concat(moveTo(1), deltas(12, 18), lineTo(3), deltas(5, 0, 0, 5, -5, 0), closePath());

        Geometry geometry = decode(GeomType.POLYGON, polygonWithHole(), secondPolygon);

        GeometryAssertions.assertThat(geometry)
                .equalsExact("MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)),"
                        + " ((20 20, 25 20, 25 25, 20 25, 20 20)))");
    }

    @Test
    void skipsRingOfZeroArea() {
        int[] shell = concat(moveTo(1), deltas(-20, 0), lineTo(3), deltas(10, 0, 0, 10, -10, 0), closePath());

        Geometry geometry = decode(GeomType.POLYGON, flatRing(), shell);

        GeometryAssertions.assertThat(geometry).equalsExact("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    }

    // The ring of zero area between the shell and its hole starts no polygon
    @Test
    void keepsHoleAfterZeroAreaRing() {
        int[] shell = concat(moveTo(1), deltas(0, 0), lineTo(3), deltas(10, 0, 0, 10, -10, 0), closePath());
        int[] zeroAreaRing = concat(moveTo(1), deltas(20, -10), lineTo(2), deltas(10, 0, 10, 0), closePath());
        int[] hole = concat(moveTo(1), deltas(-38, 2), lineTo(3), deltas(0, 6, 6, 0, 0, -6), closePath());

        Geometry geometry = decode(GeomType.POLYGON, shell, zeroAreaRing, hole);

        GeometryAssertions.assertThat(geometry)
                .equalsExact("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))");
    }

    // The shell winds against the shell of polygonWithHole: as the first ring with an area, it sets the winding
    @Test
    void skipsZeroAreaFirstRing() {
        int[] shell = concat(moveTo(1), deltas(-20, 0), lineTo(3), deltas(0, 10, 10, 0, 0, -10), closePath());
        int[] hole = concat(moveTo(1), deltas(-8, 2), lineTo(3), deltas(6, 0, 0, 6, -6, 0), closePath());

        Geometry geometry = decode(GeomType.POLYGON, flatRing(), shell, hole);

        GeometryAssertions.assertThat(geometry)
                .equalsExact("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))");
    }

    // An empty polygon, as for a feature with only rings of fewer than 3 points
    @Test
    void decodesOnlyZeroAreaRings() {
        int[] diagonal = concat(moveTo(1), deltas(0, 5), lineTo(2), deltas(5, 5, 5, 5), closePath());

        Geometry geometry = decode(GeomType.POLYGON, flatRing(), diagonal);

        GeometryAssertions.assertThat(geometry).isPolygon().isEmpty();
    }

    // The sequence of create(size, 2), filled from one array
    @Test
    void packsDefaultSequencesOfDoubles() {
        Geometry line = decode(GeomType.LINESTRING, moveTo(1), deltas(1, 2), lineTo(1), deltas(3, 4));

        CoordinateSequence sequence = ((LineString) line).getCoordinateSequence();
        assertThat(sequence).isInstanceOf(PackedCoordinateSequence.Double.class);
        assertThat(sequence.getDimension()).isEqualTo(2);
        assertThat(sequence.getMeasures()).isZero();
    }

    @Test
    void packsSequencesOfFloats() {
        GeometryFactory floats = new GeometryFactory(PackedCoordinateSequenceFactory.FLOAT_FACTORY);
        PackedGeometries geometries = pack(GeomType.LINESTRING, moveTo(1), deltas(1, 2), lineTo(1), deltas(3, 4));

        Geometry line = decoder.decode(GeomType.LINESTRING, geometries, 0, floats);

        GeometryAssertions.assertThat(line).equalsExact("LINESTRING (1 2, 4 6)");
        assertThat(((LineString) line).getCoordinateSequence()).isInstanceOf(PackedCoordinateSequence.Float.class);
    }

    @Test
    void decodesUnknownAsCollection() {
        Geometry geometry = decode(GeomType.UNKNOWN, moveTo(1), deltas(25, 17));

        GeometryAssertions.assertThat(geometry).isGeometryCollection().isEmpty();
    }

    @Test
    void decodesCountPastEndAsEmpty() {
        int[][] commands = {moveTo(1), deltas(0, 0), lineTo(536_870_911), deltas(6, 10)};

        Geometry geometry =
                assertTimeoutPreemptively(Duration.ofSeconds(5), () -> decode(GeomType.LINESTRING, commands));

        GeometryAssertions.assertThat(geometry).isLineString().isEmpty();
    }

    @Test
    void decodesLineToFirstAsEmpty() {
        Geometry geometry = decode(GeomType.LINESTRING, lineTo(1), deltas(3, 4));

        GeometryAssertions.assertThat(geometry).isLineString().isEmpty();
    }

    @Test
    void appliesTransformation() {
        AffineTransformation scaleByTwo = AffineTransformation.scaleInstance(2, 2);
        GeometryDecoder scaling = new GeometryDecoder(DEFAULT_GEOMETRY_FACTORY, GeometryReader.toFunction(scaleByTwo));
        PackedGeometries geometries = pack(GeomType.POINT, moveTo(1), deltas(25, 17));

        Geometry geometry = scaling.decode(GeomType.POINT, geometries, 0, DEFAULT_GEOMETRY_FACTORY);

        GeometryAssertions.assertThat(geometry).equalsExact("POINT (50 34)");
    }

    // A transformation in place changes the coordinates of the geometry read, and leaves the next read unchanged
    @Test
    void readsIntoNewCoordinates() {
        PackedGeometries geometries = pack(GeomType.LINESTRING, moveTo(1), deltas(1, 2), lineTo(1), deltas(3, 4));
        Geometry first = decoder.decode(GeomType.LINESTRING, geometries, 0, DEFAULT_GEOMETRY_FACTORY);

        first.apply(AffineTransformation.translationInstance(100, 100));

        Geometry second = decoder.decode(GeomType.LINESTRING, geometries, 0, DEFAULT_GEOMETRY_FACTORY);
        GeometryAssertions.assertThat(first).equalsExact("LINESTRING (101 102, 104 106)");
        GeometryAssertions.assertThat(second).equalsExact("LINESTRING (1 2, 4 6)");
    }

    @Test
    void viewsPartsOfDefaultFactory() {
        PackedGeometries geometries = pack(GeomType.LINESTRING, twoLines());

        Geometry lines = decoder.decode(GeomType.LINESTRING, geometries, 0, DEFAULT_GEOMETRY_FACTORY);

        GeometryAssertions.assertThat(lines).equalsExact("MULTILINESTRING ((1 1, 2 2), (5 5, 6 6, 7 7))");
        assertThat(sequenceOfLine(lines, 0)).isInstanceOf(SubCoordinateSequence.class);
        assertThat(sequenceOfLine(lines, 1)).isInstanceOf(SubCoordinateSequence.class);
    }

    @Test
    void copiesPartsOfOtherFactory() {
        GeometryFactory arrays = new GeometryFactory(CoordinateArraySequenceFactory.instance());
        PackedGeometries geometries = pack(GeomType.LINESTRING, twoLines());

        Geometry lines = decoder.decode(GeomType.LINESTRING, geometries, 0, arrays);

        GeometryAssertions.assertThat(lines).equalsExact("MULTILINESTRING ((1 1, 2 2), (5 5, 6 6, 7 7))");
        assertThat(lines.getFactory()).isSameAs(arrays);
        assertThat(sequenceOfLine(lines, 0)).isInstanceOf(CoordinateArraySequence.class);
        assertThat(sequenceOfLine(lines, 1)).isInstanceOf(CoordinateArraySequence.class);
    }

    @Test
    void printsCommands() {
        byte[] bytes = varints(concat(moveTo(1), deltas(25, 17)));
        VarintCursor commands = new VarintCursor(bytes, 0, bytes.length);

        String printed = GeometryDecoder.toString(commands);

        assertThat(printed).contains("MoveTo, Count: 1", "Point 0: (25, 17)");
    }

    private Geometry decode(GeomType type, int[]... commands) {
        return decoder.decode(type, pack(type, commands), 0, DEFAULT_GEOMETRY_FACTORY);
    }

    /** Packs the commands as the only feature of a layer. A malformed geometry is packed as empty, as by default. */
    private static PackedGeometries pack(GeomType type, int[]... commands) {
        byte[] bytes = varints(concat(commands));
        GeometryPacker packer = new GeometryPacker();
        packer.startLayer(1);
        try {
            packer.pack(type, bytes, 0, bytes.length);
        } catch (MalformedGeometryException e) {
            // The feature keeps no part
        }
        return packer.finishLayer();
    }

    private static CoordinateSequence sequenceOfLine(Geometry lines, int index) {
        return ((LineString) lines.getGeometryN(index)).getCoordinateSequence();
    }

    // The MoveTo of the hole counts from 0 10: ClosePath leaves the cursor at the last point of the shell
    private static int[] polygonWithHole() {
        return concat(
                moveTo(1),
                deltas(0, 0),
                lineTo(3),
                deltas(10, 0, 0, 10, -10, 0),
                closePath(),
                moveTo(1),
                deltas(2, -8),
                lineTo(3),
                deltas(0, 6, 6, 0, 0, -6),
                closePath());
    }

    /** MoveTo(0,0) LineTo(10,0) LineTo(20,0) ClosePath: a ring of zero area. ClosePath leaves the cursor at 20 0. */
    private static int[] flatRing() {
        return concat(moveTo(1), deltas(0, 0), lineTo(2), deltas(10, 0, 10, 0), closePath());
    }

    private static int[] twoLines() {
        return concat(
                moveTo(1),
                deltas(1, 1),
                lineTo(1),
                deltas(1, 1),
                moveTo(1),
                deltas(3, 3),
                lineTo(2),
                deltas(1, 1, 1, 1));
    }

    private static int[] concat(int[]... commands) {
        return Arrays.stream(commands).flatMapToInt(Arrays::stream).toArray();
    }

    // As in protobuf, a uint32 takes five bytes at most, even as a negative Java int
    private static byte[] varints(int[] values) {
        ProtoWriter writer = new ProtoWriter();
        for (int value : values) {
            writer.varint(Integer.toUnsignedLong(value));
        }
        return writer.toByteArray();
    }

    private static int[] moveTo(int count) {
        return new int[] {(count << 3) | 1};
    }

    private static int[] lineTo(int count) {
        return new int[] {(count << 3) | 2};
    }

    private static int[] closePath() {
        return new int[] {15};
    }

    private static int[] deltas(int... deltas) {
        return Arrays.stream(deltas).map(GeometryDecoderTest::zigzag).toArray();
    }

    private static int zigzag(int delta) {
        return (delta << 1) ^ (delta >> 31);
    }
}
