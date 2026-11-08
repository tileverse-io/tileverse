/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class CommandEncoderTest {

    private static final String SPEC_POLYGON = "POLYGON ((3 6, 8 12, 20 34, 3 6))";
    private static final int[] SPEC_POLYGON_COMMANDS = {9, 6, 12, 18, 10, 12, 24, 44, 15};

    private static final String SPEC_MULTIPOLYGON = """
            MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)),
            ((11 11, 20 11, 20 20, 11 20, 11 11), (13 13, 13 17, 17 17, 17 13, 13 13)))
            """;
    private static final int[] SPEC_MULTIPOLYGON_COMMANDS = {
        9, 0, 0, 26, 20, 0, 0, 20, 19, 0, 15, 9, 22, 2, 26, 18, 0, 0, 18, 17, 0, 15, 9, 4, 13, 26, 0, 8, 8, 0, 0, 7, 15
    };

    /** The square 0 0, 10 0, 10 10, 0 10: the first polygon of the multipolygon of the specification. */
    private static final int[] SQUARE_COMMANDS = {9, 0, 0, 26, 20, 0, 0, 20, 19, 0, 15};

    private final CommandEncoder encoder = new CommandEncoder();

    @Test
    void encodesSpecLineString() {
        String line = "LINESTRING (2 2, 2 10, 10 10)";

        assertThat(commands(line)).containsExactly(9, 4, 4, 18, 0, 16, 16, 0);
        assertThat(type(line)).isEqualTo(GeomType.LINESTRING);
    }

    @Test
    void encodesSpecMultiLineString() {
        String lines = "MULTILINESTRING ((2 2, 2 10, 10 10), (1 1, 3 5))";

        assertThat(commands(lines)).containsExactly(9, 4, 4, 18, 0, 16, 16, 0, 9, 17, 17, 10, 4, 8);
        assertThat(type(lines)).isEqualTo(GeomType.LINESTRING);
    }

    @Test
    void encodesSpecPolygon() {
        assertThat(commands(SPEC_POLYGON)).containsExactly(SPEC_POLYGON_COMMANDS);
        assertThat(type(SPEC_POLYGON)).isEqualTo(GeomType.POLYGON);
    }

    @Test
    void encodesSpecMultiPoint() {
        String points = "MULTIPOINT ((5 7), (3 2))";

        assertThat(commands(points)).containsExactly(17, 10, 14, 3, 9);
        assertThat(type(points)).isEqualTo(GeomType.POINT);
    }

    @Test
    void encodesSpecMultiPolygon() {
        assertThat(commands(SPEC_MULTIPOLYGON)).containsExactly(SPEC_MULTIPOLYGON_COMMANDS);
        assertThat(type(SPEC_MULTIPOLYGON)).isEqualTo(GeomType.POLYGON);
    }

    // A reversed ring keeps its first point: walked backwards from it, it gives the ring of the specification
    @Test
    void reversesRingsWithWrongOrientation() {
        Geometry reversed = geom(SPEC_MULTIPOLYGON).reverse();

        assertThat(commands(reversed)).containsExactly(SPEC_MULTIPOLYGON_COMMANDS);
    }

    @Test
    void reversesExteriorRingWithNegativeArea() {
        assertThat(commands("POLYGON ((3 6, 20 34, 8 12, 3 6))")).containsExactly(SPEC_POLYGON_COMMANDS);
    }

    @Test
    void dropsRepeatedPoints() {
        assertThat(commands("LINESTRING (3 6, 8 12, 8 12, 20 34)")).containsExactly(9, 6, 12, 18, 10, 12, 24, 44);
    }

    // Math.round rounds a half towards positive infinity: -0.5 to 0, 2.5 to 3
    @Test
    void roundsHalfUp() {
        assertThat(commands("LINESTRING (0.5 -0.5, 2.49 2.5)")).containsExactly(9, 2, 0, 10, 2, 6);
    }

    // The second ring is walked backwards: its point on the first point comes last
    @ParameterizedTest(name = "{0}")
    @MethodSource("ringsBackOnFirstPoint")
    void dropsTrailingPointOnFirstPoint(String wkt) {
        assertThat(commands(wkt)).containsExactly(9, 0, 0, 18, 20, 0, 0, 20, 15);
    }

    static Stream<String> ringsBackOnFirstPoint() {
        return Stream.of("POLYGON ((0 0, 10 0, 10 10, 0.2 0.1, 0 0))", "POLYGON ((0 0, 0.2 0.1, 10 10, 10 0, 0 0))");
    }

    // The second ring has an area before rounding
    @ParameterizedTest(name = "{0}")
    @MethodSource("zeroAreaRings")
    void dropsZeroAreaRing(String wkt) {
        assertThat(encode(wkt)).isFalse();
    }

    static Stream<String> zeroAreaRings() {
        return Stream.of("POLYGON ((0 0, 10 0, 20 0, 0 0))", "POLYGON ((0 0, 10 0, 10 0.4, 0 0))");
    }

    @Test
    void dropsZeroAreaHole() {
        String polygon = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 2 2, 3 3, 1 1))";

        assertThat(commands(polygon)).containsExactly(SQUARE_COMMANDS);
    }

    // The hole has an area: only the rule on the holes of a dropped exterior ring leaves it out
    @Test
    void dropsHolesOfDroppedShell() {
        String polygons =
                "MULTIPOLYGON (((0 0, 10 0, 20 0, 0 0), (1 1, 1 3, 3 3, 1 1)), ((0 0, 10 0, 10 10, 0 10, 0 0)))";

        assertThat(commands(polygons)).containsExactly(SQUARE_COMMANDS);
    }

    // In the second case, the MoveTo after the dropped part counts from the end of the first line
    @ParameterizedTest(name = "{0}")
    @MethodSource("linesWithCollapsedPart")
    void dropsCollapsedLinePart(String wkt, int[] expected) {
        assertThat(commands(wkt)).containsExactly(expected);
    }

    static Stream<Arguments> linesWithCollapsedPart() {
        return Stream.of(
                arguments("MULTILINESTRING ((0.2 0.2, 0.3 0.3), (1 1, 3 5))", new int[] {9, 2, 2, 10, 4, 8}),
                arguments(
                        "MULTILINESTRING ((1 1, 3 5), (8.2 8.2, 8.3 8.3), (4 4, 6 6))",
                        new int[] {9, 2, 2, 10, 4, 8, 9, 2, 1, 10, 4, 4}));
    }

    @Test
    void leavesOutCollapsedLine() {
        assertThat(encode("LINESTRING (0.2 0.2, 0.3 0.3)")).isFalse();
    }

    @Test
    void keepsRepeatedPointsOfMultiPoint() {
        assertThat(commands("MULTIPOINT ((3 6), (3 6))")).containsExactly(17, 6, 12, 0, 0);
    }

    @Test
    void writesLinearRingAsLine() {
        String ring = "LINEARRING (0 0, 10 0, 10 10, 0 0)";

        assertThat(commands(ring)).containsExactly(9, 0, 0, 26, 20, 0, 0, 20, 19, 19);
        assertThat(type(ring)).isEqualTo(GeomType.LINESTRING);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("geometriesWithEmptyParts")
    void skipsEmptyParts(String wkt, int[] expected) {
        assertThat(commands(wkt)).containsExactly(expected);
    }

    static Stream<Arguments> geometriesWithEmptyParts() {
        return Stream.of(
                arguments("MULTIPOINT ((1 2), EMPTY)", new int[] {9, 2, 4}),
                arguments("MULTILINESTRING (EMPTY, (1 1, 3 5))", new int[] {9, 2, 2, 10, 4, 8}),
                arguments("MULTIPOLYGON (EMPTY, ((0 0, 10 0, 10 10, 0 10, 0 0)))", SQUARE_COMMANDS),
                arguments("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), EMPTY)", SQUARE_COMMANDS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ordinatesOutsideIntRange")
    void rejectsCoordinatesOutsideIntRange(String wkt, String ordinate) {
        Geometry geometry = geom(wkt);

        assertThatThrownBy(() -> encoder.encode(geometry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ordinate);
    }

    static Stream<Arguments> ordinatesOutsideIntRange() {
        return Stream.of(
                arguments("POINT (1e10 0)", "1.0E10"),
                arguments("POINT (NaN 0)", "NaN"),
                arguments("POINT (0 2147483647.5)", "2.1474836475E9"),
                arguments("POLYGON ((0 0, -2147483648.6 0, 0 1, 0 0))", "-2.1474836486E9"));
    }

    // A delta from the cursor at 0,0 reaches -(2^31 - 1): the line reaches -2^31 from -1
    @Test
    void keepsOrdinatesAtIntBounds() {
        assertThat(encode("POINT (2147483647.4 -2147483647.4)")).isTrue();
        assertThat(encode("LINESTRING (-1 -1, -2147483648.5 -2147483648.5)")).isTrue();
    }

    // The specification supports deltas from -(2^31 - 1) to 2^31 - 1, and two ordinates of an int can differ by more
    @ParameterizedTest(name = "{0}")
    @MethodSource("deltasOutsideRange")
    void rejectsDeltaOutsideRange(String wkt, String delta) {
        Geometry geometry = geom(wkt);

        assertThatThrownBy(() -> encoder.encode(geometry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("delta " + delta + " ");
    }

    static Stream<Arguments> deltasOutsideRange() {
        return Stream.of(
                arguments("LINESTRING (-2000000000 0, 2000000000 0)", "4000000000"),
                arguments("MULTIPOINT ((0 2000000000), (0 -2000000000))", "-4000000000"),
                arguments("POINT (0 -2147483648)", "-2147483648"));
    }

    // Twice the area of the square is 1.8e19, beyond a long: the ring keeps its positive area, walked forwards
    @Test
    void ordersRingOfAreaBeyondLong() {
        String square = """
                POLYGON ((-1500000000 -1500000000, 0 -1500000000, 1500000000 -1500000000, 1500000000 0,
                1500000000 1500000000, 0 1500000000, -1500000000 1500000000, -1500000000 0,
                -1500000000 -1500000000))
                """;
        int plus = zigZag(1_500_000_000);
        int minus = zigZag(-1_500_000_000);
        int lineToSeven = GeometryCommand.packCommandAndLength(GeometryCommand.LineTo, 7);
        int[] expected = {
            9, minus, minus, lineToSeven, plus, 0, plus, 0, 0, plus, 0, plus, minus, 0, minus, 0, 0, minus, 15
        };

        assertThat(commands(square)).containsExactly(expected);
    }

    // Rounding before scaling would give 0 and 16
    @Test
    void roundsScaledCoordinates() {
        Geometry point = geom("POINT (0.25 0.75)");

        assertThat(commands(point, 16)).containsExactly(9, 8, 24);
    }

    // At scale 1, the polygon rounds to a point and has no area
    @Test
    void writesScaledPolygon() {
        Geometry polygon = geom("POLYGON ((0 0, 0.25 0, 0.25 0.25, 0 0.25, 0 0))");

        int[] scaled = commands(polygon, 16);
        int[] expected = commands("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
        boolean writtenAtScaleOne = encoder.encode(polygon, 1);

        assertThat(scaled).containsExactly(expected);
        assertThat(writtenAtScaleOne).isFalse();
    }

    // The message names the ordinate as given, and the scale
    @Test
    void rejectsScaledOrdinateOutsideIntRange() {
        Geometry point = geom("POINT (200000000 0)");

        assertThat(encoder.encode(point, 1)).isTrue();
        assertThatThrownBy(() -> encoder.encode(point, 16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2.0E8")
                .hasMessageContaining("16.0");
    }

    @Test
    void rejectsGeometryCollection() {
        Geometry collection = geom("GEOMETRYCOLLECTION (POINT (1 2), POINT (3 4))");

        assertThatThrownBy(() -> encoder.encode(collection))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GeometryCollection");
    }

    // A point is encoded first: the empty geometry must clear its commands
    @ParameterizedTest(name = "{0}")
    @MethodSource("emptyGeometries")
    void emptyGeometryWritesNothing(String wkt) {
        encode("POINT (25 17)");

        boolean written = encode(wkt);

        assertThat(written).isFalse();
        assertThat(encoder.commands().isEmpty()).isTrue();
    }

    static Stream<String> emptyGeometries() {
        return Stream.of("POINT EMPTY", "MULTIPOINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY");
    }

    // The cursor starts again at 0,0: the point and its commands are the example of the specification
    @Test
    void reusesBuffersBetweenCalls() {
        encode(SPEC_MULTIPOLYGON);
        IntArray firstCommands = encoder.commands();

        int[] secondCommands = commands("POINT (25 17)");

        assertThat(secondCommands).containsExactly(9, 50, 34);
        assertThat(encoder.type()).isEqualTo(GeomType.POINT);
        assertThat(encoder.commands()).isSameAs(firstCommands);
    }

    private boolean encode(String wkt) {
        return encoder.encode(geom(wkt));
    }

    private int[] commands(String wkt) {
        return commands(geom(wkt));
    }

    private int[] commands(Geometry geometry) {
        return commands(geometry, 1);
    }

    private int[] commands(Geometry geometry, double scale) {
        encoder.encode(geometry, scale);
        IntArray commands = encoder.commands();
        return Arrays.copyOf(commands.array(), commands.size());
    }

    private GeomType type(String wkt) {
        encode(wkt);
        return encoder.type();
    }

    /** The parameter of a delta, by the formula of the specification. */
    private static int zigZag(int delta) {
        return (delta << 1) ^ (delta >> 31);
    }

    private static Geometry geom(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
