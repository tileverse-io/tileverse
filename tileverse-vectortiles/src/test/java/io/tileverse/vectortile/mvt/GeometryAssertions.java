/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.vectortile.mvt;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * AssertJ-style fluent assertions for JTS geometries with proper type safety.
 *
 * <p>Usage example:
 * <pre>{@code
 * Geometry actual = ...;
 *
 * GeometryAssertions.assertThat(actual)
 *     .isValid()
 *     .isSimple()
 *     .equalsExact("LINESTRING(0 0, 10 10)", 0.001)  // WKT overload
 *     .intersects("POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))")  // WKT overload
 *     .covers("POINT(5 5)");
 * }</pre>
 */
public class GeometryAssertions extends AbstractAssert<GeometryAssertions, Geometry> {

    public GeometryAssertions(Geometry actual) {
        super(actual, GeometryAssertions.class);
    }

    /**
     * Entry point for fluent assertions on JTS geometries.
     */
    public static GeometryAssertions assertThat(Geometry actual) {
        return new GeometryAssertions(actual);
    }

    /**
     * Helper method to parse WKT strings into geometries.
     * @param wkt the WKT string to parse
     * @return the parsed geometry
     * @throws IllegalArgumentException if the WKT cannot be parsed
     */
    private Geometry parseWkt(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse WKT: " + wkt, e);
        }
    }

    /**
     * Assert that the geometry is exactly equal to the expected geometry within the specified tolerance.
     * Uses JTS {@link Geometry#equalsExact(Geometry, double)} for precise coordinate comparison.
     */
    public GeometryAssertions equalsExact(Geometry expected, double tolerance) {
        isNotNull();
        boolean isEqual = actual.equalsExact(expected, tolerance);
        Assertions.assertThat(isEqual)
                .overridingErrorMessage(
                        "Expected geometry to be exactly equal to <%s> within tolerance <%s> but was <%s>",
                        expected, tolerance, actual)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is exactly equal to the expected geometry (zero tolerance).
     * Uses JTS {@link Geometry#equalsExact(Geometry)} for precise coordinate comparison.
     */
    public GeometryAssertions equalsExact(Geometry expected) {
        return equalsExact(expected, 0.0);
    }

    /**
     * Assert that the geometry is exactly equal to the expected WKT geometry within the specified tolerance.
     * Uses JTS {@link Geometry#equalsExact(Geometry, double)} for precise coordinate comparison.
     */
    public GeometryAssertions equalsExact(String expectedWkt, double tolerance) {
        return equalsExact(parseWkt(expectedWkt), tolerance);
    }

    /**
     * Assert that the geometry is exactly equal to the expected WKT geometry (zero tolerance).
     * Uses JTS {@link Geometry#equalsExact(Geometry)} for precise coordinate comparison.
     */
    public GeometryAssertions equalsExact(String expectedWkt) {
        return equalsExact(parseWkt(expectedWkt), 0.0);
    }

    /**
     * Assert that the geometry is normalized-equal to the expected geometry.
     * Uses JTS {@link Geometry#equalsNorm(Geometry)} which normalizes geometries before comparison.
     */
    public GeometryAssertions equalsNorm(Geometry expected) {
        isNotNull();
        boolean isEqual = actual.equalsNorm(expected);
        Geometry normExpected = expected.norm();
        Geometry normActual = actual.norm();
        Assertions.assertThat(isEqual)
                .overridingErrorMessage(
                        "Expected geometry to be norm-equal to <%s> but was <%s>", normExpected, normActual)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is topologically equal to the expected geometry.
     * Uses JTS {@link Geometry#equalsTopo(Geometry)} for topological comparison.
     */
    public GeometryAssertions equalsTopo(Geometry expected) {
        isNotNull();
        boolean isEqual = actual.equalsTopo(expected);
        Assertions.assertThat(isEqual)
                .overridingErrorMessage(
                        "Expected geometry to be topologically equal to <%s> but was <%s>", expected, actual)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is normalized-equal to the expected WKT geometry.
     * Uses JTS {@link Geometry#equalsNorm(Geometry)} which normalizes geometries before comparison.
     */
    public GeometryAssertions equalsNorm(String expectedWkt) {
        return equalsNorm(parseWkt(expectedWkt));
    }

    /**
     * Assert that the geometry is topologically equal to the expected WKT geometry.
     * Uses JTS {@link Geometry#equalsTopo(Geometry)} for topological comparison.
     */
    public GeometryAssertions equalsTopo(String expectedWkt) {
        return equalsTopo(parseWkt(expectedWkt));
    }

    /**
     * Assert that the geometry is normalized.
     */
    public GeometryAssertions isNormalized() {
        isNotNull();
        Geometry expected = actual.norm();
        return equalsNorm(expected);
    }

    /**
     * Assert that the geometry is valid according to OGC standards.
     * Uses JTS {@link Geometry#isValid()}.
     */
    public GeometryAssertions isValid() {
        isNotNull();
        boolean valid = actual.isValid();
        Assertions.assertThat(valid)
                .overridingErrorMessage("Expected geometry to be valid but was invalid: <%s>", actual)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is invalid according to OGC standards.
     * Uses JTS {@link Geometry#isValid()}.
     */
    public GeometryAssertions isInvalid() {
        isNotNull();
        boolean valid = actual.isValid();
        Assertions.assertThat(valid)
                .overridingErrorMessage("Expected geometry to be invalid but was valid: <%s>", actual)
                .isFalse();
        return this;
    }

    /**
     * Assert that the geometry is simple (no self-intersections).
     * Uses JTS {@link Geometry#isSimple()}.
     */
    public GeometryAssertions isSimple() {
        isNotNull();
        boolean simple = actual.isSimple();
        Assertions.assertThat(simple)
                .overridingErrorMessage("Expected geometry to be simple but was not simple: <%s>", actual)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is not simple (has self-intersections).
     * Uses JTS {@link Geometry#isSimple()}.
     */
    public GeometryAssertions isNotSimple() {
        isNotNull();
        boolean simple = actual.isSimple();
        Assertions.assertThat(simple)
                .overridingErrorMessage("Expected geometry to not be simple but was simple: <%s>", actual)
                .isFalse();
        return this;
    }

    /**
     * Assert that the geometry is empty.
     * Uses JTS {@link Geometry#isEmpty()}.
     */
    public GeometryAssertions isEmpty() {
        isNotNull();
        boolean empty = actual.isEmpty();
        Assertions.assertThat(empty)
                .overridingErrorMessage("Expected geometry to be empty but was not empty: <%s>", actual)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is not empty.
     * Uses JTS {@link Geometry#isEmpty()}.
     */
    public GeometryAssertions isNotEmpty() {
        isNotNull();
        boolean empty = actual.isEmpty();
        Assertions.assertThat(empty)
                .overridingErrorMessage("Expected geometry to not be empty but was empty: <%s>", actual)
                .isFalse();
        return this;
    }

    // Geometry type assertions

    /**
     * Assert that the geometry is a Point.
     */
    public GeometryAssertions isPoint() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage("Expected geometry to be a Point but was <%s>", actual.getGeometryType())
                .isInstanceOf(Point.class);
        return this;
    }

    /**
     * Assert that the geometry is a LineString.
     */
    public GeometryAssertions isLineString() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage("Expected geometry to be a LineString but was <%s>", actual.getGeometryType())
                .isInstanceOf(LineString.class);
        return this;
    }

    /**
     * Assert that the geometry is a Polygon.
     */
    public GeometryAssertions isPolygon() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage("Expected geometry to be a Polygon but was <%s>", actual.getGeometryType())
                .isInstanceOf(Polygon.class);
        return this;
    }

    /**
     * Assert that the geometry is a MultiPoint.
     */
    public GeometryAssertions isMultiPoint() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage("Expected geometry to be a MultiPoint but was <%s>", actual.getGeometryType())
                .isInstanceOf(MultiPoint.class);
        return this;
    }

    /**
     * Assert that the geometry is a MultiLineString.
     */
    public GeometryAssertions isMultiLineString() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage(
                        "Expected geometry to be a MultiLineString but was <%s>", actual.getGeometryType())
                .isInstanceOf(MultiLineString.class);
        return this;
    }

    /**
     * Assert that the geometry is a MultiPolygon.
     */
    public GeometryAssertions isMultiPolygon() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage("Expected geometry to be a MultiPolygon but was <%s>", actual.getGeometryType())
                .isInstanceOf(MultiPolygon.class);
        return this;
    }

    /**
     * Assert that the geometry is a GeometryCollection.
     */
    public GeometryAssertions isGeometryCollection() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage(
                        "Expected geometry to be a GeometryCollection but was <%s>", actual.getGeometryType())
                .isInstanceOf(GeometryCollection.class);
        return this;
    }

    /**
     * Assert that the geometry has the expected geometry type name.
     */
    public GeometryAssertions hasGeometryType(String expectedTypeName) {
        isNotNull();
        Assertions.assertThat(actual.getGeometryType())
                .overridingErrorMessage(
                        "Expected geometry type to be <%s> but was <%s>", expectedTypeName, actual.getGeometryType())
                .isEqualTo(expectedTypeName);
        return this;
    }

    // Geometry interface-based assertions (covers multiple specific types)

    /**
     * Assert that the geometry is puntal (point-like): Point or MultiPoint.
     * Uses JTS {@link Puntal} interface which covers both Point and MultiPoint geometries.
     */
    public GeometryAssertions isPuntal() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage(
                        "Expected geometry to be puntal (Point or MultiPoint) but was <%s>", actual.getGeometryType())
                .isInstanceOf(Puntal.class);
        return this;
    }

    /**
     * Assert that the geometry is lineal (line-like): LineString or MultiLineString.
     * Uses JTS {@link Lineal} interface which covers both LineString and MultiLineString geometries.
     */
    public GeometryAssertions isLineal() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage(
                        "Expected geometry to be lineal (LineString or MultiLineString) but was <%s>",
                        actual.getGeometryType())
                .isInstanceOf(Lineal.class);
        return this;
    }

    /**
     * Assert that the geometry is polygonal (polygon-like): Polygon or MultiPolygon.
     * Uses JTS {@link Polygonal} interface which covers both Polygon and MultiPolygon geometries.
     */
    public GeometryAssertions isPolygonal() {
        isNotNull();
        Assertions.assertThat(actual)
                .overridingErrorMessage(
                        "Expected geometry to be polygonal (Polygon or MultiPolygon) but was <%s>",
                        actual.getGeometryType())
                .isInstanceOf(Polygonal.class);
        return this;
    }

    // Spatial relationship assertions

    /**
     * Assert that the geometry is disjoint from the other geometry.
     * Uses JTS {@link Geometry#disjoint(Geometry)}.
     */
    public GeometryAssertions disjoint(Geometry other) {
        isNotNull();
        boolean isDisjoint = actual.disjoint(other);
        Assertions.assertThat(isDisjoint)
                .overridingErrorMessage(
                        "Expected geometry <%s> to be disjoint from <%s> but they intersect", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry touches the other geometry.
     * Uses JTS {@link Geometry#touches(Geometry)}.
     */
    public GeometryAssertions touches(Geometry other) {
        isNotNull();
        boolean touches = actual.touches(other);
        Assertions.assertThat(touches)
                .overridingErrorMessage("Expected geometry <%s> to touch <%s> but they don't touch", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry intersects the other geometry.
     * Uses JTS {@link Geometry#intersects(Geometry)}.
     */
    public GeometryAssertions intersects(Geometry other) {
        isNotNull();
        boolean intersects = actual.intersects(other);
        Assertions.assertThat(intersects)
                .overridingErrorMessage(
                        "Expected geometry <%s> to intersect <%s> but they don't intersect", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry does not intersect the other geometry.
     * Uses JTS {@link Geometry#intersects(Geometry)}.
     */
    public GeometryAssertions doesNotIntersect(Geometry other) {
        isNotNull();
        boolean intersects = actual.intersects(other);
        Assertions.assertThat(intersects)
                .overridingErrorMessage(
                        "Expected geometry <%s> to not intersect <%s> but they intersect", actual, other)
                .isFalse();
        return this;
    }

    /**
     * Assert that the geometry crosses the other geometry.
     * Uses JTS {@link Geometry#crosses(Geometry)}.
     */
    public GeometryAssertions crosses(Geometry other) {
        isNotNull();
        boolean crosses = actual.crosses(other);
        Assertions.assertThat(crosses)
                .overridingErrorMessage("Expected geometry <%s> to cross <%s> but they don't cross", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is within the other geometry.
     * Uses JTS {@link Geometry#within(Geometry)}.
     */
    public GeometryAssertions within(Geometry other) {
        isNotNull();
        boolean within = actual.within(other);
        Assertions.assertThat(within)
                .overridingErrorMessage("Expected geometry <%s> to be within <%s> but it's not", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry contains the other geometry.
     * Uses JTS {@link Geometry#contains(Geometry)}.
     */
    public GeometryAssertions contains(Geometry other) {
        isNotNull();
        boolean contains = actual.contains(other);
        Assertions.assertThat(contains)
                .overridingErrorMessage("Expected geometry <%s> to contain <%s> but it doesn't", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry does not contain the other geometry.
     * Uses JTS {@link Geometry#contains(Geometry)}.
     */
    public GeometryAssertions doesNotContain(Geometry other) {
        isNotNull();
        boolean contains = actual.contains(other);
        Assertions.assertThat(contains)
                .overridingErrorMessage("Expected geometry <%s> to not contain <%s> but it does", actual, other)
                .isFalse();
        return this;
    }

    /**
     * Assert that the geometry overlaps the other geometry.
     * Uses JTS {@link Geometry#overlaps(Geometry)}.
     */
    public GeometryAssertions overlaps(Geometry other) {
        isNotNull();
        boolean overlaps = actual.overlaps(other);
        Assertions.assertThat(overlaps)
                .overridingErrorMessage("Expected geometry <%s> to overlap <%s> but they don't overlap", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry covers the other geometry.
     * Uses JTS {@link Geometry#covers(Geometry)}.
     */
    public GeometryAssertions covers(Geometry other) {
        isNotNull();
        boolean covers = actual.covers(other);
        Assertions.assertThat(covers)
                .overridingErrorMessage("Expected geometry <%s> to cover <%s> but it doesn't", actual, other)
                .isTrue();
        return this;
    }

    /**
     * Assert that the geometry is covered by the other geometry.
     * Uses JTS {@link Geometry#coveredBy(Geometry)}.
     */
    public GeometryAssertions coveredBy(Geometry other) {
        isNotNull();
        boolean coveredBy = actual.coveredBy(other);
        Assertions.assertThat(coveredBy)
                .overridingErrorMessage("Expected geometry <%s> to be covered by <%s> but it's not", actual, other)
                .isTrue();
        return this;
    }

    // WKT overloads for spatial relationship assertions

    /**
     * Assert that the geometry is disjoint from the other WKT geometry.
     * Uses JTS {@link Geometry#disjoint(Geometry)}.
     */
    public GeometryAssertions disjoint(String otherWkt) {
        return disjoint(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry touches the other WKT geometry.
     * Uses JTS {@link Geometry#touches(Geometry)}.
     */
    public GeometryAssertions touches(String otherWkt) {
        return touches(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry intersects the other WKT geometry.
     * Uses JTS {@link Geometry#intersects(Geometry)}.
     */
    public GeometryAssertions intersects(String otherWkt) {
        return intersects(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry does not intersect the other WKT geometry.
     * Uses JTS {@link Geometry#intersects(Geometry)}.
     */
    public GeometryAssertions doesNotIntersect(String otherWkt) {
        return doesNotIntersect(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry crosses the other WKT geometry.
     * Uses JTS {@link Geometry#crosses(Geometry)}.
     */
    public GeometryAssertions crosses(String otherWkt) {
        return crosses(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry is within the other WKT geometry.
     * Uses JTS {@link Geometry#within(Geometry)}.
     */
    public GeometryAssertions within(String otherWkt) {
        return within(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry contains the other WKT geometry.
     * Uses JTS {@link Geometry#contains(Geometry)}.
     */
    public GeometryAssertions contains(String otherWkt) {
        return contains(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry does not contain the other WKT geometry.
     * Uses JTS {@link Geometry#contains(Geometry)}.
     */
    public GeometryAssertions doesNotContain(String otherWkt) {
        return doesNotContain(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry overlaps the other WKT geometry.
     * Uses JTS {@link Geometry#overlaps(Geometry)}.
     */
    public GeometryAssertions overlaps(String otherWkt) {
        return overlaps(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry covers the other WKT geometry.
     * Uses JTS {@link Geometry#covers(Geometry)}.
     */
    public GeometryAssertions covers(String otherWkt) {
        return covers(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry is covered by the other WKT geometry.
     * Uses JTS {@link Geometry#coveredBy(Geometry)}.
     */
    public GeometryAssertions coveredBy(String otherWkt) {
        return coveredBy(parseWkt(otherWkt));
    }

    /**
     * Assert that the geometry has the expected area within the specified tolerance.
     */
    public GeometryAssertions hasArea(double expectedArea, double tolerance) {
        isNotNull();
        double actualArea = actual.getArea();
        Assertions.assertThat(actualArea)
                .overridingErrorMessage(
                        "Expected geometry area to be <%s> ± <%s> but was <%s>", expectedArea, tolerance, actualArea)
                .isCloseTo(expectedArea, Assertions.within(tolerance));
        return this;
    }

    /**
     * Assert that the geometry has the expected length/perimeter within the specified tolerance.
     */
    public GeometryAssertions hasLength(double expectedLength, double tolerance) {
        isNotNull();
        double actualLength = actual.getLength();
        Assertions.assertThat(actualLength)
                .overridingErrorMessage(
                        "Expected geometry length to be <%s> ± <%s> but was <%s>",
                        expectedLength, tolerance, actualLength)
                .isCloseTo(expectedLength, Assertions.within(tolerance));
        return this;
    }

    /**
     * Assert that the geometry has the expected number of points.
     */
    public GeometryAssertions hasNumPoints(int expectedNumPoints) {
        isNotNull();
        int actualNumPoints = actual.getNumPoints();
        Assertions.assertThat(actualNumPoints)
                .overridingErrorMessage(
                        "Expected geometry to have <%s> points but had <%s>", expectedNumPoints, actualNumPoints)
                .isEqualTo(expectedNumPoints);
        return this;
    }

    /**
     * Assert that the geometry has the expected number of geometries (for collections).
     */
    public GeometryAssertions hasNumGeometries(int expectedNumGeometries) {
        isNotNull();
        int actualNumGeometries = actual.getNumGeometries();
        Assertions.assertThat(actualNumGeometries)
                .overridingErrorMessage(
                        "Expected geometry to have <%s> sub-geometries but had <%s>",
                        expectedNumGeometries, actualNumGeometries)
                .isEqualTo(expectedNumGeometries);
        return this;
    }
}
