/*
 * Copyright 2015 Electronic Chart Centre
 * Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Modifications: Modernized and integrated into Tileverse PMTiles project.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.vectortile.model.Feature;
import io.tileverse.vectortile.model.Tile;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.util.Stopwatch;

class VectorTileCodecTest {

    private GeometryFactory gf = new GeometryFactory();
    private VectorTileCodec codec = new VectorTileCodec();

    private int extent = 4096;
    //
    //    @Test
    //    void testAutoScaleOnOff() throws IOException {
    //        // Use a point clearly within 0-255 range to test mixed modes
    //        Geometry geometry = geom("POINT(128 96)");
    //
    //        // applies to both the encoder and decoder in testFeatureRoundTrip
    //        autoScale = false;
    //        extent = 512;
    //        testFeatureRoundTrip(geometry, geometry, Map.of());
    //
    //        // encoder preserves coordinates (128,96 stored directly)
    //        autoScale = false;
    //        byte[] encoded = encode(geometry, Map.of(), "layer");
    //        // decoder scales down to 0..255: (128,96) * (256/512) = (64, 48)
    //        Tile tile = decode(encoded, true); // autoScale=true for decoder
    //        Feature decoded =
    //                tile.getLayer("layer").orElseThrow().getFeatures().findFirst().orElseThrow();
    //        Geometry expected = geom("POINT(64 48)");
    //        Geometry actual = decoded.getGeometry();
    //        assertEquals(expected.toString(), actual.toString());
    //
    //        // encoder scales: input (128,96) * (512/256) = (256,192) stored
    //        autoScale = true;
    //        encoded = encode(geometry, Map.of(), "layer");
    //        // decoder preserves coordinates: (256,192) * 1.0 = (256,192)
    //        tile = decode(encoded, false); // autoScale=false for decoder
    //        decoded = tile.getLayer("layer")
    //                .orElseThrow()
    //                .getFeatures()
    //                .findFirst()
    //                .orElseThrow(); // no coordinate transform = extent space
    //        expected = geom("POINT(256 192)");
    //        actual = decoded.getGeometry();
    //        assertEquals(expected.toString(), actual.toString());
    //    }

    @Test
    void testCustomGeometryFactory() throws IOException {
        // Create a tile with test geometries using default codec
        Geometry point = geom("POINT(100 200)");
        Geometry line = geom("LINESTRING(0 0, 50 50, 100 0)");

        VectorTileBuilder builder = new VectorTileBuilder().setExtent(4096);
        Tile tile = builder.layer()
                .name("test_layer")
                .feature(Map.of("type", "point"), point)
                .feature(Map.of("type", "line"), line)
                .build()
                .build();

        byte[] encoded = codec.encode(tile);

        // Test 1: Default codec uses PackedCoordinateSequenceFactory
        VectorTileCodec defaultCodec = new VectorTileCodec();
        Tile decodedDefault = defaultCodec.decode(encoded);

        Feature defaultPointFeature =
                decodedDefault.getFeatures("test_layer").findFirst().orElseThrow();
        Geometry defaultGeometry = defaultPointFeature.getGeometry();

        // Verify default uses PackedCoordinateSequenceFactory
        assertThat(defaultGeometry.getFactory()).isSameAs(MvtTile.DEFAULT_GEOMETRY_FACTORY);

        // Test 2: Custom codec with CoordinateArraySequenceFactory
        GeometryFactory customFactory = new GeometryFactory(CoordinateArraySequenceFactory.instance());
        VectorTileCodec customCodec = new VectorTileCodec(customFactory);
        Tile decodedCustom = customCodec.decode(encoded);

        Feature customPointFeature =
                decodedCustom.getFeatures("test_layer").findFirst().orElseThrow();
        Geometry customGeometry = customPointFeature.getGeometry();

        // Verify custom uses CoordinateArraySequenceFactory
        assertThat(customGeometry.getFactory()).isSameAs(customFactory);

        assertThat(customGeometry.getFactory().getCoordinateSequenceFactory())
                .isInstanceOf(CoordinateArraySequenceFactory.class);

        // Verify coordinates are identical despite different factories
        assertThat(customGeometry.getCoordinate()).isEqualTo(defaultGeometry.getCoordinate());

        // Test LineString as well to verify factory is used for all geometry types
        Feature customLineFeature =
                decodedCustom.getFeatures("test_layer").skip(1).findFirst().orElseThrow();
        Geometry customLine = customLineFeature.getGeometry();
        assertThat(customLine.getFactory()).isSameAs(customFactory);
    }

    @Test
    void testPoint() throws IOException {
        Geometry geometry = geom("POINT(2 3)");
        testFeatureRoundTrip(geometry, Map.of("hello", 123));
    }

    @Test
    void testMultiPoint() throws IOException {
        Geometry geometry = geom("MULTIPOINT((2 3), (3 4))");
        testFeatureRoundTrip(geometry, Map.of("hello", 123));
    }

    @Test
    void testMultiPointRepeatedPoints() throws IOException {
        Geometry geom = geom("MULTIPOINT((3 6), (3 6), (3 6), (3 6))");
        testFeatureRoundTrip(geom, Map.of());
    }

    @Test
    void testLineString() throws IOException {
        Geometry geometry = geom("LINESTRING(1 2, 10 20, 100 200)");
        testFeatureRoundTrip(geometry, Map.of("aa", "bb", "cc", "bb"));
    }

    @Test
    void testMultiLineString() throws IOException {
        Geometry geometry = geom("MULTILINESTRING((1 2, 5 6, 7 8), (8 10, 11 12))");
        testFeatureRoundTrip(geometry, Map.of());
    }

    @Test
    void testPolygon() throws IOException {
        String wkt = "POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))";
        Geometry geometry = geom(wkt);
        assertTrue(geometry.isValid());

        Feature f = testFeatureRoundTrip(geometry, Map.of());

        Geometry decodedGeometry = f.getGeometry();

        GeometryAssertions.assertThat(decodedGeometry).equalsNorm(geometry);
    }

    @Test
    void testPolygonWithHole() throws IOException {
        // beware the input polygon is counter-clockwise, we should be lenient despite the spec mandating outer rings
        // being clockwise
        Geometry geometry = geom("POLYGON((10 10, 20 10, 20 20, 10 20, 10 10), (11 11, 11 19, 19 19, 19 11, 11 11))");
        testFeatureRoundTrip(geometry, Map.of());
    }

    @Test
    void testMultiPolygon() throws IOException {
        // beware the input polygon is counter-clockwise, we should be lenient despite the spec mandating outer rings
        // being clockwise
        Geometry geometry = geom(
                "MULTIPOLYGON(((10 10, 20 10, 20 20, 10 20, 10 10)), ((110 110, 110 190, 190 190, 190 110, 110 110)))");
        assertTrue(geometry.isValid());

        testFeatureRoundTrip(geometry, Map.of());
    }

    @Test
    void testExternal() throws IOException {
        // from https://github.com/mapbox/vector-tile-js/tree/master/test/fixtures
        Tile decoded = decodeClasspathResource("/14-8801-5371.vector.pbf");

        assertThat(decoded.getLayerNames())
                .containsExactlyInAnyOrder(
                        "admin",
                        "aeroway",
                        "barrier_line",
                        "bridge",
                        "building",
                        "country_label_line",
                        "country_label",
                        "landuse_overlay",
                        "landuse",
                        "marine_label",
                        "place_label",
                        "poi_label",
                        "road_label",
                        "road",
                        "state_label",
                        "tunnel",
                        "water_label",
                        "water",
                        "waterway_label",
                        "waterway");

        assertThat(decoded.getFeatures("poi_label")).hasSize(558);

        Feature park = decoded.getFeatures("poi_label").skip(11).findFirst().orElseThrow();
        assertThat(park.getAttribute("name")).isEqualTo("Mauerpark");
        assertThat(park.getAttribute("type")).isEqualTo("Park");

        Geometry parkGeometry = park.getGeometry();

        assertCoordEquals(
                new Coordinate(3898.0, 1731.0),
                park.getLayer().getExtent(),
                parkGeometry.getCoordinates()[0]);

        Feature building = decoded.getFeatures("building").findFirst().orElseThrow();
        Geometry buildingGeometry = building.getGeometry();
        Geometry expected = geom("POLYGON ((2039 -32, 2035 -31, 2032 -31, 2032 -32, 2039 -32))");
        assertEquals(5, buildingGeometry.getCoordinates().length);
        GeometryAssertions.assertThat(buildingGeometry).equalsExact(expected);
    }

    // SINGLE TILE DECODING GET ONLY GEOMETRY (-Xmx2G):
    // baseline old decoder:            memory diff: 449 MB, time: 23215 ms
    // VectorTilereader					memory diff: 617 MB, time:  9077 ms
    @Test
    void testBigTile() throws IOException, InterruptedException {
        // System.out.println("serialized size: " + decodeResource("/bigtile.vector.pbf").getSerializedSize());
        testBigTile(10);
    }

    private void testBigTile(int iterations) throws IOException {
        System.gc();
        long memoryStart = Runtime.getRuntime().totalMemory();
        Tile tile = decodeClasspathResource("/bigtile.vector.pbf");
        Stopwatch sw = new Stopwatch();
        for (int i = 0; i < iterations; i++) {
            assertThat(tile.getFeatures().peek(f -> {
                        // f.getAttributes();
                        f.getGeometry();
                    }))
                    .hasSize(100_000);
        }
        sw.stop();
        long memoryEnd = Runtime.getRuntime().totalMemory();
        long memoryDiff = memoryEnd - memoryStart;
        System.out.println("Start memory: %d, end memory: %d".formatted(memoryStart, memoryEnd));
        System.out.println("memory diff: " + memoryDiff / (1024 * 1024) + " MB, time: " + sw.getTime() + " ms");
    }

    @Test
    void testLineWithOnePoint() throws IOException {
        Tile tile = decodeClasspathResource("/cells-11-1065-567.mvt");
        assertThat(tile.getFeatures()).hasSize(306);
        List<Feature> list = tile.getFeatures().toList();
        for (int i = 0; i < list.size(); i++) {
            Feature f = list.get(i);
            Geometry geometry = f.getGeometry();
            assertThat(f.getAttributes()).isNotNull();
            GeometryAssertions.assertThat(geometry).isNotNull();
        }
    }

    @Test
    void testPolygonWithThreePointHole() throws IOException {
        Tile tile = decodeClasspathResource("/cells-11-1058-568.mvt");
        assertThat(tile.getFeatures()).hasSize(699);
        List<Feature> features = tile.getFeatures().toList();
        for (Feature f : features) {
            Geometry g = f.getGeometry();
            GeometryAssertions.assertThat(g).isNotNull();
        }
    }

    @Test
    void testMixedValidAndInvalidGeometries() throws IOException {
        // Test case to trigger SubCoordinateSequence fallback when invalid geometries are discarded
        String layerName = "test";
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(extent);

        // Add a degenerate polygon (zero area - should be discarded)
        Tile built = builder.layer()
                .name(layerName)
                .feature(Map.of(), geom("POLYGON((5 5, 5 5, 5 5, 5 5))"))
                // Add a valid polygon
                .feature(Map.of(), geom("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"))
                // Add a valid linestring
                .feature(Map.of(), geom("LINESTRING(0 0, 10 10, 20 0)"))
                // Add a single-point linestring (should be discarded)
                .feature(Map.of(), geom("LINESTRING(15 15, 15 15)"))
                // Add another valid polygon
                .feature(Map.of(), geom("POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))"))
                // build the layer
                .build()
                // build the tile
                .build();

        byte[] data = codec.encode(built);
        Tile tile = codec.decode(data);

        List<Feature> features = tile.getFeatures().toList();

        // Should only have 3 valid features (2 polygons + 1 linestring)
        // The degenerate polygon and single-point linestring should be discarded during decoding
        assertThat(features).hasSize(3);

        for (Feature f : features) {
            Geometry g = f.getGeometry();
            assertNotNull(g);
            assertFalse(g.isEmpty());
        }
    }

    private void assertCoordEquals(Coordinate expected, int extent, Coordinate actual) {
        assertEquals(expected.x, actual.x, 1e-7);
        assertEquals(expected.y, actual.y, 1e-7);
    }

    private void assertPropsEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertThat(actual.keySet()).isEqualTo(expected.keySet());

        for (Map.Entry<String, Object> e : expected.entrySet()) {
            String key = e.getKey();
            Object expectedValue = e.getValue();
            Object realValue = actual.get(key);

            if (expectedValue instanceof Number exp) {
                assertThat(realValue).isInstanceOf(Number.class);

                Number rea = (Number) realValue;
                assertEquals(exp.intValue(), rea.intValue());
                assertEquals(exp.floatValue(), rea.floatValue(), 0.003);
                assertEquals(exp.doubleValue(), rea.doubleValue(), 0.003);
            } else {
                assertEquals(expectedValue.getClass(), realValue.getClass());
                assertEquals(expectedValue, realValue);
            }
        }
    }

    private Tile decodeClasspathResource(String resource) throws IOException {
        try (InputStream is = Objects.requireNonNull(getClass().getResourceAsStream(resource))) {
            return codec.decode(is);
        }
    }

    private Geometry geom(String wkt) {
        try {
            return new WKTReader(gf).read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private Feature testFeatureRoundTrip(Geometry geometry, Map<String, Object> attributes) throws IOException {
        return testFeatureRoundTrip(geometry, geometry, attributes);
    }

    private Feature testFeatureRoundTrip(Geometry geometry, Geometry expectedGeometry, Map<String, Object> attributes)
            throws IOException {
        String layerName = "layer";
        byte[] encoded = encode(geometry, attributes, layerName);

        Tile decoded = decode(encoded);

        assertEquals(Set.of(layerName), decoded.getLayerNames());

        // Apply coordinate transformation if needed for round-trip testing
        Feature f = decoded.getLayer(layerName)
                .orElseThrow()
                .getFeatures()
                .findFirst()
                .orElseThrow();

        assertPropsEquals(attributes, f.getAttributes());
        Geometry actualGeometry = f.getGeometry();
        GeometryAssertions.assertThat(actualGeometry).equalsNorm(expectedGeometry);
        return f;
    }

    private Tile decode(byte[] encoded) throws IOException {
        return codec.decode(encoded);
    }

    private byte[] encode(Geometry geometry, Map<String, Object> attributes, String layerName) {
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(extent);
        Tile tile = builder.layer()
                .name(layerName)
                .feature()
                .geometry(geometry)
                .attributes(attributes)
                .build()
                .build()
                .build();
        // Feature f =
        //        tile.getLayer(layerName).orElseThrow().getFeatures().findFirst().orElseThrow();
        // System.err.printf("input:%n\t%s%nencoded:%n%s%n", geometry, f);
        return codec.encode(tile);
    }

    public static void main(String... args) throws IOException {
        int iterations = 1000;
        System.out.println(
                "Running %s.testBigTile(%d)".formatted(VectorTileCodecTest.class.getSimpleName(), iterations));
        new VectorTileCodecTest().testBigTile(iterations);
    }
}
