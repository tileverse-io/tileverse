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

import static io.tileverse.vectortile.mvt.ProtobufReference.parse;
import static io.tileverse.vectortile.mvt.ProtobufReference.read;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.tileverse.vectortile.model.GeometryReader;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.mvt.ProtobufReference.TileContent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.util.Stopwatch;

@Slf4j
class VectorTileCodecTest {

    // Field numbers of the vector tile schema, for the tiles written with ProtoWriter
    private static final int TILE_LAYER = 3;
    private static final int LAYER_NAME = 1;
    private static final int LAYER_FEATURE = 2;
    private static final int LAYER_KEY = 3;
    private static final int LAYER_VALUE = 4;
    private static final int LAYER_VERSION = 15;
    private static final int FEATURE_ID = 1;
    private static final int FEATURE_TAGS = 2;
    private static final int FEATURE_TYPE = 3;
    private static final int FEATURE_GEOMETRY = 4;
    private static final int VALUE_STRING = 1;
    private static final int VALUE_INT = 4;

    /** A field number used by no message of the schema. */
    private static final int UNKNOWN = 9;

    private static final int POINT = 1;
    private static final int LINESTRING = 2;
    private static final int POLYGON = 3;

    // Expected outcomes for the fixtures of mapbox/mvt-fixtures. A fixture in none of these sets decodes, and its
    // features can be read. The fixtures marked invalid that decode anyway are accepted as protobuf accepts them: a
    // field of the wrong wire type is skipped, and ClosePath counts are not checked.

    /** Layers without their required name or version. */
    private static final Set<String> FIXTURES_FAILING_TO_DECODE = Set.of("007", "014", "023", "024", "061");

    /** Tags pointing past the keys or the values of the layer, or an odd number of tags. */
    private static final Set<String> FIXTURES_FAILING_ON_ATTRIBUTES = Set.of("005", "013", "040", "041", "042");

    /**
     * Malformed geometry commands: the geometry decodes as empty, and the strict codec fails to decode the tile. 057 is
     * marked valid, although its MoveTo announces more points than it holds.
     */
    private static final Set<String> FIXTURES_FAILING_ON_GEOMETRY = Set.of("044", "045", "051", "052", "057", "058");

    /**
     * The values written in turn in place of each byte of the tile. As tags, the first five name field 0, each with a
     * wire type of its own: 0 (varint), 3 (start of a group), 4 (end of a group), and the unknown wire types 6 and 7.
     * 0x7F is the largest one-byte varint, and 0x80 and 0xFF continue a varint.
     */
    private static final int[] DAMAGE_BYTES = {0x00, 0x03, 0x04, 0x06, 0x07, 0x7F, 0x80, 0xFF};

    /** Feature with id 1, type LINESTRING, MoveTo(0, 0), then a LineTo of count 536870911 followed by one pair. */
    private static final byte[] FEATURE_WITH_COUNT_PAST_END =
            bytes(0x08, 0x01, 0x18, 0x02, 0x22, 0x0A, 0x09, 0x00, 0x00, 0xFA, 0xFF, 0xFF, 0xFF, 0x0F, 0x0C, 0x14);

    private VectorTileCodec codec = new VectorTileCodec();

    private final VectorTileCodec strictCodec = VectorTileCodec.failingOnMalformedGeometry();

    private int extent = 4096;

    @Test
    void testCustomGeometryFactory() throws IOException {
        // Create a tile with test geometries using default codec
        Geometry point = geom("POINT(100 200)");
        Geometry line = geom("LINESTRING(0 0, 50 50, 100 0)");

        VectorTileBuilder builder = new VectorTileBuilder().setExtent(4096);
        VectorTile tile = builder.layer()
                .name("test_layer")
                .feature(Map.of("type", "point"), point)
                .feature(Map.of("type", "line"), line)
                .build()
                .build();

        byte[] encoded = codec.encode(tile);

        // Test 1: Default codec uses PackedCoordinateSequenceFactory
        VectorTileCodec defaultCodec = new VectorTileCodec();
        VectorTile decoded = defaultCodec.decode(encoded);

        Feature defaultPointFeature =
                decoded.getFeatures("test_layer").findFirst().orElseThrow();
        Geometry defaultGeometry = defaultPointFeature.getGeometry();

        // Verify default uses PackedCoordinateSequenceFactory
        assertThat(defaultGeometry.getFactory()).isSameAs(GeometryDecoder.DEFAULT_GEOMETRY_FACTORY);

        // Test 2: Custom GeometryReader with CoordinateArraySequenceFactory
        GeometryFactory customFactory = new GeometryFactory(CoordinateArraySequenceFactory.instance());
        GeometryReader geometryReader = VectorTileCodec.newGeometryReader().withGeometryFactory(customFactory);

        Predicate<Feature> filter = f -> true;
        Feature customPointFeature = decoded.getFeatures("test_layer", filter, geometryReader)
                .findFirst()
                .orElseThrow();
        Geometry customGeometry = customPointFeature.getGeometry();

        // Verify custom uses CoordinateArraySequenceFactory
        assertThat(customGeometry.getFactory()).isSameAs(customFactory);

        assertThat(customGeometry.getFactory().getCoordinateSequenceFactory())
                .isInstanceOf(CoordinateArraySequenceFactory.class);

        // Verify coordinates are identical despite different factories
        assertThat(customGeometry.getCoordinate()).isEqualTo(defaultGeometry.getCoordinate());

        // Test LineString as well to verify factory is used for all geometry types
        Feature customLineFeature = decoded.getFeatures("test_layer", filter, geometryReader)
                .skip(1)
                .findFirst()
                .orElseThrow();
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
        Geometry geom = geom("MULTILINESTRING EMPTY");
        testFeatureRoundTrip(geom, null, Map.of());

        geom = geom("MULTILINESTRING((1 2, 5 6, 7 8))");
        testFeatureRoundTrip(geom, geom("LINESTRING(1 2, 5 6, 7 8)"), Map.of());

        geom = geom("MULTILINESTRING((1 2, 5 6, 7 8), (8 10, 11 12))");
        testFeatureRoundTrip(geom, Map.of());
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
        VectorTile decoded = decodeClasspathResource("/14-8801-5371.vector.pbf");

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

        assertCoordEquals(new Coordinate(3898.0, 1731.0), parkGeometry.getCoordinates()[0]);

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
    void testBigTile() throws IOException {
        testBigTile(10);
    }

    private void testBigTile(int iterations) throws IOException {
        System.gc();
        long memoryStart = Runtime.getRuntime().totalMemory();
        VectorTile tile = decodeClasspathResource("/bigtile.vector.pbf");
        Stopwatch sw = new Stopwatch();
        for (int i = 0; i < iterations; i++) {
            assertThat(tile.getFeatures().peek(Feature::getGeometry)).hasSize(100_000);
        }
        sw.stop();
        long memoryEnd = Runtime.getRuntime().totalMemory();
        long memoryDiff = memoryEnd - memoryStart;
        log.debug("Start memory: {}, end memory: {}", memoryStart, memoryEnd);
        log.debug("memory diff: {} MB, time: {} ms", memoryDiff / (1024 * 1024), sw.getTime());
    }

    @Test
    void testLineWithOnePoint() throws IOException {
        VectorTile tile = decodeClasspathResource("/cells-11-1065-567.mvt");
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
        VectorTile tile = decodeClasspathResource("/cells-11-1058-568.mvt");
        assertThat(tile.getFeatures()).hasSize(699);
        List<Feature> features = tile.getFeatures().toList();
        for (Feature f : features) {
            Geometry g = f.getGeometry();
            GeometryAssertions.assertThat(g).isNotNull();
        }
    }

    // java-vector-tile 1.4.2 builds as many coordinates: it skips the four rings of zero area of the tile too
    @Test
    void skipsZeroAreaRingsOfCells() throws IOException {
        VectorTile tile = decodeClasspathResource("/cells-11-1058-568.mvt");

        int coordinates =
                tile.getFeatures().mapToInt(f -> f.getGeometry().getNumPoints()).sum();

        assertThat(coordinates).isEqualTo(36_689);
    }

    @Test
    void testMixedValidAndInvalidGeometries() throws IOException {
        // Test case to trigger SubCoordinateSequence fallback when invalid geometries are discarded
        String layerName = "test";
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(extent);

        // Add a degenerate polygon (zero area - should be discarded)
        VectorTile built = builder.layer()
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
        VectorTile tile = codec.decode(data);

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

    // Tiles written by VersaTiles hold features with a zero-length packed geometry field
    @ParameterizedTest
    @MethodSource("decodePaths")
    void testEmptyGeometry(TileDecoder decoder) throws IOException {
        // id 1, type POLYGON, zero-length geometry
        byte[] feature = {0x08, 0x01, 0x18, 0x03, 0x22, 0x00};

        VectorTile tile = decoder.decode(codec, tileWithFeature(feature));

        Feature decoded = tile.getFeatures("x").findFirst().orElseThrow();
        assertThat(decoded.getId()).isEqualTo(1L);
        GeometryAssertions.assertThat(decoded.getGeometry()).isPolygon().isEmpty();
    }

    @ParameterizedTest
    @MethodSource("decodePaths")
    void testEmptyTags(TileDecoder decoder) throws IOException {
        // id 1, zero-length tags, type POINT, MoveTo(2, 3)
        byte[] feature = {0x08, 0x01, 0x12, 0x00, 0x18, 0x01, 0x22, 0x03, 0x09, 0x04, 0x06};

        VectorTile tile = decoder.decode(codec, tileWithFeature(feature));

        Feature decoded = tile.getFeatures("x").findFirst().orElseThrow();
        assertThat(decoded.getAttributes()).isEmpty();
        GeometryAssertions.assertThat(decoded.getGeometry()).equalsExact("POINT (2 3)");
    }

    @Test
    void testCommandCountPastEnd() {
        byte[] tile = tileWithFeature(FEATURE_WITH_COUNT_PAST_END);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            Feature decoded = onlyFeature(tile);
            GeometryAssertions.assertThat(decoded.getGeometry()).isLineString().isEmpty();
            assertThatThrownBy(() -> strictCodec.decode(tile)).isInstanceOf(IOException.class);
        });
    }

    @Test
    void testToStringOfMalformedFeature() throws IOException {
        VectorTile tile = codec.decode(tileWithFeature(FEATURE_WITH_COUNT_PAST_END));

        Feature decoded = tile.getFeatures("x").findFirst().orElseThrow();
        assertThat(decoded.toString()).contains("LineTo, Count: 536870911", "Point 1: (6, 10)");
    }

    @Test
    void malformedGeometryIsEmpty() throws IOException {
        for (String id : FIXTURES_FAILING_ON_GEOMETRY) {
            byte[] tile = MvtFixture.load(id).tile();

            VectorTile decoded = codec.decode(tile);

            assertThat(read(decoded)).as(id).isEqualTo(parse(tile));
            decoded.getFeatures().forEach(VectorTileCodecTest::assertEmptyGeometryOfItsType);
        }
    }

    @Test
    void strictCodecRejectsMalformed() {
        for (String id : FIXTURES_FAILING_ON_GEOMETRY) {
            byte[] tile = MvtFixture.load(id).tile();

            assertThatThrownBy(() -> strictCodec.decode(tile))
                    .as(id)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("feature 0 of layer hello");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unclosedRings")
    void unclosedRingIsMalformed(int[][] commands) throws IOException {
        byte[] tile = tileWithGeometry(POLYGON, commands);

        Geometry geometry = onlyFeature(tile).getGeometry();

        GeometryAssertions.assertThat(geometry).isPolygon().isEmpty();
        assertThatThrownBy(() -> strictCodec.decode(tile))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("feature 0 of layer x");
    }

    /** Polygons with a ring of 3 points or more and no ClosePath: four points, or zero area after a closed square. */
    static Stream<Named<int[][]>> unclosedRings() {
        int[][] fourPoints = {moveTo(1), deltas(0, 0), lineTo(3), deltas(10, 0, 0, 10, -10, 0)};
        int[][] zeroAreaAfterSquare = {
            moveTo(1),
            deltas(0, 0),
            lineTo(3),
            deltas(10, 0, 0, 10, -10, 0),
            closePath(),
            moveTo(1),
            deltas(30, 20),
            lineTo(2),
            deltas(5, 0, 5, 0)
        };
        return Stream.of(Named.of("four points", fourPoints), Named.of("zero area", zeroAreaAfterSquare));
    }

    @Test
    void keepsLineWithoutSkippedPart() throws IOException {
        byte[] tile = tileWithGeometry(
                LINESTRING, moveTo(1), deltas(0, 0), lineTo(2), deltas(10, 0, 0, 10), moveTo(1), deltas(5, 5));

        Geometry geometry = onlyFeature(tile).getGeometry();

        GeometryAssertions.assertThat(geometry).equalsExact("LINESTRING (0 0, 10 0, 10 10)");
    }

    // The second ring holds two points: the point of its MoveTo, and the same point again for its ClosePath
    @Test
    void keepsPolygonWithoutSkippedRing() throws IOException {
        byte[] tile = tileWithGeometry(
                POLYGON,
                moveTo(1),
                deltas(0, 0),
                lineTo(3),
                deltas(10, 0, 0, 10, -10, 0),
                closePath(),
                moveTo(1),
                deltas(20, 10),
                closePath());

        Geometry geometry = onlyFeature(tile).getGeometry();

        GeometryAssertions.assertThat(geometry).equalsExact("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    }

    // The deltas of both tiles take three-byte varints, and the tiles have the same size
    @Test
    void storesLargeCoordinatesAsInts() throws IOException {
        byte[] large = tileWithGeometry(POINT, moveTo(2), deltas(40000, 5, -80000, 0));
        byte[] small = tileWithGeometry(POINT, moveTo(2), deltas(20000, 5, -40000, 0));

        int largeHeapSize = codec.decode(large).heapSize().orElseThrow();
        int smallHeapSize = codec.decode(small).heapSize().orElseThrow();

        GeometryAssertions.assertThat(onlyFeature(large).getGeometry())
                .equalsExact("MULTIPOINT ((40000 5), (-40000 5))");
        long intsOverShorts = HeapSize.array(Integer.BYTES, 4) - HeapSize.array(Short.BYTES, 4);
        assertThat(large).hasSameSizeAs(small);
        assertThat(largeHeapSize - smallHeapSize).isEqualTo(intsOverShorts);
    }

    /** Checks that the geometry is empty, and of the geometry type of the feature: a collection for UNKNOWN. */
    private static void assertEmptyGeometryOfItsType(Feature feature) {
        Geometry geometry = feature.getGeometry();
        String expectedType =
                switch (((MvtFeature) feature).geometryType()) {
                    case POINT -> Geometry.TYPENAME_POINT;
                    case LINESTRING -> Geometry.TYPENAME_LINESTRING;
                    case POLYGON -> Geometry.TYPENAME_POLYGON;
                    default -> Geometry.TYPENAME_GEOMETRYCOLLECTION;
                };
        assertThat(geometry.isEmpty()).as(feature.toString()).isTrue();
        assertThat(geometry.getGeometryType()).isEqualTo(expectedType);
    }

    /** Wraps a feature with id 1, the type and the geometry commands in layer "x". */
    private static byte[] tileWithGeometry(int type, int[]... commands) {
        int[] geometry = Arrays.stream(commands).flatMapToInt(Arrays::stream).toArray();
        ProtoWriter feature = new ProtoWriter()
                .varintField(FEATURE_ID, 1)
                .varintField(FEATURE_TYPE, type)
                .packedField(FEATURE_GEOMETRY, geometry);
        return tileWithFeature(feature.toByteArray());
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

    /** The parameters of the points of a command: the zigzag encoding of each delta. */
    private static int[] deltas(int... deltas) {
        return Arrays.stream(deltas).map(delta -> (delta << 1) ^ (delta >> 31)).toArray();
    }

    @Test
    void testLayersInTileOrder() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(extent);
        for (String name : List.of("c", "b", "a")) {
            builder.layer().name(name).feature(Map.of(), geom("POINT (1 1)")).build();
        }

        VectorTile tile = codec.decode(codec.encode(builder.build()));

        assertThat(tile.getLayerNames()).containsExactly("c", "b", "a");
        assertThat(tile.getLayers()).extracting(VectorTile.Layer::getName).containsExactly("c", "b", "a");
    }

    @Test
    void testLayersWithSameName() throws IOException {
        VectorTile tile = codec.decode(MvtFixture.load("015").tile());

        List<VectorTile.Layer> layers = tile.getLayers();

        assertThat(layers).extracting(VectorTile.Layer::getName).containsExactly("hello", "hello");
        assertThat(firstFeature(layers.get(0)).getAttribute("name")).isEqualTo("layer-one");
        assertThat(firstFeature(layers.get(1)).getAttribute("name")).isEqualTo("layer-two");
    }

    @ParameterizedTest
    @MethodSource("moduleTiles")
    void decodesLikeProtobuf(byte[] tile) throws IOException {
        assertThat(read(codec.decode(tile))).isEqualTo(parse(tile));
    }

    static Stream<Named<byte[]>> moduleTiles() throws IOException {
        return Stream.of(
                resourceTile("14-8801-5371.vector.pbf"),
                resourceTile("bigtile.vector.pbf"),
                resourceTile("cells-11-1058-568.mvt"),
                resourceTile("cells-11-1065-567.mvt"),
                Named.of("points", pointsTile()),
                Named.of("lines", linesTile()),
                Named.of("polygons with holes", polygonsWithHolesTile()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mvtFixtures")
    void fixturesDecodeLikeProtobuf(MvtFixture fixture) {
        assertSameOutcome(fixture.tile(), "fixture " + fixture.id());
    }

    // Each prefix of the tile, from no byte up to all bytes but the last
    @Test
    void truncatedTilesMatchProtobuf() {
        byte[] tile = sweptTile();

        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            for (int length = 0; length < tile.length; length++) {
                byte[] prefix = Arrays.copyOf(tile, length);
                assertSameOutcome(prefix, "prefix of " + length + " bytes");
            }
        });
    }

    @Test
    void damagedTilesMatchProtobuf() {
        byte[] tile = sweptTile();

        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            for (int offset = 0; offset < tile.length; offset++) {
                for (int value : DAMAGE_BYTES) {
                    byte[] damaged = tile.clone();
                    damaged[offset] = (byte) value;
                    assertSameOutcome(damaged, "byte %d replaced by 0x%02X".formatted(offset, value));
                }
            }
        });
    }

    /** Two layers of three features each, with string, integer, float, double and boolean values. */
    private static byte[] sweptTile() {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder pois = builder.layer().name("pois");
        addFeature(pois, 1, "POINT (10 20)", "name", "cafe", "rank", 1, "score", 4.5f, "open", true);
        addFeature(pois, 2, "POINT (30 40)", "name", "bar", "rank", -2, "price", 12.25);
        addFeature(pois, 300, "MULTIPOINT ((1 2), (3 4))", "name", "atm", "open", false);
        pois.build();
        LayerBuilder roads = builder.layer().name("roads");
        addFeature(roads, 4, "LINESTRING (0 0, 10 10, 20 0)", "class", "primary", "lanes", 2, "speed", 50.5);
        addFeature(roads, 5, "MULTILINESTRING ((0 0, 5 5), (10 10, 15 15, 20 10))", "class", "rail", "bridge", true);
        addFeature(roads, 6, "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0))", "class", "park", "area", 1.5f);
        roads.build();
        return new VectorTileCodec().encode(builder.build());
    }

    /**
     * Adds a feature with its attributes in the order given, each key followed by its value. The order of Map.of
     * changes between runs, and the swept tile must keep the same bytes.
     */
    private static void addFeature(LayerBuilder layer, long id, String wkt, Object... keysAndValues) {
        FeatureBuilder feature = layer.feature().id(id).geometry(geom(wkt));
        for (int i = 0; i < keysAndValues.length; i += 2) {
            feature.attribute((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        feature.build();
    }

    private void assertSameOutcome(byte[] tile, String description) {
        Object expected = referenceOutcome(tile);

        assertThat(outcome(tile)).as(description).isEqualTo(expected);
    }

    /** The content of the tile read by protobuf's parser, or "IOException" when parsing throws one. */
    private static Object referenceOutcome(byte[] tile) {
        try {
            return parse(tile);
        } catch (IOException e) {
            return "IOException";
        }
    }

    /**
     * The content of the tile read through the model, or "IOException" when decoding throws one. Protobuf throws no
     * other exception. Another exception of the decoder makes a mismatch, and the outcome names it with its message.
     */
    private Object outcome(byte[] tile) {
        try {
            return read(codec.decode(tile));
        } catch (IOException e) {
            return "IOException";
        } catch (RuntimeException e) {
            return e.toString();
        }
    }

    // Single varints are the form of a repeated field written without packing
    @Test
    void testUnpackedTagsAndGeometry() throws IOException {
        ProtoWriter feature = new ProtoWriter()
                .varintField(FEATURE_ID, 7)
                .varintField(FEATURE_TAGS, 0)
                .varintField(FEATURE_TAGS, 0)
                .varintField(FEATURE_TAGS, 1)
                .varintField(FEATURE_TAGS, 1)
                .varintField(FEATURE_TYPE, POINT)
                .varintField(FEATURE_GEOMETRY, 9)
                .varintField(FEATURE_GEOMETRY, 50)
                .varintField(FEATURE_GEOMETRY, 34);

        assertDecodesTaggedPoint(tileWithTaggedFeature(feature));
    }

    // The first geometry run ends between the two parameters of the MoveTo command
    @Test
    void testSplitPackedFields() throws IOException {
        ProtoWriter feature = new ProtoWriter()
                .varintField(FEATURE_ID, 7)
                .packedField(FEATURE_TAGS, 0, 0)
                .packedField(FEATURE_TAGS, 1, 1)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 50)
                .packedField(FEATURE_GEOMETRY, 34);

        assertDecodesTaggedPoint(tileWithTaggedFeature(feature));
    }

    // An empty run comes before the tags, and another after the geometry commands
    @Test
    void testEmptyRunsInSplitFields() throws IOException {
        ProtoWriter feature = new ProtoWriter()
                .varintField(FEATURE_ID, 7)
                .packedField(FEATURE_TAGS)
                .packedField(FEATURE_TAGS, 0, 0, 1, 1)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 50, 34)
                .packedField(FEATURE_GEOMETRY);

        assertDecodesTaggedPoint(tileWithTaggedFeature(feature));
    }

    @Test
    void testFeatureFieldsOutOfOrder() throws IOException {
        ProtoWriter feature = new ProtoWriter()
                .packedField(FEATURE_GEOMETRY, 9, 50, 34)
                .packedField(FEATURE_TAGS, 0, 0, 1, 1)
                .varintField(FEATURE_ID, 7)
                .varintField(FEATURE_TYPE, POINT);

        assertDecodesTaggedPoint(tileWithTaggedFeature(feature));
    }

    @Test
    void testUnknownFieldsInFeature() throws IOException {
        ProtoWriter feature = new ProtoWriter();
        writeUnknownFields(feature).varintField(FEATURE_ID, 7);
        writeUnknownFields(feature).packedField(FEATURE_TAGS, 0, 0, 1, 1);
        writeUnknownFields(feature).varintField(FEATURE_TYPE, POINT);
        writeUnknownFields(feature).packedField(FEATURE_GEOMETRY, 9, 50, 34);
        writeUnknownFields(feature);

        assertDecodesTaggedPoint(tileWithTaggedFeature(feature));
    }

    // Protobuf keeps a number outside the GeomType enum as an unknown field
    @Test
    void testInvalidTypeKeepsLast() throws IOException {
        ProtoWriter feature = new ProtoWriter()
                .varintField(FEATURE_TYPE, POINT)
                .varintField(FEATURE_TYPE, 7)
                .packedField(FEATURE_GEOMETRY, 9, 50, 34);
        byte[] tile = tileWithTaggedFeature(feature);

        TileContent decoded = read(codec.decode(tile));

        assertThat(decoded).isEqualTo(parse(tile));
        assertThat(decoded.layers().get(0).features().get(0).type()).isEqualTo(GeomType.POINT);
    }

    @Test
    void testDecodeCopiesArray() throws IOException {
        byte[] input = resource("14-8801-5371.vector.pbf");
        VectorTile tile = codec.decode(input);
        TileContent before = read(tile);

        Arrays.fill(input, (byte) 0);

        assertThat(read(tile)).isEqualTo(before);
    }

    @Test
    void testDecodeCopiesBuffer() throws IOException {
        ByteBuffer input = sliceAtOffset(resource("14-8801-5371.vector.pbf"));
        VectorTile tile = codec.decode(input);
        TileContent before = read(tile);

        Arrays.fill(input.array(), (byte) 0);

        assertThat(read(tile)).isEqualTo(before);
    }

    // Each buffer is decoded a second time, as by a caller reading it again
    @Test
    void testDecodeKeepsBufferPosition() throws IOException {
        byte[] tile = tileWithTaggedFeature(taggedPoint());
        int capacity = tile.length + 5;
        List<ByteBuffer> buffers = List.of(
                padded(ByteBuffer.allocate(capacity), tile),
                padded(ByteBuffer.allocate(capacity), tile).asReadOnlyBuffer(),
                padded(ByteBuffer.allocateDirect(capacity), tile));

        for (ByteBuffer buffer : buffers) {
            codec.decode(buffer);

            assertThat(buffer.position()).as(buffer.toString()).isEqualTo(3);
            assertThat(buffer.limit()).as(buffer.toString()).isEqualTo(3 + tile.length);
            assertThat(read(codec.decode(buffer))).isEqualTo(parse(tile));
        }
    }

    @Test
    void testConcurrentReads() throws Exception {
        VectorTile tile = codec.decode(resource("14-8801-5371.vector.pbf"));
        TileReading expected = readWithGeometries(tile);
        Callable<TileReading> reading = () -> readWithGeometries(tile);

        List<Future<TileReading>> readings;
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try {
            readings = threads.invokeAll(Collections.nCopies(8, reading));
        } finally {
            threads.shutdownNow();
        }

        for (Future<TileReading> concurrentReading : readings) {
            assertThat(concurrentReading.get()).isEqualTo(expected);
        }
    }

    @Test
    void testFeatureLeadsToTileCopy() throws IOException {
        VectorTile tile = codec.decode(tileWithTaggedFeature(taggedPoint()));
        Envelope bounds = new Envelope(0, 4096, 0, 4096);

        VectorTile copy = tile.withBoundingBox(bounds);

        Feature feature = copy.getFeatures().findFirst().orElseThrow();
        assertThat(feature.getLayer().getTile().boundingBox()).isEqualTo(Optional.of(bounds));
        Feature original = tile.getFeatures().findFirst().orElseThrow();
        assertThat(original.getLayer().getTile().boundingBox()).isEmpty();
    }

    @Test
    void testAttributesMapIsMutable() throws IOException {
        Feature feature = onlyFeature(tileWithTaggedFeature(taggedPoint()));

        feature.getAttributes().put("extra", 1);

        assertThat(feature.getAttribute("extra")).isEqualTo(1);
        assertThat(feature.getAttribute("name")).isEqualTo("a");
    }

    // The spy reads the attributes first, from the fields copied by Mockito
    @Test
    void testSpiedFeature() throws IOException {
        Feature feature = onlyFeature(tileWithTaggedFeature(taggedPoint()));

        Feature spied = spy(feature);

        assertThat(spied.getAttributes()).isEqualTo(feature.getAttributes());
    }

    // The layer starts with its version field, and protobuf writes it last, in field number order
    @Test
    void testEncodeReturnsTileBytes() throws IOException {
        // id 1, type POINT, MoveTo(2, 3)
        byte[] bytes = tileWithFeature(new byte[] {0x08, 0x01, 0x18, 0x01, 0x22, 0x03, 0x09, 0x04, 0x06});
        VectorTile tile = codec.decode(bytes);

        byte[] first = codec.encode(tile);
        byte[] second = codec.encode(tile);

        assertThat(first).isEqualTo(bytes).isNotSameAs(second);
        first[0] = 0;
        assertThat(second).isEqualTo(bytes);
        assertThat(codec.encode(tile)).isEqualTo(bytes);
    }

    @Test
    void testTileOwnsItsBytes() throws IOException {
        byte[] bytes = tileWithTaggedFeature(taggedPoint());

        MvtTile tile = (MvtTile) codec.decode(bytes);

        assertThat(tile.bytes()).isEqualTo(bytes).isNotSameAs(bytes);
    }

    @Test
    void testEncodeToStreamAndBuffer() throws IOException {
        byte[] bytes = tileWithTaggedFeature(taggedPoint());
        VectorTile tile = codec.decode(bytes);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 2).position(1);

        codec.encode(tile, stream);
        codec.encode(tile, buffer);

        assertThat(stream.toByteArray()).isEqualTo(bytes);
        assertThat(buffer.position()).isEqualTo(1 + bytes.length);
        assertThat(Arrays.copyOfRange(buffer.array(), 1, 1 + bytes.length)).isEqualTo(bytes);
        assertThat(codec.getSerializedSize(tile)).isEqualTo(bytes.length);
        assertThat(tile.serializedSize()).hasValue(bytes.length);
    }

    @Test
    void testEncodeToSmallBuffer() throws IOException {
        byte[] bytes = tileWithTaggedFeature(taggedPoint());
        VectorTile tile = codec.decode(bytes);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length - 1);

        assertThatThrownBy(() -> codec.encode(tile, buffer))
                .isInstanceOfSatisfying(
                        InsufficientBufferException.class,
                        e -> assertThat(e.getSerializedSize()).isEqualTo(bytes.length));
        assertThat(buffer.position()).isZero();
    }

    @Test
    void testEncodeOtherTileFails() {
        VectorTile other = mock(VectorTile.class);

        assertThatThrownBy(() -> codec.encode(other)).isInstanceOf(UnsupportedOperationException.class);
    }

    // Keys, values and coordinates take memory twice: in the tile bytes, and decoded. The second tile holds more
    // coordinates per byte.
    @Test
    void testHeapSizeOfDecodedTile() throws IOException {
        assertThat(heapSizeRatio("14-8801-5371.vector.pbf")).isBetween(2.5, 2.7);
        assertThat(heapSizeRatio("cells-11-1058-568.mvt")).isBetween(3.0, 3.2);
    }

    /** The estimated heap size of the decoded tile, divided by the size of the tile. */
    private double heapSizeRatio(String resource) throws IOException {
        byte[] tile = resource(resource);
        int heapSize = codec.decode(tile).heapSize().orElseThrow();
        return heapSize / (double) tile.length;
    }

    private static Named<byte[]> resourceTile(String name) throws IOException {
        return Named.of(name, resource(name));
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = Objects.requireNonNull(VectorTileCodecTest.class.getResourceAsStream("/" + name))) {
            return in.readAllBytes();
        }
    }

    /** A layer of points, with an attribute of each type accepted by the builder. */
    private static byte[] pointsTile() {
        Map<String, Object> first = Map.ofEntries(
                entry("string", "a"),
                entry("integer", 1),
                entry("long", 1L << 40),
                entry("float", 1.5f),
                entry("double", 2.25),
                entry("boolean", true),
                entry("decimal", new BigDecimal("3.75")));
        Map<String, Object> second = Map.ofEntries(
                entry("string", "b"),
                entry("integer", -2),
                entry("long", -3L),
                entry("float", -0.5f),
                entry("double", 1e10),
                entry("boolean", false),
                entry("decimal", new BigDecimal("-1.5")));
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("points")
                .feature(first, geom("POINT (10 20)"))
                .feature(second, geom("MULTIPOINT ((1 2), (3 4))"))
                .build();
        return new VectorTileCodec().encode(builder.build());
    }

    private static byte[] linesTile() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("lines")
                .feature(Map.of("class", "road"), geom("LINESTRING (0 0, 10 10, 20 0)"))
                .feature(Map.of("class", "rail"), geom("MULTILINESTRING ((0 0, 5 5), (10 10, 15 15, 20 10))"))
                .build();
        return new VectorTileCodec().encode(builder.build());
    }

    private static byte[] polygonsWithHolesTile() {
        Geometry polygon = geom("POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0), (10 10, 10 20, 20 20, 20 10, 10 10))");
        Geometry multiPolygon = geom("MULTIPOLYGON (((200 200, 300 200, 300 300, 200 300, 200 200),"
                + " (210 210, 210 220, 220 220, 220 210, 210 210)),"
                + " ((400 400, 500 400, 500 500, 400 500, 400 400), (410 410, 410 420, 420 420, 420 410, 410 410)))");
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("polygons")
                .feature(Map.of("rank", 1), polygon)
                .feature(Map.of("rank", 2), multiPolygon)
                .build();
        return new VectorTileCodec().encode(builder.build());
    }

    /** Checks a tile holding the feature of {@link #taggedPoint()}, with its fields written in any form. */
    private void assertDecodesTaggedPoint(byte[] tile) throws IOException {
        VectorTile decoded = codec.decode(tile);

        assertThat(read(decoded)).isEqualTo(parse(tile));
        Feature feature = decoded.getFeatures().findFirst().orElseThrow();
        assertThat(feature.getId()).isEqualTo(7L);
        assertThat(feature.getAttributes()).isEqualTo(Map.of("name", "a", "rank", 1L));
        GeometryAssertions.assertThat(feature.getGeometry()).equalsExact("POINT (25 17)");
    }

    /** Wraps the feature in layer "x", of version 2, with the keys "name" and "rank" and the values "a" and 1. */
    private static byte[] tileWithTaggedFeature(ProtoWriter feature) {
        byte[] stringValue = new ProtoWriter().stringField(VALUE_STRING, "a").toByteArray();
        byte[] intValue = new ProtoWriter().varintField(VALUE_INT, 1).toByteArray();
        ProtoWriter layer = new ProtoWriter()
                .varintField(LAYER_VERSION, 2)
                .stringField(LAYER_NAME, "x")
                .stringField(LAYER_KEY, "name")
                .stringField(LAYER_KEY, "rank")
                .bytesField(LAYER_VALUE, stringValue)
                .bytesField(LAYER_VALUE, intValue)
                .bytesField(LAYER_FEATURE, feature.toByteArray());
        return new ProtoWriter().bytesField(TILE_LAYER, layer.toByteArray()).toByteArray();
    }

    /** Feature with id 7, the tags name=a and rank=1, type POINT and MoveTo(25, 17), in packed fields. */
    private static ProtoWriter taggedPoint() {
        return new ProtoWriter()
                .varintField(FEATURE_ID, 7)
                .packedField(FEATURE_TAGS, 0, 0, 1, 1)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 50, 34);
    }

    /**
     * Writes fields unknown to protobuf: field 9 in each wire type, and the field numbers of a feature with the wrong
     * wire type. The group holds a feature id, and protobuf skips it with the group.
     */
    private static ProtoWriter writeUnknownFields(ProtoWriter writer) {
        return writer.varintField(UNKNOWN, 300)
                .fixed64Field(UNKNOWN, 7L)
                .bytesField(UNKNOWN, new byte[] {1, 2, 3})
                .group(UNKNOWN, new ProtoWriter().varintField(FEATURE_ID, 1).toByteArray())
                .fixed32Field(UNKNOWN, 7)
                .fixed64Field(FEATURE_ID, 8L)
                .fixed32Field(FEATURE_TAGS, 1)
                .bytesField(FEATURE_TYPE, new byte[] {3})
                .fixed64Field(FEATURE_GEOMETRY, 9L);
    }

    /** Writes the tile between three leading and two trailing bytes, and sets the position and the limit around it. */
    private static ByteBuffer padded(ByteBuffer buffer, byte[] tile) {
        buffer.put(new byte[] {1, 2, 3}).put(tile).put(new byte[] {4, 5});
        return buffer.position(3).limit(3 + tile.length);
    }

    /** The content of a tile and the WKT of each geometry, read through the model. */
    private record TileReading(TileContent content, List<String> geometries) {}

    private static TileReading readWithGeometries(VectorTile tile) {
        List<String> geometries = tile.getFeatures()
                .map(feature -> feature.getGeometry().toText())
                .toList();
        return new TileReading(read(tile), geometries);
    }

    private Feature onlyFeature(byte[] tile) throws IOException {
        return codec.decode(tile).getFeatures().findFirst().orElseThrow();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mvtFixtures")
    void conformsToMvtFixtures(MvtFixture fixture) {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertOutcome(fixture));
    }

    static Stream<MvtFixture> mvtFixtures() {
        return MvtFixture.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("wellFormedFixtures")
    void strictCodecAcceptsWellFormed(MvtFixture fixture) throws IOException {
        byte[] tile = fixture.tile();

        List<String> strictGeometries = geometries(strictCodec.decode(tile));

        assertThat(strictGeometries).isEqualTo(geometries(codec.decode(tile)));
    }

    /** The fixtures decoded by protobuf's parser, without a malformed geometry. */
    static Stream<MvtFixture> wellFormedFixtures() {
        return MvtFixture.all()
                .filter(fixture -> !FIXTURES_FAILING_TO_DECODE.contains(fixture.id()))
                .filter(fixture -> !FIXTURES_FAILING_ON_GEOMETRY.contains(fixture.id()));
    }

    private static List<String> geometries(VectorTile tile) {
        return tile.getFeatures().map(feature -> feature.getGeometry().toText()).toList();
    }

    private void assertOutcome(MvtFixture fixture) throws IOException {
        String id = fixture.id();
        if (FIXTURES_FAILING_TO_DECODE.contains(id)) {
            assertThatThrownBy(() -> codec.decode(fixture.tile())).isInstanceOf(IOException.class);
            return;
        }
        VectorTile tile = codec.decode(fixture.tile());
        if (FIXTURES_FAILING_ON_ATTRIBUTES.contains(id)) {
            assertThatThrownBy(() -> readAttributes(tile)).isInstanceOf(RuntimeException.class);
        } else if (FIXTURES_FAILING_ON_GEOMETRY.contains(id)) {
            tile.getFeatures().forEach(VectorTileCodecTest::assertEmptyGeometryOfItsType);
            assertThatThrownBy(() -> strictCodec.decode(fixture.tile())).isInstanceOf(IOException.class);
        } else if (fixture.valid()) {
            assertContent(tile, fixture.expectedLayers());
        } else {
            readAttributes(tile);
            readGeometries(tile);
        }
    }

    private static void readAttributes(VectorTile tile) {
        tile.getFeatures().forEach(Feature::getAttributes);
    }

    private static void readGeometries(VectorTile tile) {
        tile.getFeatures().forEach(Feature::getGeometry);
    }

    private static void assertContent(VectorTile tile, List<MvtFixture.ExpectedLayer> expected) {
        List<VectorTile.Layer> layers = tile.getLayers();
        assertThat(layers).hasSize(expected.size());
        for (int i = 0; i < layers.size(); i++) {
            assertLayer(layers.get(i), expected.get(i));
        }
    }

    private static void assertLayer(VectorTile.Layer layer, MvtFixture.ExpectedLayer expected) {
        assertThat(layer.getName()).isEqualTo(expected.name());
        assertThat(layer.getExtent()).isEqualTo(expected.extent());
        List<Feature> features = layer.getFeatures().toList();
        assertThat(features).hasSize(expected.features().size());
        for (int i = 0; i < features.size(); i++) {
            assertFeature(features.get(i), expected.features().get(i));
        }
    }

    private static void assertFeature(Feature feature, MvtFixture.ExpectedFeature expected) {
        assertThat(feature.getId()).isEqualTo(expected.id());
        assertThat(feature.getAttributes()).isEqualTo(expected.attributes());
        Geometry geometry = feature.getGeometry();
        assertThat(geometry.getDimension()).isEqualTo(expected.dimension());
        assertThat(geometry.getCoordinates()).containsExactlyElementsOf(expected.coordinates());
    }

    private static Feature firstFeature(VectorTile.Layer layer) {
        return layer.getFeatures().findFirst().orElseThrow();
    }

    private static byte[] bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    @FunctionalInterface
    interface TileDecoder {
        VectorTile decode(VectorTileCodec codec, byte[] tile) throws IOException;
    }

    static Stream<Named<TileDecoder>> decodePaths() {
        return Stream.of(
                Named.of("byte[]", VectorTileCodec::decode),
                Named.of("heap buffer", (codec, tile) -> codec.decode(sliceAtOffset(tile))),
                Named.of(
                        "read-only buffer",
                        (codec, tile) -> codec.decode(ByteBuffer.wrap(tile).asReadOnlyBuffer())),
                Named.of("direct buffer", (codec, tile) -> codec.decode(direct(tile))),
                Named.of("InputStream", (codec, tile) -> codec.decode(new ByteArrayInputStream(tile))));
    }

    // The tile starts past both the array offset and the position of the buffer
    private static ByteBuffer sliceAtOffset(byte[] tile) {
        byte[] padded = new byte[tile.length + 3];
        System.arraycopy(tile, 0, padded, 3, tile.length);
        return ByteBuffer.wrap(padded).position(1).slice().position(2);
    }

    private static ByteBuffer direct(byte[] tile) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(tile.length);
        buffer.put(tile).flip();
        return buffer;
    }

    /** Wraps the feature in layer "x", of version 2 and extent 4096, and the layer in a tile. */
    private static byte[] tileWithFeature(byte[] feature) {
        ByteArrayOutputStream layer = new ByteArrayOutputStream();
        layer.writeBytes(new byte[] {0x78, 0x02}); // version 2
        layer.writeBytes(new byte[] {0x0A, 0x01, 'x'}); // name "x"
        layer.write(0x12); // feature, then its length
        layer.write(feature.length);
        layer.writeBytes(feature);
        layer.writeBytes(new byte[] {0x28, (byte) 0x80, 0x20}); // extent 4096
        ByteArrayOutputStream tile = new ByteArrayOutputStream();
        tile.write(0x1A); // layer, then its length
        tile.write(layer.size());
        tile.writeBytes(layer.toByteArray());
        return tile.toByteArray();
    }

    private void assertCoordEquals(Coordinate expected, Coordinate actual) {
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

    private VectorTile decodeClasspathResource(String resource) throws IOException {
        try (InputStream is = Objects.requireNonNull(getClass().getResourceAsStream(resource))) {
            return codec.decode(is);
        }
    }

    private static Geometry geom(String wkt) {
        try {
            return new WKTReader().read(wkt);
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

        VectorTile decoded = decode(encoded);

        if (expectedGeometry == null) {
            assertThat(decoded.getLayers()).isEmpty();
            return null;
        }
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

    private VectorTile decode(byte[] encoded) throws IOException {
        return codec.decode(encoded);
    }

    private byte[] encode(Geometry geometry, Map<String, Object> attributes, String layerName) {
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(extent);
        VectorTile tile = builder.layer()
                .name(layerName)
                .feature()
                .geometry(geometry)
                .attributes(attributes)
                .build()
                .build()
                .build();
        return codec.encode(tile);
    }

    public static void main(String... args) throws IOException {
        int iterations = 1000;
        log.debug("Running {}.testBigTile({})", VectorTileCodecTest.class.getSimpleName(), iterations);
        new VectorTileCodecTest().testBigTile(iterations);
    }
}
