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

import static io.tileverse.vectortile.mvt.TileAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.model.VectorTile.Layer;
import io.tileverse.vectortile.model.VectorTile.Layer.Feature;
import io.tileverse.vectortile.pipeline.ClipStep;
import io.tileverse.vectortile.pipeline.GeometryPipeline;
import io.tileverse.vectortile.pipeline.GeometryStep;
import io.tileverse.vectortile.pipeline.GridSnapStep;
import io.tileverse.vectortile.pipeline.PolygonRepairStep;
import io.tileverse.vectortile.pipeline.SimplificationStep;
import io.tileverse.vectortile.pipeline.SizeFilterStep;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class VectorTileBuilderTest {

    // Field numbers of the vector tile schema, for the tiles written with ProtoWriter
    private static final int TILE_LAYER = 3;
    private static final int LAYER_NAME = 1;
    private static final int LAYER_FEATURE = 2;
    private static final int LAYER_KEY = 3;
    private static final int LAYER_VALUE = 4;
    private static final int LAYER_EXTENT = 5;
    private static final int LAYER_VERSION = 15;
    private static final int FEATURE_ID = 1;
    private static final int FEATURE_TAGS = 2;
    private static final int FEATURE_TYPE = 3;
    private static final int FEATURE_GEOMETRY = 4;
    private static final int VALUE_UINT = 5;
    private static final int VALUE_SINT = 6;

    private static final int POINT = 1;
    private static final int LINESTRING = 2;
    private static final int POLYGON = 3;

    private GeometryFactory gf = new GeometryFactory();

    private Geometry geom(String wkt) {
        try {
            return new WKTReader(gf).read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private VectorTile build(String name, int extent, Geometry... geometries) {
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(extent);
        LayerBuilder layer = builder.layer().name(name);
        Map<String, Object> attributes = Map.of();
        for (Geometry geom : geometries) {
            layer.feature(attributes, geom);
        }
        return layer.build().build();
    }

    @Test
    void testEncode() {
        Geometry geometry = geom("LINESTRING(3 6, 8 12, 20 34)");
        Geometry geometry2 = geom("LINESTRING(3 6, 8 12, 20 34, 33 72)");

        VectorTile tile = build("DEPCNT", 256, geometry, geometry2);

        // Test with the new hierarchical assertion API
        assertThat(tile)
                .layer("DEPCNT")
                .hasFeatureCount(2)
                .feature(0)
                .geometry()
                .isLineString()
                .moveTo(3, 6)
                .lineTo(8, 12)
                .lineTo(20, 34)
                .matches()
                .layer()
                .feature(1)
                .geometry()
                .isLineString()
                .moveTo(3, 6)
                .lineTo(8, 12)
                .lineTo(20, 34)
                .lineTo(33, 72)
                .matches();
    }

    @Test
    void testPointEncode() {
        Geometry point = geom("POINT(5 7)");
        Geometry multiPoint = geom("MULTIPOINT((1 2), (3 4))");

        VectorTile tile = build("POINTS", 256, point, multiPoint);

        assertThat(tile)
                .layer("POINTS")
                .hasFeatureCount(2)
                .feature(0)
                .geometry()
                .isPoint()
                .moveTo(5, 7)
                .matches()
                .layer()
                .feature(1)
                .geometry()
                .isMultiPoint()
                .moveTo(1, 2)
                .moveTo(3, 4)
                .matches();
    }

    @Test
    @SuppressWarnings(
            "java:S5853") // Two assertThat(tile) chains navigate into distinct layers ("roads" then "buildings");
    // joining them would require backtracking the fluent assertion off one layer to start a sibling chain on the
    // other, which the AssertJ API here does not support.
    void testComprehensiveAssertions() {
        // Create a tile with multiple layers and features
        VectorTileBuilder vtm = new VectorTileBuilder().setExtent(4096);

        VectorTile tile = vtm.layer()
                .name("roads")
                .feature(Map.of("highway", "primary", "name", "Main St"), geom("LINESTRING(0 0, 1600 1600)"))
                .feature(Map.of("highway", "secondary"), geom("LINESTRING(50 0, 150 50)"))
                .build()
                .layer()
                .name("buildings")
                .feature(Map.of("type", "residential"), geom("POLYGON((160 160, 320 160, 320 320, 160 320, 160 160))"))
                .build()
                .build();

        // Test tile-level assertions
        assertThat(tile)
                .hasLayerCount(2)
                .hasExactLayers("roads", "buildings")
                .layer("roads")
                .hasName("roads")
                .hasExtent(4096)
                .hasFeatureCount(2)
                .hasAttributes("highway", "name")
                .firstFeature()
                .hasAttribute("highway", "primary")
                .hasAttribute("name", "Main St")
                .geometry()
                .isLineString()
                .moveTo(0, 0)
                .lineTo(1600, 1600)
                .matches();

        // Test individual layer
        assertThat(tile)
                .layer("buildings")
                .hasFeatureCount(1)
                .hasAttribute("type")
                .firstFeature()
                .hasAttribute("type", "residential")
                .geometry()
                .isPolygon()
                .isValid()
                .moveTo(160, 160)
                .lineTo(320, 160)
                .lineTo(320, 320)
                .lineTo(160, 320)
                .closePath()
                .matches();
    }

    @Test
    void testLinestringCommands() {
        // Test the specific LINESTRING from the MVT spec example
        Geometry geometry = geom("LINESTRING(3 6, 8 12, 20 34)");

        VectorTile tile = build("COMMANDS", 256, geometry);

        assertThat(tile)
                .layer("COMMANDS")
                .firstFeature()
                .geometry()
                .isLineString()
                .moveTo(3, 6)
                .lineTo(8, 12)
                .lineTo(20, 34)
                .matches();
    }

    @Test
    void testMultiPointCommands() {
        // Test MULTIPOINT encoding as single MoveTo with count=2
        Geometry multiPoint = geom("MULTIPOINT((5 7), (3 2))");

        VectorTile tile = build("MULTIPOINT", 256, multiPoint);

        // According to MVT spec: MULTIPOINT uses single MoveTo with count > 1
        assertThat(tile)
                .layer("MULTIPOINT")
                .firstFeature()
                .geometry()
                .isMultiPoint()
                .moveTo()
                .count(2)
                .coord(5, 7)
                .coord(3, 2)
                .done()
                .matches();
    }

    @Test
    void testNullAttributeHandling() {
        // Test that null attributes are filtered out during encoding
        Geometry geometry = geom("POINT(3 6)");

        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(256);
        LayerBuilder layer = vtb.layer().name("ATTRS");

        // Add feature with a null attribute
        layer.feature()
                .attribute("key1", "value1")
                .attribute("key2", null)
                .attribute("key3", "value3")
                .geometry(geometry)
                .build();
        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("ATTRS")
                .firstFeature()
                .hasAttribute("key1", "value1")
                .hasAttribute("key3", "value3")
                // Verify that key2 is not present (as it would be filtered out if it were null)
                .doesNotHaveTag("key2")
                // but when working as a layer's feature it returns null
                .hasAttribute("key2", null)
                .geometry()
                .isPoint()
                .moveTo(3, 6)
                .matches();
    }

    @Test
    void testAttributeTypes() {
        // Test various attribute data types
        Geometry geometry = geom("POINT(3 6)");

        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(256);
        LayerBuilder layer = vtb.layer().name("TYPES");

        // Add feature with various attribute types using fluent builder pattern
        layer.feature()
                .geometry(geometry)
                .attribute("string_val", "value1")
                .attribute("int_val", 123)
                .attribute("float_val", 234.1f)
                .attribute("double_val", 567.123d)
                .attribute("long_val", -123L)
                .attribute("bool_true", true)
                .attribute("bool_false", false)
                .build();

        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("TYPES")
                .firstFeature()
                .hasAttribute("string_val", "value1")
                .hasAttribute("int_val", 123L) // MVT encodes integers as longs
                .hasAttribute("float_val", 234.1f)
                .hasAttribute("double_val", 567.123d)
                .hasAttribute("long_val", -123L)
                .hasAttribute("bool_true", true)
                .hasAttribute("bool_false", false)
                .geometry()
                .isPoint()
                .moveTo(3, 6)
                .matches();
    }

    @Test
    void testExtentScaling() {
        // Coordinates given in a grid of 256 for a tile of extent 512: the input scale multiplies them by 512/256 = 2
        Geometry geometry = geom("POINT(3 6)");

        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(512).setInputScale(2);
        LayerBuilder layer = vtb.layer().name("SCALING");
        layer.feature(Map.of(), geometry);
        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("SCALING")
                .hasExtent(512)
                .firstFeature()
                .geometry()
                .isPoint()
                .moveTo(6, 12) // 3*2=6, 6*2=12
                .matches();
    }

    @Test
    void testCommandsFilter() {
        // Test that duplicate consecutive points are filtered during encoding
        Geometry geometry = geom("LINESTRING(3 6, 8 12, 8 12, 20 34)");

        VectorTile tile = build("FILTER", 256, geometry);

        assertThat(tile)
                .layer("FILTER")
                .firstFeature()
                .geometry()
                .isLineString()
                .moveTo(3, 6)
                .lineTo(8, 12)
                // Note: duplicate (8, 12) point should be filtered out
                .lineTo(20, 34)
                .matches();
    }

    @Test
    void testPolygonClockwiseIsNormalized() {
        // Test polygon with clockwise exterior ring
        Geometry polygon = geom("POLYGON((3 6, 20 34, 8 12, 3 6))");

        VectorTile tile = build("CW_POLYGON", 256, polygon);

        assertThat(tile)
                .layer("CW_POLYGON")
                .firstFeature()
                .geometry()
                .isPolygon()
                .equalsNorm(polygon)
                .moveTo(3, 6)
                .lineTo(8, 12)
                .lineTo(20, 34)
                .closePath()
                .matches();
    }

    @Test
    void testPolygonCounterClockwise() {
        // Test polygon with counter-clockwise exterior ring
        Geometry polygon = geom("POLYGON((3 6, 8 12, 20 34, 3 6))");

        VectorTile tile = build("CCW_POLYGON", 256, polygon);

        assertThat(tile)
                .layer("CCW_POLYGON")
                .firstFeature()
                .geometry()
                .isPolygon()
                .equalsNorm(polygon)
                .moveTo(3, 6)
                .lineTo(8, 12)
                .lineTo(20, 34)
                .closePath()
                .matches();
    }

    @Test
    void testPolygonCounterClockwiseWithHole() {
        // Test polygon with counter-clockwise exterior ring and clockwise interior ring
        Geometry polygon = geom("""
                POLYGON(
                  (10 0, 0 0, 0 10, 10 10, 10 0),
                  (3 3, 3 7, 7 7, 7 3, 3 3)
                )
                """);

        VectorTile tile = build("CCW_POLYGON", 256, polygon);

        assertThat(tile)
                .layer("CCW_POLYGON")
                .firstFeature()
                .geometry()
                .isPolygon()
                .equalsExact("""
                        POLYGON(
                          (10 0, 10 10, 0 10, 0 0, 10 0),
                          (3 3, 3 7, 7 7, 7 3, 3 3)
                        )
                        """)
                .moveTo(10, 0)
                .lineTo(10, 10)
                .lineTo(0, 10)
                .lineTo(0, 0)
                .closePath()
                .moveTo(3, 3)
                .lineTo(3, 7)
                .lineTo(7, 7)
                .lineTo(7, 3)
                .closePath()
                .matches();
    }

    @Test
    void testAllAttributeTypes() {
        // Test various attribute data types supported by MVT
        Geometry geometry = geom("POINT(3 6)");

        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(256);
        LayerBuilder layer = vtb.layer().name("ATTR_TYPES");

        // Add feature with comprehensive attribute types
        Map<String, Object> attributes = Map.of(
                "string_val",
                "value1",
                "int_val",
                Integer.valueOf(123),
                "float_val",
                Float.valueOf(234.1f),
                "double_val",
                Double.valueOf(567.123d),
                "long_val",
                Long.valueOf(-123),
                "string_val_2",
                "value6",
                "bool_true",
                Boolean.TRUE,
                "bool_false",
                Boolean.FALSE);

        layer.feature(attributes, geometry);
        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("ATTR_TYPES")
                .firstFeature()
                .hasAttribute("string_val", "value1")
                .hasAttribute("int_val", 123L) // MVT encodes integers as longs
                .hasAttribute("float_val", 234.1f)
                .hasAttribute("double_val", 567.123d)
                .hasAttribute("long_val", -123L)
                .hasAttribute("string_val_2", "value6")
                .hasAttribute("bool_true", true)
                .hasAttribute("bool_false", false)
                .geometry()
                .isPoint()
                .moveTo(3, 6)
                .matches();
    }

    @Test
    void testProvidedFeatureIds() {
        // Test providing specific feature IDs
        Geometry geometry = geom("POINT(3 6)");

        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(256);
        LayerBuilder layer = vtb.layer().name("PROVIDED_IDS");

        Map<String, Object> attributes = Map.of("key1", "value1");
        layer.feature(attributes, geometry, 50); // Provide specific ID

        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("PROVIDED_IDS")
                .hasFeatureCount(1)
                .firstFeature()
                .hasId(50)
                .hasAttribute("key1", "value1")
                .geometry()
                .isPoint()
                .moveTo(3, 6)
                .matches();
    }

    @Test
    void testMixedProvidedAndGeneratedIds() {
        // Test mixing provided IDs with auto-generated ones
        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(256).setAutoIncrementIds(false);
        LayerBuilder layer = vtb.layer().name("MIXED_IDS");

        Map<String, Object> attributes1 = Map.of("key", "value1");
        Map<String, Object> attributes2 = Map.of("key", "value2");
        Map<String, Object> attributes3 = Map.of("key", "value3");

        // Mix of provided and default (0) IDs
        layer.feature(attributes1, geom("POINT(3 6)"), 50); // Provided ID
        layer.feature(attributes2, geom("POINT(6 9)")); // Default ID (0)
        layer.feature(attributes3, geom("POINT(9 12)"), 27); // Another provided ID

        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("MIXED_IDS")
                .hasFeatureCount(3)
                .feature(0)
                .hasId(50)
                .hasAttribute("key", "value1")
                .layer()
                .feature(1)
                .hasId(0) // Default ID when not provided
                .hasAttribute("key", "value2")
                .layer()
                .feature(2)
                .hasId(27)
                .hasAttribute("key", "value3");
    }

    @Test
    void testDefaultFeatureIds() {
        // Test that features get default ID of 0 when not provided
        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(256);
        LayerBuilder layer = vtb.layer().name("DEFAULT_IDS");

        Map<String, Object> attributes = Map.of("key1", "value1");
        layer.feature(attributes, geom("POINT(3 6)")); // No ID provided

        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("DEFAULT_IDS")
                .firstFeature()
                .hasId(0) // Default ID
                .hasAttribute("key1", "value1");
    }

    @Test
    void testGenericGeometryCollection() {
        // Test that a generic GeometryCollection (with disparate geometry types)
        // gets decomposed into multiple features, each with the same attributes
        Geometry geomCollection = geom("""
                GEOMETRYCOLLECTION(
                  POINT(10 20),
                  LINESTRING(30 40, 50 60),
                  POLYGON((70 80, 90 80, 90 100, 70 100, 70 80))
                )
                """);

        VectorTileBuilder builder = new VectorTileBuilder().setExtent(256);
        Map<String, Object> attributes = Map.of("source", "collection", "type", "mixed");

        VectorTile tile = builder.layer()
                .name("MIXED")
                .feature(attributes, geomCollection)
                .build()
                .build();

        // The pipeline splits the generic collection into 3 separate features with identical attributes
        assertThat(tile)
                .layer("MIXED")
                .hasFeatureCount(3)
                // First feature: POINT(10 20)
                .feature(0)
                .hasAttribute("source", "collection")
                .hasAttribute("type", "mixed")
                .geometry()
                .isPoint()
                .moveTo(10, 20)
                .matches()
                .layer()
                // Second feature: LINESTRING(30 40, 50 60)
                .feature(1)
                .hasAttribute("source", "collection")
                .hasAttribute("type", "mixed")
                .geometry()
                .isLineString()
                .moveTo(30, 40)
                .lineTo(50, 60)
                .matches()
                .layer()
                // Third feature: POLYGON, written in its own order: its exterior ring has a positive area
                .feature(2)
                .hasAttribute("source", "collection")
                .hasAttribute("type", "mixed")
                .geometry()
                .isPolygon()
                .moveTo(70, 80)
                .lineTo(90, 80)
                .lineTo(90, 100)
                .lineTo(70, 100)
                .closePath()
                .matches();
    }

    @Test
    void testPrecisionSnapping() {
        // Test that fractional coordinates get snapped to integers by GeometryPrecisionReducer
        // Using coordinates with decimal places close to extent range 4096
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(4096).setUsePrecisionModelSnapping(true);

        // Create geometries with fractional coordinates using WKT
        Geometry point = geom("POINT(4095.7 2048.3)");
        Geometry lineString = geom("LINESTRING(100.2 200.8, 300.6 400.4, 500.9 600.1)");

        VectorTile tile = builder.layer()
                .name("PRECISION_TEST")
                .feature(Map.of("type", "snapped"), point)
                .feature(Map.of("type", "snapped"), lineString)
                .build()
                .build();

        // Verify that fractional coordinates are snapped to integers
        assertThat(tile)
                .layer("PRECISION_TEST")
                .hasFeatureCount(2)
                // Point: (4095.7, 2048.3) -> (4096, 2048)
                .feature(0)
                .hasAttribute("type", "snapped")
                .geometry()
                .isPoint()
                .moveTo(4096, 2048)
                .matches()
                .layer()
                // LineString: fractional coordinates snapped to nearest integers
                .feature(1)
                .hasAttribute("type", "snapped")
                .geometry()
                .isLineString()
                .moveTo(100, 201) // (100.2, 200.8) -> (100, 201)
                .lineTo(301, 400) // (300.6, 400.4) -> (301, 400)
                .lineTo(501, 600) // (500.9, 600.1) -> (501, 600)
                .matches();
    }

    @Test
    void testGeometryClipping() {
        // Test that geometries are clipped according to extent + clipBuffer
        // Default clipBuffer is 32, extent is 4096, so clipping envelope is (-32, -32) to (4128, 4128)
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(4096).setClipBuffer(64);

        // Point outside extent but within buffer - should be included
        Geometry pointInBuffer = geom("POINT(4120 2000)"); // x=4120 > extent=4096 but < extent+buffer=4160

        // Point outside extent and buffer - should be excluded
        Geometry pointOutsideBuffer = geom("POINT(5000 2000)"); // x=5000 > extent+buffer=4160

        // LineString that crosses tile boundary - should be clipped
        // With extent=4096 and buffer=64, clipping envelope is (-64, -64) to (4160, 4160)
        Geometry lineStringCrossing = geom("LINESTRING(3000 3000, 4200 4200)"); // extends beyond extent+buffer

        // LineString completely outside - should be excluded
        Geometry lineStringOutside = geom("LINESTRING(5000 5000, 6000 6000)");

        VectorTile tile = builder.layer()
                .name("CLIPPING_TEST")
                .feature(Map.of("type", "in_buffer"), pointInBuffer)
                .feature(Map.of("type", "outside_buffer"), pointOutsideBuffer)
                .feature(Map.of("type", "crossing"), lineStringCrossing)
                .feature(Map.of("type", "outside"), lineStringOutside)
                .build()
                .build();

        // Only geometries within extent+buffer should be included
        // pointInBuffer should be included, pointOutsideBuffer should be excluded
        // lineStringCrossing should be clipped, lineStringOutside should be excluded
        assertThat(tile)
                .layer("CLIPPING_TEST")
                .hasFeatureCount(2) // Only pointInBuffer and clipped lineStringCrossing
                .feature(0)
                .hasAttribute("type", "in_buffer")
                .geometry()
                .isPoint()
                .moveTo(4120, 2000)
                .matches()
                .layer()
                .feature(1)
                .hasAttribute("type", "crossing")
                .geometry()
                .isLineString()
                // LineString clipped at x=4160 (extent+buffer boundary)
                // Original: LINESTRING(3000 3000, 4200 4200)
                // Clipped: LINESTRING(3000 3000, 4160 4160) - intersection at clip boundary
                .equalsExact("LINESTRING(3000 3000, 4160 4160)")
                .moveTo(3000, 3000)
                .lineTo(4160, 4160)
                .matches();
    }

    @Test
    void testFourEqualPoints() {
        // Test that four identical points in a MULTIPOINT are encoded as MoveTo with count=4
        Geometry geom = geom("MULTIPOINT((3 6), (3 6), (3 6), (3 6))");
        VectorTile tile = build("MP_DUPS", 256, geom);

        // According to MVT spec: MULTIPOINT uses single MoveTo with count > 1
        assertThat(tile)
                .layer("MP_DUPS")
                .firstFeature()
                .geometry()
                .isMultiPoint()
                .moveTo()
                .count(4)
                .coord(3, 6)
                .coord(3, 6)
                .coord(3, 6)
                .coord(3, 6)
                .done()
                .matches();
    }

    // The second build of a held feature builder writes the second geometry alone
    @Test
    void buildsHeldFeatureBuilderTwice() {
        LayerBuilder layer = new VectorTileBuilder().setExtent(256).layer().name("HELD");
        FeatureBuilder feature = layer.feature();
        feature.geometry(geom("POINT(3 6)")).build();
        feature.geometry(geom("POINT(5 7)")).build();

        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("HELD")
                .hasFeatureCount(2)
                .feature(1)
                .geometry()
                .isPoint()
                .moveTo(5, 7)
                .matches();
    }

    // Protobuf writes a parsed tile back in its own encoding, with its field order and packed fields
    @Test
    void writesProtobufEncoding() throws IOException {
        VectorTile automaticIds = tileWithSeveralLayers(new VectorTileBuilder());
        VectorTile withoutIds = tileWithSeveralLayers(new VectorTileBuilder().setAutoIncrementIds(false));

        assertThat(automaticIds).hasExactLayers("values", "geometries");
        assertProtobufEncoding(automaticIds);
        assertProtobufEncoding(withoutIds);
    }

    /** Builds a layer with each type of value, a layer without features, and a layer of multi-geometries. */
    private VectorTile tileWithSeveralLayers(VectorTileBuilder builder) {
        LayerBuilder values = builder.layer().name("values");
        values.feature(eachValueType(), geom("POINT(3 6)"));
        values.feature(Map.of("rank", -2), geom("POINT(5 7)"), 300);
        values.build();
        builder.layer().name("empty").build();
        LayerBuilder geometries = builder.layer().name("geometries");
        geometries.feature(Map.of(), geom("MULTIPOINT((1 2), (3 4))"));
        geometries.feature(Map.of(), geom("MULTILINESTRING((0 0, 5 5), (10 10, 15 15, 20 10))"));
        geometries.feature(
                Map.of(), geom("MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))"));
        // A feature for each member, both with the next automatic id
        geometries.feature(Map.of("kind", "mixed"), geom("GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 9 9))"));
        geometries.build();
        return builder.build();
    }

    private static Map<String, Object> eachValueType() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("string", "text");
        values.put("int", -7);
        values.put("long", 1L << 40);
        values.put("float", 1.5f);
        values.put("double", -2.25d);
        values.put("decimal", new BigDecimal("1.10"));
        values.put("short", (short) 3);
        values.put("bool", true);
        values.put("character", 'c');
        return values;
    }

    private static void assertProtobufEncoding(VectorTile tile) throws IOException {
        assertProtobufEncoding(new VectorTileCodec().encode(tile));
    }

    private static void assertProtobufEncoding(byte[] bytes) throws IOException {
        byte[] rewritten = VectorTileProto.Tile.parseFrom(bytes).toByteArray();
        Assertions.assertThat(rewritten).isEqualTo(bytes);
    }

    @Test
    void failsOnRepeatedLayerName() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)")).build();
        LayerBuilder layer = builder.layer();

        assertThatThrownBy(() -> layer.name("roads")).isInstanceOf(IllegalArgumentException.class);
    }

    // UTF-8 has no encoding for an unpaired surrogate: both names would be written as "roads?"
    @Test
    void rejectsUnpairedSurrogate() {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder layer = builder.layer();

        assertThatThrownBy(() -> layer.name("roads\uD800")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> layer.name("roads\uDC00")).isInstanceOf(IllegalArgumentException.class);
        layer.name("roads\uD83D\uDE97").feature(Map.of(), geom("POINT(3 6)")).build();

        assertThat(builder.build()).hasExactLayers("roads\uD83D\uDE97");
    }

    // The tile holds nothing under the name of a layer without features: one built, then one closed by the next layer
    @Test
    void freesNameOfEmptyLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").build();
        builder.layer().name("roads");
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)")).build();

        assertThat(builder.build()).hasExactLayers("roads");
    }

    @Test
    void failsOnLayerBuilderAfterNextLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder first = builder.layer().name("a");
        builder.layer().name("b");

        assertThatThrownBy(first::feature).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failsOnLayerBuilderAfterBuild() {
        LayerBuilder layer = new VectorTileBuilder().layer().name("a");
        layer.feature(Map.of(), geom("POINT(3 6)")).build();

        assertThatThrownBy(layer::build).isInstanceOf(IllegalStateException.class);
    }

    // A feature builder held past the build of its layer would write into the next layer
    @Test
    void failsOnFeatureAfterLayerBuild() {
        LayerBuilder layer = new VectorTileBuilder().layer().name("a");
        Geometry point = geom("POINT(3 6)");
        FeatureBuilder feature = layer.feature().geometry(point);
        layer.build();
        Map<String, Object> noAttributes = Map.of();

        assertThatThrownBy(() -> feature.id(1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> feature.geometry(point)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> feature.attribute("name", "x")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> feature.attributes(noAttributes)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(feature::build).isInstanceOf(IllegalStateException.class);
    }

    // The next layer has a feature builder of its own: the held one stays closed
    @Test
    void failsOnFeatureAfterNextLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        FeatureBuilder held = builder.layer().name("a").feature().geometry(geom("POINT(3 6)"));
        held.build().build();
        FeatureBuilder next = builder.layer().name("b").feature().attribute("kind", "bar");

        assertThatThrownBy(() -> held.attribute("name", "x")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(held::build).isInstanceOf(IllegalStateException.class);
        next.geometry(geom("POINT(5 7)")).build().build();

        assertThat(builder.build()).layer("b").hasFeatureCount(1).hasExactAttributes("kind");
    }

    // The first member of the collection is written before the second one fails
    @Test
    void rollsBackFeatureOnFailure() {
        VectorTileBuilder builder = new VectorTileBuilder().setPipeline(GeometryPipeline.empty());
        LayerBuilder layer = builder.layer().name("points");
        FeatureBuilder failing = layer.feature().geometry(geom("GEOMETRYCOLLECTION(POINT(1 1), POINT(NaN 1))"));

        assertThatThrownBy(failing::build).isInstanceOf(IllegalArgumentException.class);
        layer.feature(Map.of(), geom("POINT(3 6)")).build();

        assertThat(builder.build())
                .layer("points")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(3, 6)
                .matches();
    }

    // The step lets the first member through and fails on the second with an Error, as a Java assert does
    @Test
    void stepErrorRollsBackFeature() throws IOException {
        GeometryStep failingOnSecond = geometry -> {
            if (geometry.getCoordinate().x == 2) {
                throw new AssertionError("the second member");
            }
            return geometry;
        };
        VectorTileBuilder builder = new VectorTileBuilder().setPipeline(GeometryPipeline.of(failingOnSecond));
        LayerBuilder layer = builder.layer().name("points");
        Geometry collection = geom("GEOMETRYCOLLECTION(POINT(1 1), POINT(2 2))");

        assertThatThrownBy(() -> layer.feature(Map.of(), collection)).isInstanceOf(AssertionError.class);
        layer.feature(Map.of(), geom("POINT(3 6)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    // The first member of each collection is written before the second one fails
    @Test
    void failedBuildKeepsAutomaticIds() throws IOException {
        Geometry failing = geom("GEOMETRYCOLLECTION(POINT(1 1), POINT(NaN 1))");
        VectorTileBuilder builder = new VectorTileBuilder().setPipeline(GeometryPipeline.empty());
        LayerBuilder layer = builder.layer().name("points");

        assertThatThrownBy(() -> layer.feature(Map.of(), failing)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> layer.feature(Map.of(), failing, Long.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        layer.feature(Map.of(), geom("POINT(3 6)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    // The step closes the open layer: the feature would be written into the next layer
    @Test
    void failsOnLayerFromStep() {
        VectorTileBuilder builder = new VectorTileBuilder();
        GeometryStep startingLayer = geometry -> {
            builder.layer();
            return geometry;
        };
        builder.setPipeline(GeometryPipeline.of(startingLayer));
        LayerBuilder points = builder.layer().name("points");
        Geometry point = geom("POINT(3 6)");

        assertThatThrownBy(() -> points.feature(Map.of(), point)).isInstanceOf(IllegalStateException.class);
        builder.setPipeline(GeometryPipeline.empty());
        points.feature(Map.of(), geom("POINT(5 7)")).build();

        assertThat(builder.build())
                .layer("points")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(5, 7)
                .matches();
    }

    // The layer has one feature builder: the step would reset the feature being written
    @Test
    void failsOnFeatureFromStep() {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder points = builder.layer().name("points");
        GeometryStep startingFeature = geometry -> {
            points.feature();
            return geometry;
        };
        builder.setPipeline(GeometryPipeline.of(startingFeature));
        Map<String, Object> cafe = Map.of("kind", "cafe");
        Geometry point = geom("POINT(3 6)");

        assertThatThrownBy(() -> points.feature(cafe, point)).isInstanceOf(IllegalStateException.class);
    }

    // The step changes the feature being written: a second build after a failed one would write the change
    @ParameterizedTest
    @MethodSource("featureSetters")
    void failsOnFeatureSetterFromStep(Consumer<FeatureBuilder> setter) throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        FeatureBuilder feature = builder.layer().name("points").feature().geometry(geom("POINT(3 6)"));
        GeometryStep changingFeature = geometry -> {
            setter.accept(feature);
            return geometry;
        };
        builder.setPipeline(GeometryPipeline.of(changingFeature));

        assertThatThrownBy(feature::build).isInstanceOf(IllegalStateException.class);
        builder.setPipeline(GeometryPipeline.empty());
        feature.build().build();

        byte[] bytes = builder.toByteArray();
        Assertions.assertThat(writtenIds(bytes)).containsExactly(0L);
        assertThat(new VectorTileCodec().decode(bytes))
                .layer("points")
                .firstFeature()
                .geometry()
                .moveTo(3, 6)
                .matches();
    }

    static Stream<Named<Consumer<FeatureBuilder>>> featureSetters() {
        Geometry otherPoint = new GeometryFactory().createPoint(new Coordinate(9, 9));
        return Stream.of(
                Named.of("id", feature -> feature.id(Long.MAX_VALUE)),
                Named.of("geometry", feature -> feature.geometry(otherPoint)));
    }

    // The value fails once its key is in the tables of the layer: the feature would keep the key without a value
    @Test
    void failedAttributeKeepsTagPairs() {
        VectorTileBuilder builder = new VectorTileBuilder();
        FeatureBuilder feature = builder.layer().name("points").feature().attribute("kind", "cafe");
        Object failing = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("A value without text");
            }
        };

        assertThatThrownBy(() -> feature.attribute("name", failing)).isInstanceOf(IllegalStateException.class);
        feature.geometry(geom("POINT(3 6)")).build().build();

        Feature written = onlyLayer(builder.build()).getFeatures().findFirst().orElseThrow();
        Assertions.assertThat(written.getAttributes()).isEqualTo(Map.of("kind", "cafe"));
    }

    // The step replaces the pipeline while it runs: the next features would skip its steps
    @Test
    void failsOnPipelineFromStep() {
        VectorTileBuilder builder = new VectorTileBuilder();
        GeometryStep replacingPipeline = geometry -> {
            builder.setPipeline(GeometryPipeline.empty());
            return geometry;
        };
        GeometryPipeline pipeline = GeometryPipeline.of(replacingPipeline);
        builder.setPipeline(pipeline);
        LayerBuilder points = builder.layer().name("points");
        Geometry point = geom("POINT(3 6)");

        assertThatThrownBy(() -> points.feature(Map.of(), point)).isInstanceOf(IllegalStateException.class);
        Assertions.assertThat(builder.pipeline()).isSameAs(pipeline);
    }

    @Test
    void failsOnNewLayerWhileLayerOpen() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)"));

        assertThatThrownBy(builder::layer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("roads");
    }

    @Test
    void namesUnnamedOpenLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().feature(Map.of(), geom("POINT(3 6)"));

        assertThatThrownBy(builder::layer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("the unnamed layer");
    }

    @Test
    void namesUnnamedClosedLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder unnamed = builder.layer();
        builder.layer();

        assertThatThrownBy(unnamed::feature)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("the unnamed layer");
    }

    @Test
    void closesEmptyOpenLayerSilently() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("a");
        builder.layer().name("b").feature(Map.of(), geom("POINT(3 6)")).build();

        assertThat(builder.build()).hasExactLayers("b");
    }

    @Test
    void failsTileBuildWhileLayerOpen() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(builder::toByteArray).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> builder.writeTo(out)).isInstanceOf(IllegalStateException.class);
    }

    // The decoded tile comes from the same builder, cleared by toByteArray()
    @Test
    void writesBytesWithoutDecodedTile() {
        VectorTileBuilder builder = new VectorTileBuilder();
        byte[] bytes = addLayers(builder).toByteArray();

        VectorTile tile = addLayers(builder).build();

        Assertions.assertThat(bytes).isEqualTo(new VectorTileCodec().encode(tile));
    }

    @Test
    void writesToStream() throws IOException {
        VectorTileBuilder streamed = addLayers(new VectorTileBuilder());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        streamed.writeTo(out);

        byte[] copied = addLayers(new VectorTileBuilder()).toByteArray();
        Assertions.assertThat(out.toByteArray()).isEqualTo(copied);
    }

    // A builder reused after a failed write starts the next tile without the layers of the failed one
    @Test
    void clearsWhenStreamFails() {
        VectorTileBuilder builder = addLayers(new VectorTileBuilder());
        OutputStream failing = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("closed");
            }
        };

        assertThatThrownBy(() -> builder.writeTo(failing)).isInstanceOf(IOException.class);
        Assertions.assertThat(builder.toByteArray()).isEmpty();
    }

    // The stream starts a layer while the tile is written: the builder would clear it with the written tile
    @Test
    void failsOnLayerFromStream() {
        VectorTileBuilder builder = addLayers(new VectorTileBuilder());
        OutputStream startingLayer = new OutputStream() {
            @Override
            public void write(int b) {
                builder.layer();
            }
        };

        assertThatThrownBy(() -> builder.writeTo(startingLayer)).isInstanceOf(IllegalStateException.class);
        Assertions.assertThat(builder.toByteArray()).isEmpty();
    }

    /** Adds two layers, with automatic ids, an attribute per feature and the three types of geometry. */
    private VectorTileBuilder addLayers(VectorTileBuilder builder) {
        builder.layer()
                .name("roads")
                .feature(Map.of("class", "primary"), geom("LINESTRING(0 0, 10 10, 20 0)"))
                .feature(Map.of("class", "park"), geom("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"))
                .build();
        builder.layer()
                .name("pois")
                .feature(Map.of("rank", 2), geom("POINT(3 6)"))
                .build();
        return builder;
    }

    @Test
    void clearsForNextTile() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)")).build();
        builder.toByteArray();

        builder.layer().name("roads").feature(Map.of(), geom("POINT(5 7)")).build();

        assertThat(builder.build())
                .hasExactLayers("roads")
                .layer("roads")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(5, 7)
                .matches();
    }

    @Test
    void dropsSmallFeaturesByDefault() {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("small")
                .feature(Map.of(), geom("LINESTRING(0 0, 0.5 0)"))
                .feature(Map.of(), geom("POLYGON((0 0, 0.5 0, 0.5 0.5, 0 0.5, 0 0))"))
                .feature(Map.of(), geom("POINT(3 6)"))
                .build();

        assertThat(builder.build())
                .layer("small")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .isPoint();
    }

    @Test
    void usesExplicitPipeline() {
        VectorTileBuilder builder = new VectorTileBuilder().setPipeline(GeometryPipeline.empty());
        builder.layer()
                .name("unfiltered")
                .feature(Map.of(), geom("POINT(5000 2000)"))
                .feature(Map.of(), geom("LINESTRING(0 0, 0.5 0)"))
                .build();

        assertThat(builder.build())
                .layer("unfiltered")
                .hasFeatureCount(2)
                .feature(0)
                .geometry()
                .moveTo(5000, 2000)
                .matches()
                .layer()
                .feature(1)
                .geometry()
                .moveTo(0, 0)
                .lineTo(1, 0)
                .matches();
    }

    @Test
    void derivesPipelineFromSettings() {
        VectorTileBuilder builder = new VectorTileBuilder();
        List<GeometryStep> derived = builder.pipeline().steps();
        List<GeometryStep> explicit =
                builder.setPipeline(GeometryPipeline.empty()).pipeline().steps();
        List<GeometryStep> rebuilt = builder.setClipBuffer(8).pipeline().steps();

        Assertions.assertThat(derived)
                .extracting(Object::getClass)
                .containsExactly(SizeFilterStep.class, ClipStep.class);
        Assertions.assertThat(explicit).isEmpty();
        Assertions.assertThat(rebuilt)
                .extracting(Object::getClass)
                .containsExactly(SizeFilterStep.class, ClipStep.class);
        Assertions.assertThat(((ClipStep) rebuilt.get(1)).buffer()).isEqualTo(8.0);
    }

    @Test
    void settingsAddSimplificationAndSnapping() {
        VectorTileBuilder builder =
                new VectorTileBuilder().setSimplificationDistanceTolerance(1).setUsePrecisionModelSnapping(true);

        Assertions.assertThat(builder.pipeline().steps())
                .extracting(Object::getClass)
                .containsExactly(
                        SizeFilterStep.class,
                        SimplificationStep.class,
                        ClipStep.class,
                        GridSnapStep.class,
                        PolygonRepairStep.class);
    }

    @Test
    void toleranceAddsRepairStep() {
        VectorTileBuilder builder = new VectorTileBuilder().setSimplificationDistanceTolerance(0.1);

        Assertions.assertThat(builder.pipeline().steps())
                .extracting(Object::getClass)
                .containsExactly(
                        SizeFilterStep.class, SimplificationStep.class, ClipStep.class, PolygonRepairStep.class);
    }

    // A step setting given after setPipeline derives the pipeline from the settings again
    @Test
    void stepSettingsRebuildPipeline() {
        VectorTileBuilder builder = new VectorTileBuilder();
        GeometryPipeline explicit = GeometryPipeline.empty();

        List<GeometryStep> afterTolerance = builder.setPipeline(explicit)
                .setSimplificationDistanceTolerance(1)
                .pipeline()
                .steps();
        List<GeometryStep> afterSnapping = builder.setPipeline(explicit)
                .setUsePrecisionModelSnapping(true)
                .pipeline()
                .steps();
        List<GeometryStep> afterScale =
                builder.setPipeline(explicit).setInputScale(2).pipeline().steps();

        Assertions.assertThat(afterTolerance).hasAtLeastOneElementOfType(SimplificationStep.class);
        Assertions.assertThat(afterSnapping).hasAtLeastOneElementOfType(GridSnapStep.class);
        Assertions.assertThat(afterScale).hasAtLeastOneElementOfType(ClipStep.class);
    }

    @ParameterizedTest
    @ValueSource(doubles = {0, -1, Double.NaN, Double.POSITIVE_INFINITY})
    void rejectsInvalidInputScale(double scale) {
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.setInputScale(scale))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The input scale must be positive and finite: " + scale);
    }

    // The extent of the clip step, 4096 divided by the scale, is infinite: the step rejects it
    @Test
    void failedScaleKeepsSettings() {
        VectorTileBuilder builder = new VectorTileBuilder();
        GeometryPipeline pipeline = builder.pipeline();

        assertThatThrownBy(() -> builder.setInputScale(Double.MIN_NORMAL)).isInstanceOf(IllegalArgumentException.class);
        builder.layer().name("points").feature(Map.of(), geom("POINT(3 6)")).build();

        Assertions.assertThat(builder.pipeline()).isSameAs(pipeline);
        assertThat(builder.build())
                .layer("points")
                .firstFeature()
                .geometry()
                .moveTo(3, 6)
                .matches();
    }

    // The settings in tile units, divided by the input scale
    @Test
    void settingsInInputUnits() {
        VectorTileBuilder builder = new VectorTileBuilder().setInputScale(16);

        List<GeometryStep> steps = builder.pipeline().steps();
        List<GeometryStep> simplifying =
                builder.setSimplificationDistanceTolerance(2).pipeline().steps();

        Assertions.assertThat(steps).extracting(Object::getClass).containsExactly(SizeFilterStep.class, ClipStep.class);
        Assertions.assertThat(steps.get(0)).hasToString("SizeFilterStep[minimumLength=0.0625, minimumArea=0.00390625]");
        ClipStep clip = (ClipStep) steps.get(1);
        Assertions.assertThat(clip.extent()).isEqualTo(256.0);
        Assertions.assertThat(clip.buffer()).isEqualTo(2.0);
        Assertions.assertThat(simplifying)
                .extracting(Object::toString)
                .contains("SimplificationStep[distanceTolerance=0.125]");
    }

    // The default clip buffer of 32 tile units is 2 units of the input: the clip area ends at 258
    @Test
    void scaledInputClipsInInputUnits() {
        VectorTileBuilder builder = new VectorTileBuilder().setInputScale(16);
        builder.layer()
                .name("pois")
                .feature(Map.of(), geom("POINT(257.9 3)"))
                .feature(Map.of(), geom("POINT(258.1 3)"))
                .build();

        assertThat(builder.build())
                .layer("pois")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(4126, 48)
                .matches();
    }

    // The first line is 0.05 units of the input long, 0.8 tile units
    @Test
    void scaledInputDropsShortLines() {
        VectorTileBuilder builder = new VectorTileBuilder().setInputScale(16);
        builder.layer()
                .name("roads")
                .feature(Map.of(), geom("LINESTRING(1 1, 1.05 1)"))
                .feature(Map.of(), geom("LINESTRING(1 1, 1.07 1)"))
                .build();

        assertThat(builder.build())
                .layer("roads")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(16, 16)
                .lineTo(17, 16)
                .matches();
    }

    // Once rounded, the hole shares two of its edges with the shell
    @Test
    void snappingOffWritesRoundedPolygon() {
        Geometry polygon = geom("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.4 0.4, 0.4 2, 2 2, 2 0.4, 0.4 0.4))");

        VectorTile tile = build("rounded", 4096, polygon);

        assertThat(tile)
                .layer("rounded")
                .firstFeature()
                .geometry()
                .isPolygon()
                .equalsNorm("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0 0, 0 2, 2 2, 2 0, 0 0))")
                .isInvalid();
    }

    @Test
    void snappingOnRepairsRoundedPolygon() {
        Geometry polygon = geom("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.4 0.4, 0.4 2, 2 2, 2 0.4, 0.4 0.4))");
        VectorTileBuilder builder = new VectorTileBuilder().setUsePrecisionModelSnapping(true);

        builder.layer().name("snapped").feature(Map.of(), polygon).build();

        assertThat(builder.build())
                .layer("snapped")
                .firstFeature()
                .geometry()
                .isPolygon()
                .equalsNorm("POLYGON((0 2, 2 2, 2 0, 10 0, 10 10, 0 10, 0 2))")
                .isValid();
    }

    // The polygon of snappingOffWritesRoundedPolygon: the simplification keeps its points
    @Test
    void writesValidPolygonWithSimplification() {
        Geometry polygon = geom("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (0.4 0.4, 0.4 2, 2 2, 2 0.4, 0.4 0.4))");
        VectorTileBuilder builder = new VectorTileBuilder().setSimplificationDistanceTolerance(0.1);

        builder.layer().name("simplified").feature(Map.of(), polygon).build();

        assertThat(builder.build())
                .layer("simplified")
                .firstFeature()
                .geometry()
                .isPolygon()
                .isValid();
    }

    @Test
    void rejectsExtentBelowOne() {
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.setExtent(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The extent must be at least 1: 0");
    }

    // The same extent and an open layer without features accept the setting
    @Test
    void failsOnExtentChangeInLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder points = builder.layer().name("points").feature(Map.of(), geom("POINT(2048 2048)"));

        assertThatThrownBy(() -> builder.setExtent(256))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("points");
        builder.setExtent(4096);
        points.build();
        LayerBuilder more = builder.layer().name("more");
        builder.setExtent(256);
        more.feature(Map.of(), geom("POINT(128 128)")).build();

        VectorTile tile = builder.build();
        assertThat(tile).layer("points").hasExtent(4096);
        assertThat(tile).layer("more").hasExtent(256);
    }

    @Test
    void rejectsNegativeClipBuffer() {
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.setClipBuffer(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The clip buffer must be at least 0: -1");
    }

    // The setting is checked before it is taken: the builder derives its next pipelines without it
    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
    void rejectsNonFiniteTolerance(double tolerance) {
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.setSimplificationDistanceTolerance(tolerance))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The simplification distance tolerance must be finite: " + tolerance);
        Assertions.assertThat(builder.setClipBuffer(8).pipeline().steps())
                .extracting(Object::getClass)
                .containsExactly(SizeFilterStep.class, ClipStep.class);
    }

    // The tolerance of the simplification step, the largest double divided by the scale 0.5, is infinite
    @Test
    void failedToleranceKeepsSettings() {
        VectorTileBuilder builder = new VectorTileBuilder().setInputScale(0.5);

        assertThatThrownBy(() -> builder.setSimplificationDistanceTolerance(Double.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);

        Assertions.assertThat(builder.setClipBuffer(8).pipeline().steps())
                .extracting(Object::getClass)
                .containsExactly(SizeFilterStep.class, ClipStep.class);
    }

    // A negative id means no id: the automatic ids start at 0 or above
    @Test
    void rejectsNegativeFirstId() {
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.setFirstFeatureId(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The first feature id must be at least 0: -1");
    }

    @Test
    void rejectsNullAttributeName() {
        FeatureBuilder feature = new VectorTileBuilder().layer().name("a").feature();

        assertThatThrownBy(() -> feature.attribute(null, "value")).isInstanceOf(NullPointerException.class);
    }

    @Test
    void negativeIdMeansNoId() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder().setAutoIncrementIds(false);
        LayerBuilder layer = builder.layer().name("ids");
        layer.feature().id(-1).geometry(geom("POINT(3 6)")).build();
        layer.build();

        VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(builder.toByteArray());

        Assertions.assertThat(tile.getLayers(0).getFeatures(0).hasId()).isFalse();
    }

    @Test
    void autoIdsContinueAfterGivenIds() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("first")
                .feature(Map.of(), geom("POINT(1 1)"))
                .feature(Map.of(), geom("POINT(2 2)"), 50)
                .feature(Map.of(), geom("POINT(3 3)"))
                .build();
        builder.layer().name("second").feature(Map.of(), geom("POINT(4 4)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L, 50L, 51L, 52L);
    }

    // The next tile starts at the first id again
    @Test
    void firstIdSetting() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder().setFirstFeatureId(1);
        builder.layer()
                .name("pois")
                .feature(Map.of(), geom("POINT(1 1)"))
                .feature(Map.of(), geom("POINT(2 2)"))
                .build();
        byte[] first = builder.toByteArray();
        builder.layer().name("pois").feature(Map.of(), geom("POINT(3 3)")).build();
        byte[] next = builder.toByteArray();

        Assertions.assertThat(writtenIds(first)).containsExactly(1L, 2L);
        Assertions.assertThat(writtenIds(next)).containsExactly(1L);
    }

    @Test
    void idsRestartForNextTile() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("pois").feature(Map.of(), geom("POINT(1 1)"), 7).build();
        builder.toByteArray();

        builder.layer().name("pois").feature(Map.of(), geom("POINT(2 2)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    @Test
    void collectionMembersShareOneId() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("pois")
                .feature(Map.of(), geom("GEOMETRYCOLLECTION(POINT(1 1), POINT(2 2))"))
                .feature(Map.of(), geom("POINT(3 3)"))
                .build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L, 0L, 1L);
    }

    // A feature takes its id before the pipeline drops it, with or without a given id
    @Test
    void droppedFeatureConsumesId() throws IOException {
        Geometry tooShort = geom("LINESTRING(0 0, 0.5 0)");
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer()
                .name("small")
                .feature(Map.of(), tooShort)
                .feature(Map.of(), geom("POINT(3 6)"))
                .feature(Map.of(), tooShort, 50)
                .feature(Map.of(), geom("POINT(5 7)"))
                .build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(1L, 51L);
    }

    // No automatic id comes after the largest long, until the next tile
    @Test
    void noAutoIdAfterLargestId() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder layer = builder.layer().name("pois").feature(Map.of(), geom("POINT(1 1)"), Long.MAX_VALUE);
        Geometry point = geom("POINT(2 2)");

        assertThatThrownBy(() -> layer.feature(Map.of(), point)).isInstanceOf(IllegalStateException.class);
        layer.build();
        byte[] tile = builder.toByteArray();
        builder.layer().name("pois").feature(Map.of(), geom("POINT(3 3)")).build();

        Assertions.assertThat(writtenIds(tile)).containsExactly(Long.MAX_VALUE);
        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    @Test
    void autoIdTakesLargestIdOnce() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder().setFirstFeatureId(Long.MAX_VALUE);
        LayerBuilder layer = builder.layer().name("pois").feature(Map.of(), geom("POINT(1 1)"));
        Geometry point = geom("POINT(2 2)");

        assertThatThrownBy(() -> layer.feature(Map.of(), point)).isInstanceOf(IllegalStateException.class);
        layer.build();
        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(Long.MAX_VALUE);
    }

    /**
     * The ids written in the tile, layer after layer. A feature without an id gives null: the decoder reads it as 0.
     */
    private static List<Long> writtenIds(byte[] bytes) throws IOException {
        VectorTileProto.Tile tile = VectorTileProto.Tile.parseFrom(bytes);
        List<Long> ids = new ArrayList<>();
        for (VectorTileProto.Tile.Layer layer : tile.getLayersList()) {
            for (VectorTileProto.Tile.Feature feature : layer.getFeaturesList()) {
                ids.add(feature.hasId() ? feature.getId() : null);
            }
        }
        return ids;
    }

    // The bytes of three layers exceed the buffers of one layer: the size must count the buffer of the tile
    @Test
    void reportsHeapOfBuffers() {
        VectorTileBuilder builder = new VectorTileBuilder();
        long empty = builder.heapSize();
        LayerBuilder first = withRoads(builder.layer().name("first"));
        long withFeatures = builder.heapSize();
        first.build();
        withRoads(builder.layer().name("second")).build();
        withRoads(builder.layer().name("third")).build();
        long built = builder.heapSize();

        byte[] bytes = builder.toByteArray();

        Assertions.assertThat(empty).isPositive();
        Assertions.assertThat(withFeatures).isGreaterThan(empty);
        Assertions.assertThat(built).isGreaterThanOrEqualTo(bytes.length);
    }

    // A memory guard reads the size after each feature
    @Test
    void heapSizeReadChangesNothing() {
        VectorTileBuilder builder = new VectorTileBuilder();
        withRoads(builder.layer().name("roads"));

        Assertions.assertThat(builder.heapSize()).isEqualTo(builder.heapSize());
    }

    // A repeated key or value has one entry in its table
    @Test
    void layerCountsTableEntriesOnce() {
        OpenLayer layer = new OpenLayer();
        long buffers = layer.heapSize();
        IntArray tags = new IntArray();
        layer.addTag(tags, "name", "road 1");
        layer.addTag(tags, "name", "road 2");
        layer.addTag(tags, "lanes", 2);
        layer.addTag(tags, "name", "road 1");

        long tables = tablesHeapSize(List.of("name", "lanes"), List.of("road 1", "road 2", 2));
        Assertions.assertThat(layer.heapSize()).isEqualTo(buffers + tables);
    }

    @Test
    void clearedLayerHasNoTables() {
        OpenLayer layer = new OpenLayer();
        long buffers = layer.heapSize();
        layer.addTag(new IntArray(), "name", "road 1");

        layer.clear();

        Assertions.assertThat(layer.heapSize()).isEqualTo(buffers);
    }

    /** The heap of the keys and values of the tables of a layer, with their entries, counted one by one. */
    private static long tablesHeapSize(List<String> keys, List<Object> values) {
        long size = 0;
        for (String key : keys) {
            size += OpenLayer.TABLE_ENTRY + HeapSize.string(key);
        }
        for (Object value : values) {
            size += OpenLayer.TABLE_ENTRY + HeapSize.value(value);
        }
        return size;
    }

    /** Adds 100 lines to the layer, each with a name value of its own. */
    private LayerBuilder withRoads(LayerBuilder layer) {
        Geometry road = geom("LINESTRING(0 0, 10 10, 20 0)");
        for (int i = 0; i < 100; i++) {
            layer.feature(Map.of("name", "road " + i), road);
        }
        return layer;
    }

    // The writer leaves out repeated points and orients the rings: both tiles are compared without repeated points,
    // normalized
    @ParameterizedTest
    @MethodSource("committedTiles")
    void roundTripsDecodedTiles(String resource) throws IOException {
        VectorTileCodec codec = new VectorTileCodec();
        VectorTile source = codec.decode(resource(resource));

        byte[] bytes = rebuild(source);

        RoundTripAssertions.assertSameFeatures(codec.decode(bytes), source);
        assertProtobufEncoding(bytes);
    }

    static Stream<String> committedTiles() {
        return Stream.of(
                "14-8801-5371.vector.pbf", "bigtile.vector.pbf", "cells-11-1058-568.mvt", "cells-11-1065-567.mvt");
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = Objects.requireNonNull(VectorTileBuilderTest.class.getResourceAsStream("/" + name))) {
            return in.readAllBytes();
        }
    }

    /** Writes the features of the tile again, one layer at a time, with their ids and attributes, without steps. */
    private static byte[] rebuild(VectorTile tile) {
        VectorTileBuilder builder = new VectorTileBuilder().setAutoIncrementIds(false);
        for (Layer layer : tile.getLayers()) {
            // A new extent rebuilds the pipeline from the settings: the empty pipeline comes after it
            builder.setExtent(layer.getExtent()).setPipeline(GeometryPipeline.empty());
            LayerBuilder rebuilt = builder.layer().name(layer.getName());
            layer.getFeatures()
                    .forEach(feature ->
                            rebuilt.feature(feature.getAttributes(), feature.getGeometry(), feature.getId()));
            rebuilt.build();
        }
        return builder.toByteArray();
    }

    // Encoding the geometries again would leave out the repeated points of 8 of the 83 polygons of the layer
    @Test
    void mergesLayerCopyingGeometryBytes() throws IOException {
        byte[] cells = resource("cells-11-1058-568.mvt");
        Layer source = new VectorTileCodec().decode(cells).getLayer("DEPARE").orElseThrow();
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("DEPARE").merge(source).build();

        byte[] bytes = builder.toByteArray();
        Layer merged = onlyLayer(new VectorTileCodec().decode(bytes));
        Assertions.assertThat(decodedFeatures(merged)).isEqualTo(decodedFeatures(source));
        Assertions.assertThat(writtenIds(cells)).containsOnlyNulls();
        Assertions.assertThat(writtenIds(bytes)).containsOnlyNulls();
    }

    // 453 of the 558 ids of the layer are 2^63 or above, and getId() returns them as negative longs
    @Test
    void mergeKeepsUnsignedIds() throws IOException {
        VectorTile streets = new VectorTileCodec().decode(resource("14-8801-5371.vector.pbf"));
        Layer source = streets.getLayer("poi_label").orElseThrow();
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("poi_label").merge(source).build();

        Layer merged = onlyLayer(builder.build());
        Assertions.assertThat(decodedFeatures(merged)).isEqualTo(decodedFeatures(source));
    }

    // The wrapper is a layer of another implementation handing out the decoded features: 453 ids are 2^63 or above
    @Test
    void mergeKeepsIdsThroughWrapper() throws IOException {
        VectorTile streets = new VectorTileCodec().decode(resource("14-8801-5371.vector.pbf"));
        Layer source = streets.getLayer("poi_label").orElseThrow();
        Layer wrapper = DelegatingTile.withDecodedFeatures(streets)
                .getLayer("poi_label")
                .orElseThrow();
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("poi_label").merge(wrapper).build();

        Layer merged = onlyLayer(builder.build());
        Assertions.assertThat(decodedFeatures(merged)).isEqualTo(decodedFeatures(source));
    }

    // Read through the model, the id 0 would stand for no id
    @Test
    void mergeKeepsIdZeroThroughWrapper() throws IOException {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer().name("pois").feature(Map.of(), geom("POINT(1 1)"), 0).build();
        VectorTile wrapper = DelegatingTile.withDecodedFeatures(source.build());
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("pois").merge(onlyLayer(wrapper)).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    // The layer of another implementation hands out a decoded feature, with attributes changed through the model
    @Test
    void mergeKeepsChangedAttributes() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer()
                .name("pois")
                .feature(Map.of("name", "cafe", "phone", "555"), geom("POINT(1 1)"))
                .build();
        Feature decoded = onlyLayer(source.build()).getFeatures().findFirst().orElseThrow();
        decoded.getAttributes().remove("phone");
        decoded.getAttributes().put("kind", "bar");
        Layer changed = mock(Layer.class);
        when(changed.getExtent()).thenReturn(4096);
        when(changed.getFeatures()).thenReturn(Stream.of(decoded));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("pois").merge(changed).build();

        Feature merged = onlyLayer(builder.build()).getFeatures().findFirst().orElseThrow();
        Assertions.assertThat(merged.getAttributes()).isEqualTo(Map.of("name", "cafe", "kind", "bar"));
    }

    // The decoder reads the uint value 2^64 - 1 as the long -1, as it reads the sint value -1
    @Test
    void mergeKeepsUnsignedValues() throws IOException {
        Layer source = onlyLayer(new VectorTileCodec().decode(unsignedAndSignedTile()));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("source").merge(source).build();

        byte[] bytes = builder.toByteArray();
        VectorTileProto.Tile.Layer merged =
                VectorTileProto.Tile.parseFrom(bytes).getLayers(0);
        Assertions.assertThat(firstFeatureValues(merged)).isEqualTo(unsignedAndSignedValues());
        Feature decoded = onlyLayer(new VectorTileCodec().decode(bytes))
                .getFeatures()
                .findFirst()
                .orElseThrow();
        Assertions.assertThat(decoded.getAttributes()).isEqualTo(Map.of("unsigned", -1L, "signed", -1L));
    }

    // A filter reads the attributes of a decoded feature, then hands the feature to the merge
    @Test
    void mergeKeepsUnsignedAfterRead() throws IOException {
        Layer source = onlyLayer(new VectorTileCodec().decode(unsignedAndSignedTile()));
        Feature read = source.getFeatures().findFirst().orElseThrow();
        Assertions.assertThat(read.getAttribute("unsigned")).isEqualTo(-1L);
        Layer filtered = mock(Layer.class);
        when(filtered.getExtent()).thenReturn(4096);
        when(filtered.getFeatures()).thenReturn(Stream.of(read));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("source").merge(filtered).build();

        VectorTileProto.Tile.Layer merged =
                VectorTileProto.Tile.parseFrom(builder.toByteArray()).getLayers(0);
        Assertions.assertThat(firstFeatureValues(merged)).isEqualTo(unsignedAndSignedValues());
    }

    /** The values of the attributes of the first feature of the layer, by key, each tag read through the tables. */
    private static Map<String, VectorTileProto.Tile.Value> firstFeatureValues(VectorTileProto.Tile.Layer layer) {
        VectorTileProto.Tile.Feature feature = layer.getFeatures(0);
        Map<String, VectorTileProto.Tile.Value> values = new LinkedHashMap<>();
        for (int i = 0; i < feature.getTagsCount(); i += 2) {
            String key = layer.getKeys(feature.getTags(i));
            values.put(key, layer.getValues(feature.getTags(i + 1)));
        }
        return values;
    }

    /** The values of {@link #unsignedAndSignedTile()} as written: a uint value and a sint value, of the same bits. */
    private static Map<String, VectorTileProto.Tile.Value> unsignedAndSignedValues() {
        VectorTileProto.Tile.Value unsigned =
                VectorTileProto.Tile.Value.newBuilder().setUintValue(-1L).build();
        VectorTileProto.Tile.Value signed =
                VectorTileProto.Tile.Value.newBuilder().setSintValue(-1L).build();
        return Map.of("unsigned", unsigned, "signed", signed);
    }

    /**
     * A tile with a point of two attributes: the uint value 2^64 - 1 under unsigned, the sint value -1 under signed.
     */
    private static byte[] unsignedAndSignedTile() {
        byte[] feature = new ProtoWriter()
                .packedField(FEATURE_TAGS, 0, 0, 1, 1)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        byte[] layer = new ProtoWriter()
                .varintField(LAYER_VERSION, 2)
                .stringField(LAYER_NAME, "source")
                .bytesField(LAYER_FEATURE, feature)
                .stringField(LAYER_KEY, "unsigned")
                .stringField(LAYER_KEY, "signed")
                .bytesField(
                        LAYER_VALUE,
                        new ProtoWriter().varintField(VALUE_UINT, -1).toByteArray())
                .bytesField(
                        LAYER_VALUE,
                        new ProtoWriter().varintField(VALUE_SINT, 1).toByteArray())
                .toByteArray();
        return new ProtoWriter().bytesField(TILE_LAYER, layer).toByteArray();
    }

    // The line collapses to its first point once scaled and rounded, and is left out
    @Test
    void mergesLayerWithOtherExtent() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer()
                .name("pois")
                .feature(Map.of(), geom("POINT(4096 2048)"))
                .feature(Map.of(), geom("LINESTRING(0 0, 4 0)"))
                .build();
        VectorTileBuilder builder = new VectorTileBuilder().setExtent(256);

        builder.layer().name("pois").merge(onlyLayer(source.build())).build();

        assertThat(builder.build())
                .layer("pois")
                .hasExtent(256)
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(256, 128)
                .matches();
    }

    // The decoder reads the extent field as a signed int: a malformed tile can give a negative extent
    @Test
    void mergeRejectsNegativeExtent() throws IOException {
        byte[] layer = new ProtoWriter()
                .varintField(LAYER_VERSION, 2)
                .stringField(LAYER_NAME, "source")
                .varintField(LAYER_EXTENT, -4096)
                .bytesField(LAYER_FEATURE, pointWithoutId())
                .toByteArray();
        byte[] tile = new ProtoWriter().bytesField(TILE_LAYER, layer).toByteArray();
        Layer source = onlyLayer(new VectorTileCodec().decode(tile));
        LayerBuilder builder = new VectorTileBuilder().layer().name("source");

        Assertions.assertThat(source.getExtent()).isEqualTo(-4096);
        assertThatThrownBy(() -> builder.merge(source)).isInstanceOf(IllegalArgumentException.class);
    }

    // The specification wants a type field in each feature: the source feature has none, and reads as UNKNOWN
    @Test
    void mergeWritesUnknownType() throws IOException {
        byte[] withoutType =
                new ProtoWriter().packedField(FEATURE_GEOMETRY, 9, 6, 12).toByteArray();
        VectorTile source = new VectorTileCodec().decode(tileWithLayer(2, withoutType));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("unknown").merge(onlyLayer(source)).build();

        VectorTileProto.Tile.Feature merged = VectorTileProto.Tile.parseFrom(builder.toByteArray())
                .getLayers(0)
                .getFeatures(0);
        Assertions.assertThat(merged.hasType()).isTrue();
        Assertions.assertThat(merged.getType()).isEqualTo(VectorTileProto.Tile.GeomType.UNKNOWN);
        Assertions.assertThat(merged.getGeometryList()).containsExactly(9, 6, 12);
    }

    // The ring has a negative surveyor's sum: version 2 of the specification wants a positive one
    @Test
    void reencodesVersionOneLayer() throws IOException {
        // MoveTo(0 0), LineTo(0 10, 10 10, 10 0), ClosePath
        byte[] polygon = new ProtoWriter()
                .varintField(FEATURE_TYPE, POLYGON)
                .packedField(FEATURE_GEOMETRY, 9, 0, 0, 26, 0, 20, 20, 0, 0, 19, 15)
                .toByteArray();
        VectorTile source = new VectorTileCodec().decode(tileWithLayer(1, polygon));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("polygons").merge(onlyLayer(source)).build();

        assertThat(builder.build())
                .layer("polygons")
                .firstFeature()
                .geometry()
                .moveTo(0, 0)
                .lineTo(10, 0)
                .lineTo(10, 10)
                .lineTo(0, 10)
                .closePath()
                .matches();
    }

    // Automatic ids are on: the merged feature without an id stays without one
    @Test
    void mergeKeepsAbsentIds() throws IOException {
        byte[] withIdZero = new ProtoWriter()
                .varintField(FEATURE_ID, 0)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        VectorTile source = new VectorTileCodec().decode(tileWithLayer(2, pointWithoutId(), withIdZero));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("points").merge(onlyLayer(source)).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(null, 0L);
    }

    // Two packed runs and a single varint, with a repeated point: encoding the line again would leave the point out
    @Test
    void mergeJoinsGeometryRuns() throws IOException {
        // MoveTo(2 3), LineTo(2 3, 4 5)
        byte[] splitLine = new ProtoWriter()
                .varintField(FEATURE_TYPE, LINESTRING)
                .packedField(FEATURE_GEOMETRY, 9, 4, 6)
                .varintField(FEATURE_GEOMETRY, 18)
                .packedField(FEATURE_GEOMETRY, 0, 0, 4, 4)
                .toByteArray();
        VectorTile source = new VectorTileCodec().decode(tileWithLayer(2, splitLine));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("lines").merge(onlyLayer(source)).build();

        byte[] bytes = builder.toByteArray();
        VectorTileProto.Tile.Feature merged =
                VectorTileProto.Tile.parseFrom(bytes).getLayers(0).getFeatures(0);
        Assertions.assertThat(merged.getGeometryList()).containsExactly(9, 4, 6, 18, 0, 0, 4, 4);
        assertProtobufEncoding(bytes);
    }

    // No geometry field in the first feature, an empty one in the second, and a MoveTo without points in the third
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void mergeLeavesOutEmptyGeometry(int version) throws IOException {
        byte[] withoutGeometry =
                new ProtoWriter().varintField(FEATURE_TYPE, POINT).toByteArray();
        byte[] emptyGeometry = new ProtoWriter()
                .varintField(FEATURE_TYPE, POINT)
                .bytesField(FEATURE_GEOMETRY, new byte[0])
                .toByteArray();
        byte[] withoutPoints = new ProtoWriter()
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 1)
                .toByteArray();
        byte[] tile = tileWithLayer(version, withoutGeometry, emptyGeometry, withoutPoints, pointWithoutId());
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer()
                .name("points")
                .merge(onlyLayer(new VectorTileCodec().decode(tile)))
                .build();

        assertThat(builder.build())
                .layer("points")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(3, 6)
                .matches();
    }

    // MoveTo announces one point and gives one parameter: the codec decodes the geometry as empty
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void mergeLeavesOutMalformedGeometry(int version) throws IOException {
        byte[] malformed = new ProtoWriter()
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6)
                .toByteArray();
        byte[] tile = tileWithLayer(version, malformed, pointWithoutId());
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer()
                .name("points")
                .merge(onlyLayer(new VectorTileCodec().decode(tile)))
                .build();

        VectorTile merged = VectorTileCodec.failingOnMalformedGeometry().decode(builder.toByteArray());
        assertThat(merged)
                .layer("points")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(3, 6)
                .matches();
    }

    // The polygon starts with a ring of negative area: by version 2 of the specification, a hole without its polygon
    @Test
    void mergeOrientsReversedPolygon() throws IOException {
        byte[] reversed = new ProtoWriter()
                .varintField(FEATURE_TYPE, POLYGON)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12, 18, 34, 56, 23, 43, 15)
                .toByteArray();
        Layer source = onlyLayer(new VectorTileCodec().decode(tileWithLayer(2, reversed)));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("polygons").merge(source).build();

        assertThat(builder.build())
                .layer("polygons")
                .firstFeature()
                .geometry()
                .moveTo(3, 6)
                .lineTo(8, 12)
                .lineTo(20, 34)
                .closePath()
                .matches();
    }

    // The layer has a feature before the merge, and the tags of the second merged feature point past the keys
    @Test
    void rollsBackMergeOnFailure() throws IOException {
        byte[] badTags = new ProtoWriter()
                .packedField(FEATURE_TAGS, 0, 0)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        Layer source = onlyLayer(new VectorTileCodec().decode(tileWithLayer(2, pointWithoutId(), badTags)));
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder layer = builder.layer().name("points");
        layer.feature(Map.of(), geom("POINT(1 2)"));

        assertThatThrownBy(() -> layer.merge(source)).isInstanceOf(IllegalArgumentException.class);
        layer.feature(Map.of(), geom("POINT(5 7)")).build();

        assertThat(builder.build())
                .layer("points")
                .hasFeatureCount(2)
                .firstFeature()
                .geometry()
                .moveTo(1, 2)
                .matches()
                .layer()
                .feature(1)
                .geometry()
                .moveTo(5, 7)
                .matches();
    }

    // The layer of another implementation fails on the geometry of its second feature with an Error
    @Test
    void mergeErrorRollsBackLayer() {
        Feature first = mock(Feature.class);
        when(first.getGeometry()).thenReturn(geom("POINT(1 1)"));
        Feature second = mock(Feature.class);
        when(second.getGeometry()).thenThrow(new AssertionError("the second feature"));
        Layer failing = mock(Layer.class);
        when(failing.getExtent()).thenReturn(4096);
        when(failing.getFeatures()).thenReturn(Stream.of(first, second));
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder layer = builder.layer().name("points");

        assertThatThrownBy(() -> layer.merge(failing)).isInstanceOf(AssertionError.class);
        layer.feature(Map.of(), geom("POINT(5 7)")).build();

        assertThat(builder.build())
                .layer("points")
                .hasFeatureCount(1)
                .firstFeature()
                .geometry()
                .moveTo(5, 7)
                .matches();
    }

    // The feature changes the extent while it is merged: its point would be written at extent 256, unscaled
    @Test
    void failsOnExtentFromMergedLayer() {
        VectorTileBuilder builder = new VectorTileBuilder();
        Feature changingExtent = mock(Feature.class);
        when(changingExtent.getGeometry()).thenAnswer(invocation -> {
            builder.setExtent(256);
            return geom("POINT(2048 2048)");
        });
        Layer layer = mock(Layer.class);
        when(layer.getExtent()).thenReturn(4096);
        when(layer.getFeatures()).thenReturn(Stream.of(changingExtent));
        LayerBuilder points = builder.layer().name("points");

        assertThatThrownBy(() -> points.merge(layer)).isInstanceOf(IllegalStateException.class);
        Assertions.assertThat(builder.extent()).isEqualTo(4096);
    }

    // The layer changes an id setting while it is merged: the setting would stay changed after a failed merge
    @ParameterizedTest
    @MethodSource("idSettings")
    void failsOnIdSettingFromMergedLayer(Consumer<VectorTileBuilder> idSetting) throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        Feature changingIds = mock(Feature.class);
        when(changingIds.getGeometry()).thenAnswer(invocation -> {
            idSetting.accept(builder);
            return geom("POINT(1 1)");
        });
        Layer layer = mock(Layer.class);
        when(layer.getExtent()).thenReturn(4096);
        when(layer.getFeatures()).thenReturn(Stream.of(changingIds));
        LayerBuilder points = builder.layer().name("points");

        assertThatThrownBy(() -> points.merge(layer)).isInstanceOf(IllegalStateException.class);
        points.feature(Map.of(), geom("POINT(3 6)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    static Stream<Named<Consumer<VectorTileBuilder>>> idSettings() {
        return Stream.of(
                Named.of("setFirstFeatureId", builder -> builder.setFirstFeatureId(100)),
                Named.of("setAutoIncrementIds", builder -> builder.setAutoIncrementIds(false)));
    }

    // The first feature has the largest id, and the tags of the second one point past the keys of the layer
    @Test
    void failedMergeKeepsAutomaticIds() throws IOException {
        byte[] largestId = new ProtoWriter()
                .varintField(FEATURE_ID, Long.MAX_VALUE)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        byte[] badTags = new ProtoWriter()
                .packedField(FEATURE_TAGS, 0, 0)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        Layer source = onlyLayer(new VectorTileCodec().decode(tileWithLayer(2, largestId, badTags)));
        VectorTileBuilder builder = new VectorTileBuilder();
        LayerBuilder layer = builder.layer().name("points");

        assertThatThrownBy(() -> layer.merge(source)).isInstanceOf(IllegalArgumentException.class);
        layer.feature(Map.of(), geom("POINT(5 7)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    // The names are checked first: the first layer of the tile is not merged either
    @Test
    void mergeTileFailsOnWrittenName() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer().name("water").feature(Map.of(), geom("POINT(1 1)")).build();
        source.layer().name("roads").feature(Map.of(), geom("POINT(2 2)")).build();
        VectorTile tile = source.build();
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)")).build();

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalArgumentException.class);
        assertThat(builder.build()).hasExactLayers("roads");
    }

    // The builder stays usable: the check comes before the features of the layer are merged
    @Test
    void mergeTileFailsOnUnnamedLayer() throws IOException {
        ProtoWriter unnamed = new ProtoWriter()
                .varintField(LAYER_VERSION, 2)
                .stringField(LAYER_NAME, "")
                .bytesField(LAYER_FEATURE, pointWithoutId());
        byte[] bytes =
                new ProtoWriter().bytesField(TILE_LAYER, unnamed.toByteArray()).toByteArray();
        VectorTile tile = new VectorTileCodec().decode(bytes);
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalArgumentException.class);
        builder.layer().name("points").feature(Map.of(), geom("POINT(5 7)")).build();

        assertThat(builder.build()).hasExactLayers("points");
    }

    // The first layer of the name is not merged either, by merge or by init
    @Test
    void mergeTileFailsOnRepeatedName() throws IOException {
        byte[] layer = new ProtoWriter()
                .varintField(LAYER_VERSION, 2)
                .stringField(LAYER_NAME, "points")
                .bytesField(LAYER_FEATURE, pointWithoutId())
                .toByteArray();
        byte[] bytes = new ProtoWriter()
                .bytesField(TILE_LAYER, layer)
                .bytesField(TILE_LAYER, layer)
                .toByteArray();
        VectorTile tile = new VectorTileCodec().decode(bytes);
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.init(tile)).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThat(builder.build().getLayers()).isEmpty();
    }

    // The names are checked first: the first layer of the tile is not merged either
    @Test
    void mergeTileFailsOnUnpairedSurrogate() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer().name("water").feature(Map.of(), geom("POINT(1 1)")).build();
        Layer water = onlyLayer(source.build());
        Layer unpaired = mock(Layer.class);
        when(unpaired.getName()).thenReturn("roads\uD800");
        when(unpaired.getExtent()).thenReturn(4096);
        when(unpaired.getFeatures()).thenReturn(Stream.empty());
        VectorTile tile = mock(VectorTile.class);
        when(tile.getLayers()).thenReturn(List.of(water, unpaired));
        VectorTileBuilder builder = new VectorTileBuilder();

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThat(builder.build().getLayers()).isEmpty();
    }

    @Test
    void mergeTileFailsWhileLayerOpen() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer().name("water").feature(Map.of(), geom("POINT(1 1)")).build();
        VectorTile tile = source.build();
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)"));

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> builder.init(tile)).isInstanceOf(IllegalStateException.class);
    }

    // The names are checked before the open layer without features is closed
    @Test
    void failedMergeKeepsOpenLayer() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer().name("roads").feature(Map.of(), geom("POINT(2 2)")).build();
        VectorTile tile = source.build();
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)")).build();
        LayerBuilder open = builder.layer().name("pois");

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.init(tile)).isInstanceOf(IllegalArgumentException.class);
        open.feature(Map.of(), geom("POINT(5 7)")).build();

        assertThat(builder.build()).hasExactLayers("roads", "pois");
    }

    // The layer changes a setting while its name is read: the failed check would leave the setting changed
    @Test
    void failsOnSettingFromLayerName() throws IOException {
        VectorTileBuilder builder = new VectorTileBuilder();
        Layer changingIds = pointLayer("roads");
        when(changingIds.getName()).thenAnswer(invocation -> {
            builder.setAutoIncrementIds(false);
            return "";
        });
        VectorTile tile = mock(VectorTile.class);
        when(tile.getLayers()).thenReturn(List.of(changingIds));

        assertThatThrownBy(() -> builder.merge(tile)).isInstanceOf(IllegalStateException.class);
        builder.layer().name("points").feature(Map.of(), geom("POINT(3 6)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(0L);
    }

    // The second layer loses its name once checked: the merge would write the first layer, then fail
    @Test
    void mergeWritesCheckedNames() {
        Layer water = pointLayer("water");
        Layer roads = pointLayer("roads");
        when(roads.getName()).thenReturn("roads", "");
        VectorTile tile = mock(VectorTile.class);
        when(tile.getLayers()).thenReturn(List.of(water, roads));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.merge(tile);

        assertThat(builder.build()).hasExactLayers("water", "roads");
    }

    @Test
    void failsOnMergeAfterLayerBuild() {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer().name("water").feature(Map.of(), geom("POINT(1 1)")).build();
        Layer water = onlyLayer(source.build());
        LayerBuilder layer = new VectorTileBuilder().layer().name("water");
        layer.build();

        assertThatThrownBy(() -> layer.merge(water)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void mergeContinuesAutomaticIds() throws IOException {
        VectorTileBuilder source = new VectorTileBuilder();
        source.layer()
                .name("pois")
                .feature(Map.of(), geom("POINT(1 1)"), 5)
                .feature(Map.of(), geom("POINT(2 2)"), 9)
                .build();
        VectorTileBuilder builder = new VectorTileBuilder().merge(source.build());

        builder.layer().name("more").feature(Map.of(), geom("POINT(3 3)")).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(5L, 9L, 10L);
    }

    // The id 2^64 - 1 reads as the long -1, and the automatic ids stay below 2^63
    @Test
    void unsignedIdLeavesAutomaticIds() throws IOException {
        byte[] withLargestId = new ProtoWriter()
                .varintField(FEATURE_ID, -1)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        Layer source = onlyLayer(new VectorTileCodec().decode(tileWithLayer(2, withLargestId)));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer()
                .name("source")
                .merge(source)
                .feature(Map.of(), geom("POINT(5 7)"))
                .build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(-1L, 0L);
    }

    // A layer added after init has the extent of the tile too
    @Test
    void initTakesExtentOfTile() {
        VectorTileBuilder source = new VectorTileBuilder().setExtent(512);
        source.layer().name("a").feature(Map.of(), geom("POINT(1 1)")).build();
        source.layer().name("b").feature(Map.of(), geom("POINT(2 2)")).build();
        VectorTileBuilder builder = new VectorTileBuilder().init(source.build());

        ClipStep clip = (ClipStep) builder.pipeline().steps().get(1);
        builder.layer().name("c").feature(Map.of(), geom("POINT(3 3)")).build();

        Assertions.assertThat(clip.extent()).isEqualTo(512.0);
        Assertions.assertThat(builder.build().getLayers())
                .extracting(Layer::getExtent)
                .containsExactly(512, 512, 512);
    }

    // The steps of a given pipeline are the caller's: init takes the extent of the tile and leaves them as given
    @Test
    void initKeepsGivenPipeline() {
        GeometryPipeline given = GeometryPipeline.of(new SizeFilterStep(1, 1));
        VectorTileBuilder builder = new VectorTileBuilder().setPipeline(given);

        builder.init(build("a", 512, geom("POINT(1 1)")));

        Assertions.assertThat(builder.pipeline()).isSameAs(given);
        Assertions.assertThat(builder.build().getLayers())
                .extracting(Layer::getExtent)
                .containsExactly(512);
    }

    // A setting after setPipeline derives the pipeline again: init derives it for the extent of the tile
    @Test
    void initDerivesAfterLaterSetting() {
        VectorTileBuilder builder =
                new VectorTileBuilder().setPipeline(GeometryPipeline.empty()).setClipBuffer(8);

        builder.init(build("a", 512, geom("POINT(1 1)")));

        ClipStep clip = (ClipStep) builder.pipeline().steps().get(1);
        Assertions.assertThat(clip.extent()).isEqualTo(512.0);
    }

    // The point of the layer of extent 512 is scaled to the extent of the builder
    @Test
    void initKeepsExtentWhenLayersDisagree() {
        VectorTileBuilder source = new VectorTileBuilder().setExtent(512);
        source.layer().name("a").feature(Map.of(), geom("POINT(1 1)")).build();
        source.setExtent(256)
                .layer()
                .name("b")
                .feature(Map.of(), geom("POINT(2 2)"))
                .build();

        VectorTileBuilder builder = new VectorTileBuilder().init(source.build());

        assertThat(builder.build())
                .layer("a")
                .hasExtent(4096)
                .firstFeature()
                .geometry()
                .moveTo(8, 8)
                .matches();
    }

    @Test
    void initOnBuilderWithLayersMergesOnly() {
        VectorTileBuilder source = new VectorTileBuilder().setExtent(512);
        source.layer().name("pois").feature(Map.of(), geom("POINT(1 1)")).build();
        VectorTileBuilder builder = new VectorTileBuilder();
        builder.layer().name("roads").feature(Map.of(), geom("POINT(3 6)")).build();

        builder.init(source.build());

        assertThat(builder.build())
                .hasExactLayers("roads", "pois")
                .layer("pois")
                .hasExtent(4096)
                .firstFeature()
                .geometry()
                .moveTo(8, 8)
                .matches();
    }

    // The model reads a feature without an id as 0: another implementation cannot tell them apart
    @Test
    void mergesOtherImplementationWithIdZeroAsNone() throws IOException {
        VectorTileBuilder source = new VectorTileBuilder().setAutoIncrementIds(false);
        source.layer()
                .name("pois")
                .feature(Map.of(), geom("POINT(1 1)"), 0)
                .feature(Map.of(), geom("POINT(2 2)"), 7)
                .build();
        VectorTile other = new DelegatingTile(source.build());
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("pois").merge(onlyLayer(other)).build();

        Assertions.assertThat(writtenIds(builder.toByteArray())).containsExactly(null, 7L);
    }

    // The model returns an id of 2^63 or above as a negative long: -1 stands for the id 2^64 - 1
    @Test
    void mergesNegativeModelIdUnsigned() throws IOException {
        byte[] withLargestId = new ProtoWriter()
                .varintField(FEATURE_ID, -1)
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
        VectorTile other = new DelegatingTile(new VectorTileCodec().decode(tileWithLayer(2, withLargestId)));
        VectorTileBuilder builder = new VectorTileBuilder();

        builder.layer().name("source").merge(onlyLayer(other)).build();

        MvtFeature merged = (MvtFeature)
                onlyLayer(builder.build()).getFeatures().findFirst().orElseThrow();
        Assertions.assertThat(merged.hasId()).isTrue();
        Assertions.assertThat(merged.getId()).isEqualTo(-1L);
    }

    private static Layer onlyLayer(VectorTile tile) {
        List<Layer> layers = tile.getLayers();
        Assertions.assertThat(layers).hasSize(1);
        return layers.get(0);
    }

    /** A layer of another implementation, of extent 4096, with one point. */
    private Layer pointLayer(String name) {
        Feature point = mock(Feature.class);
        when(point.getGeometry()).thenReturn(geom("POINT(1 1)"));
        Layer layer = mock(Layer.class);
        when(layer.getName()).thenReturn(name);
        when(layer.getExtent()).thenReturn(4096);
        when(layer.getFeatures()).thenAnswer(invocation -> Stream.of(point));
        return layer;
    }

    /** Wraps the features in layer "source" of the version, without an extent field: the extent is 4096. */
    private static byte[] tileWithLayer(int version, byte[]... features) {
        ProtoWriter layer =
                new ProtoWriter().varintField(LAYER_VERSION, version).stringField(LAYER_NAME, "source");
        for (byte[] feature : features) {
            layer.bytesField(LAYER_FEATURE, feature);
        }
        return new ProtoWriter().bytesField(TILE_LAYER, layer.toByteArray()).toByteArray();
    }

    /** Feature with type POINT and MoveTo(3, 6), without id and tags. */
    private static byte[] pointWithoutId() {
        return new ProtoWriter()
                .varintField(FEATURE_TYPE, POINT)
                .packedField(FEATURE_GEOMETRY, 9, 6, 12)
                .toByteArray();
    }

    private static List<DecodedFeature> decodedFeatures(Layer layer) {
        return layer.getFeatures().map(DecodedFeature::of).toList();
    }

    /** A feature as decoded. The geometries compare with {@code equalsExact}, as {@link Geometry#equals} does. */
    private record DecodedFeature(long id, Map<String, Object> attributes, Geometry geometry) {

        static DecodedFeature of(Feature feature) {
            return new DecodedFeature(feature.getId(), feature.getAttributes(), feature.getGeometry());
        }
    }
}
