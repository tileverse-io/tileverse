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

import io.tileverse.vectortile.model.VectorTile;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class VectorTileBuilderTest {

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
                .moveTo(160, 320)
                .lineTo(320, 320)
                .lineTo(320, 160)
                .lineTo(160, 160)
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
    @Disabled("we're not doing autoscaling anymore, waiting for a different approach")
    void testExtentScaling() {
        // Test coordinate scaling with different extents
        Geometry geometry = geom("POINT(3 6)");

        // Test with extent 512 instead of 256 - coordinates should be scaled by 512/256 = 2x
        VectorTileBuilder vtb = new VectorTileBuilder().setExtent(512);
        LayerBuilder layer = vtb.layer().name("SCALING");
        layer.feature(Map.of(), geometry);
        VectorTile tile = layer.build().build();

        assertThat(tile)
                .layer("SCALING")
                .hasExtent(512)
                .firstFeature()
                .geometry()
                .isPoint()
                .moveTo(6, 12) // 3*2=6, 6*2=12 due to 512 extent
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
                .moveTo(20, 34)
                .lineTo(8, 12)
                .lineTo(3, 6)
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
                .isNormalized()
                .equalsNorm(polygon)
                .moveTo(20, 34)
                .lineTo(8, 12)
                .lineTo(3, 6)
                .closePath()
                .matches();
    }

    @Test
    void testPolygonCounterClockwiseWithHole() {
        // Test polygon with counter-clockwise exterior ring and clockwise interior ring
        Geometry polygon = geom(
                """
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
                .equalsExact(
                        """
                        POLYGON(
                          (0 0, 0 10, 10 10, 10 0, 0 0),
                          (7 3, 7 7, 3 7, 3 3, 7 3)
                        )
                        """)
                .moveTo(0, 0)
                .lineTo(0, 10)
                .lineTo(10, 10)
                .lineTo(10, 0)
                .closePath()
                .moveTo(7, 3)
                .lineTo(7, 7)
                .lineTo(3, 7)
                .lineTo(3, 3)
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
        Geometry geomCollection = geom(
                """
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

        // According to GeometryEncoder.prepareGeometries(), the generic collection
        // should be decomposed into 3 separate features with identical attributes
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
                // Third feature: POLYGON - normalized coordinates after precision snapping
                .feature(2)
                .hasAttribute("source", "collection")
                .hasAttribute("type", "mixed")
                .geometry()
                .isPolygon()
                .moveTo(70, 100)
                .lineTo(90, 100)
                .lineTo(90, 80)
                .lineTo(70, 80)
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
}
