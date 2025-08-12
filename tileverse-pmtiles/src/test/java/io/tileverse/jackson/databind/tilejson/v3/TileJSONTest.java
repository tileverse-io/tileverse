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
package io.tileverse.jackson.databind.tilejson.v3;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for TileJSON v3.0.0 specification compliance.
 */
class TileJSONTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testMinimalTileJSON() throws Exception {
        // Create minimal valid TileJSON
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        assertEquals("3.0.0", tileJSON.tilejson());
        assertEquals(1, tileJSON.tiles().size());
        assertEquals("https://example.com/{z}/{x}/{y}.pbf", tileJSON.tiles().get(0));
        assertEquals(1, tileJSON.vectorLayers().size());
        assertEquals("test", tileJSON.vectorLayers().get(0).id());

        // Test serialization
        String json = objectMapper.writeValueAsString(tileJSON);
        assertTrue(json.contains("\"tilejson\":\"3.0.0\""));
        assertTrue(json.contains("\"tiles\":[\"https://example.com/{z}/{x}/{y}.pbf\"]"));
        assertTrue(json.contains("\"vector_layers\""));

        // Test deserialization
        TileJSON deserialized = objectMapper.readValue(json, TileJSON.class);
        assertEquals(tileJSON.tilejson(), deserialized.tilejson());
        assertEquals(tileJSON.tiles(), deserialized.tiles());
        assertEquals(
                tileJSON.vectorLayers().get(0).id(),
                deserialized.vectorLayers().get(0).id());
    }

    @Test
    void testCompleteTileJSON() throws Exception {
        VectorLayer buildings = VectorLayer.of(
                        "buildings", Map.of("height", "Number", "type", "String"), "Building footprints")
                .withZoomRange(12, 18);
        VectorLayer roads = VectorLayer.of("roads", Map.of("name", "String", "highway", "String"), "Road network")
                .withZoomRange(6, 18);

        TileJSON tileJSON = TileJSON.of(
                        "3.0.0",
                        List.of(
                                "https://tile1.example.com/{z}/{x}/{y}.pbf",
                                "https://tile2.example.com/{z}/{x}/{y}.pbf"),
                        List.of(buildings, roads),
                        "Test Tileset",
                        "A comprehensive test tileset",
                        "© Test Contributors")
                .withBounds(-180, -85, 180, 85)
                .withCenter(-74.0059, 40.7128, 10.0)
                .withZoomRange(0, 18)
                .withVersion("1.0.0");

        // Verify all fields
        assertEquals("3.0.0", tileJSON.tilejson());
        assertEquals("Test Tileset", tileJSON.name());
        assertEquals("A comprehensive test tileset", tileJSON.description());
        assertEquals("© Test Contributors", tileJSON.attribution());
        assertEquals("1.0.0", tileJSON.version());

        assertEquals(2, tileJSON.tiles().size());
        assertEquals(
                "https://tile1.example.com/{z}/{x}/{y}.pbf", tileJSON.tiles().get(0));
        assertEquals(
                "https://tile2.example.com/{z}/{x}/{y}.pbf", tileJSON.tiles().get(1));

        assertEquals(4, tileJSON.bounds().size());
        assertEquals(-180.0, tileJSON.bounds().get(0));
        assertEquals(-85.0, tileJSON.bounds().get(1));
        assertEquals(180.0, tileJSON.bounds().get(2));
        assertEquals(85.0, tileJSON.bounds().get(3));

        assertEquals(3, tileJSON.center().size());
        assertEquals(-74.0059, tileJSON.center().get(0));
        assertEquals(40.7128, tileJSON.center().get(1));
        assertEquals(10.0, tileJSON.center().get(2));

        assertEquals(Integer.valueOf(0), tileJSON.minzoom());
        assertEquals(Integer.valueOf(18), tileJSON.maxzoom());

        assertEquals(2, tileJSON.vectorLayers().size());
        assertEquals("buildings", tileJSON.vectorLayers().get(0).id());
        assertEquals("roads", tileJSON.vectorLayers().get(1).id());

        // Test serialization/deserialization
        String json = objectMapper.writeValueAsString(tileJSON);
        TileJSON deserialized = objectMapper.readValue(json, TileJSON.class);

        assertEquals(tileJSON.tilejson(), deserialized.tilejson());
        assertEquals(tileJSON.name(), deserialized.name());
        assertEquals(tileJSON.description(), deserialized.description());
        assertEquals(tileJSON.attribution(), deserialized.attribution());
        assertEquals(tileJSON.bounds(), deserialized.bounds());
        assertEquals(tileJSON.center(), deserialized.center());
        assertEquals(tileJSON.minzoom(), deserialized.minzoom());
        assertEquals(tileJSON.maxzoom(), deserialized.maxzoom());
        assertEquals(tileJSON.vectorLayers().size(), deserialized.vectorLayers().size());
    }

    @Test
    void testRequiredFieldValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));

        // Test null tilejson
        assertThrows(
                IllegalArgumentException.class,
                () -> TileJSON.of(null, List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer)));

        // Test blank tilejson
        assertThrows(
                IllegalArgumentException.class,
                () -> TileJSON.of("", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer)));

        // Test null tiles
        assertThrows(IllegalArgumentException.class, () -> TileJSON.of("3.0.0", null, List.of(layer)));

        // Test empty tiles
        assertThrows(IllegalArgumentException.class, () -> TileJSON.of("3.0.0", List.of(), List.of(layer)));

        // Test null tile URL
        assertThrows(
                IllegalArgumentException.class,
                () -> TileJSON.of("3.0.0", java.util.Arrays.asList((String) null), List.of(layer)));

        // Test blank tile URL
        assertThrows(IllegalArgumentException.class, () -> TileJSON.of("3.0.0", List.of(""), List.of(layer)));

        // Test null vector layers
        assertThrows(
                IllegalArgumentException.class,
                () -> TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), null));
    }

    @Test
    void testBoundsValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        // Test invalid bounds (west >= east)
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(10, -85, 10, 85));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(10, -85, -10, 85));

        // Test invalid bounds (south >= north)
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(-180, 85, 180, 85));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(-180, 85, 180, -85));

        // Test out of range bounds
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(-181, -85, 180, 85));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(-180, -91, 180, 85));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(-180, -85, 181, 85));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withBounds(-180, -85, 180, 91));
    }

    @Test
    void testCenterValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        // Test invalid longitude
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withCenter(-181, 0, null));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withCenter(181, 0, null));

        // Test invalid latitude
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withCenter(0, -91, null));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withCenter(0, 91, null));

        // Test invalid zoom
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withCenter(0, 0, -1.0));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withCenter(0, 0, 31.0));

        // Test valid centers
        assertDoesNotThrow(() -> tileJSON.withCenter(-180, -90, null));
        assertDoesNotThrow(() -> tileJSON.withCenter(180, 90, null));
        assertDoesNotThrow(() -> tileJSON.withCenter(0, 0, 0.0));
        assertDoesNotThrow(() -> tileJSON.withCenter(0, 0, 30.0));
    }

    @Test
    void testZoomValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        // Test invalid zoom levels
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withZoomRange(-1, 10));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withZoomRange(0, 31));
        assertThrows(IllegalArgumentException.class, () -> tileJSON.withZoomRange(10, 5));

        // Test valid zoom ranges
        assertDoesNotThrow(() -> tileJSON.withZoomRange(0, 0));
        assertDoesNotThrow(() -> tileJSON.withZoomRange(0, 30));
        assertDoesNotThrow(() -> tileJSON.withZoomRange(null, 10));
        assertDoesNotThrow(() -> tileJSON.withZoomRange(5, null));
    }

    @Test
    void testJSONDeserialization() throws Exception {
        String json =
                """
            {
                "tilejson": "3.0.0",
                "tiles": ["https://example.com/{z}/{x}/{y}.pbf"],
                "vector_layers": [
                    {
                        "id": "roads",
                        "fields": {
                            "name": "String",
                            "highway": "String"
                        },
                        "description": "Road network",
                        "minzoom": 6,
                        "maxzoom": 18
                    }
                ],
                "name": "Test Tileset",
                "description": "A test tileset",
                "attribution": "© Test",
                "bounds": [-180, -85, 180, 85],
                "center": [-74, 40.7, 10],
                "minzoom": 0,
                "maxzoom": 18,
                "version": "1.0.0",
                "unknown_property": "should be ignored"
            }
            """;

        TileJSON tileJSON = objectMapper.readValue(json, TileJSON.class);

        assertEquals("3.0.0", tileJSON.tilejson());
        assertEquals("Test Tileset", tileJSON.name());
        assertEquals("A test tileset", tileJSON.description());
        assertEquals("© Test", tileJSON.attribution());
        assertEquals("1.0.0", tileJSON.version());

        assertEquals(1, tileJSON.tiles().size());
        assertEquals("https://example.com/{z}/{x}/{y}.pbf", tileJSON.tiles().get(0));

        assertEquals(4, tileJSON.bounds().size());
        assertEquals(-180.0, tileJSON.bounds().get(0));

        assertEquals(3, tileJSON.center().size());
        assertEquals(-74.0, tileJSON.center().get(0));
        assertEquals(40.7, tileJSON.center().get(1));
        assertEquals(10.0, tileJSON.center().get(2));

        assertEquals(Integer.valueOf(0), tileJSON.minzoom());
        assertEquals(Integer.valueOf(18), tileJSON.maxzoom());

        assertEquals(1, tileJSON.vectorLayers().size());
        VectorLayer layer = tileJSON.vectorLayers().get(0);
        assertEquals("roads", layer.id());
        assertEquals("Road network", layer.description());
        assertEquals(Integer.valueOf(6), layer.minZoom());
        assertEquals(Integer.valueOf(18), layer.maxZoom());
        assertEquals("String", layer.fields().get("name"));
        assertEquals("String", layer.fields().get("highway"));
    }

    @Test
    void testBuilderMethods() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON original = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        // Test all builder methods
        TileJSON modified = original.withName("New Name")
                .withDescription("New Description")
                .withAttribution("New Attribution")
                .withVersion("2.0.0")
                .withBounds(-90, -45, 90, 45)
                .withCenter(0, 0, 5.0)
                .withZoomRange(2, 14);

        assertEquals("New Name", modified.name());
        assertEquals("New Description", modified.description());
        assertEquals("New Attribution", modified.attribution());
        assertEquals("2.0.0", modified.version());
        assertEquals(List.of(-90.0, -45.0, 90.0, 45.0), modified.bounds());
        assertEquals(List.of(0.0, 0.0, 5.0), modified.center());
        assertEquals(Integer.valueOf(2), modified.minzoom());
        assertEquals(Integer.valueOf(14), modified.maxzoom());

        // Original should be unchanged (immutability)
        assertNull(original.name());
        assertNull(original.description());
        assertNull(original.attribution());
    }
}
