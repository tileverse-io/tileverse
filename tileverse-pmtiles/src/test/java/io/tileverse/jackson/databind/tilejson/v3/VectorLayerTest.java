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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for VectorLayer following TileJSON v3.0.0 specification.
 */
class VectorLayerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testMinimalVectorLayer() throws Exception {
        Map<String, String> fields = Map.of("name", "String", "type", "String");
        VectorLayer layer = VectorLayer.of("buildings", fields);

        assertEquals("buildings", layer.id());
        assertEquals(fields, layer.fields());
        assertNull(layer.description());
        assertNull(layer.minZoom());
        assertNull(layer.maxZoom());

        // Test serialization
        String json = objectMapper.writeValueAsString(layer);
        assertTrue(json.contains("\"id\":\"buildings\""));
        assertTrue(json.contains("\"fields\":{"));
        assertTrue(json.contains("\"name\":\"String\""));

        // Test deserialization
        VectorLayer deserialized = objectMapper.readValue(json, VectorLayer.class);
        assertEquals(layer.id(), deserialized.id());
        assertEquals(layer.fields(), deserialized.fields());
    }

    @Test
    void testCompleteVectorLayer() throws Exception {
        Map<String, String> fields = Map.of(
                "name", "String",
                "height", "Number",
                "residential", "Boolean");

        VectorLayer layer = VectorLayer.of("buildings", fields, "Building footprints", 12, 18);

        assertEquals("buildings", layer.id());
        assertEquals(fields, layer.fields());
        assertEquals("Building footprints", layer.description());
        assertEquals(Integer.valueOf(12), layer.minZoom());
        assertEquals(Integer.valueOf(18), layer.maxZoom());

        // Test serialization/deserialization
        String json = objectMapper.writeValueAsString(layer);
        VectorLayer deserialized = objectMapper.readValue(json, VectorLayer.class);

        assertEquals(layer.id(), deserialized.id());
        assertEquals(layer.fields(), deserialized.fields());
        assertEquals(layer.description(), deserialized.description());
        assertEquals(layer.minZoom(), deserialized.minZoom());
        assertEquals(layer.maxZoom(), deserialized.maxZoom());
    }

    @Test
    void testRequiredFieldValidation() {
        Map<String, String> fields = Map.of("name", "String");

        // Test null id
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of(null, fields));

        // Test blank id
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("", fields));
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("   ", fields));

        // Test null fields
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("test", null));

        // Test valid creation
        assertDoesNotThrow(() -> VectorLayer.of("test", fields));
    }

    @Test
    void testZoomValidation() {
        Map<String, String> fields = Map.of("name", "String");

        // Test invalid minZoom
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("test", fields, null, -1, 10));
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("test", fields, null, 31, 10));

        // Test invalid maxZoom
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("test", fields, null, 5, -1));
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("test", fields, null, 5, 31));

        // Test minZoom > maxZoom
        assertThrows(IllegalArgumentException.class, () -> VectorLayer.of("test", fields, null, 10, 5));

        // Test valid zoom ranges
        assertDoesNotThrow(() -> VectorLayer.of("test", fields, null, 0, 30));
        assertDoesNotThrow(() -> VectorLayer.of("test", fields, null, 5, 5));
        assertDoesNotThrow(() -> VectorLayer.of("test", fields, null, null, 10));
        assertDoesNotThrow(() -> VectorLayer.of("test", fields, null, 5, null));
    }

    @Test
    void testBuilderMethods() {
        Map<String, String> originalFields = Map.of("name", "String");
        Map<String, String> newFields = Map.of("name", "String", "type", "String");

        VectorLayer original = VectorLayer.of("test", originalFields);

        // Test builder methods
        VectorLayer modified = original.withDescription("Test description")
                .withZoomRange(5, 15)
                .withFields(newFields);

        assertEquals("test", modified.id()); // id should remain the same
        assertEquals("Test description", modified.description());
        assertEquals(Integer.valueOf(5), modified.minZoom());
        assertEquals(Integer.valueOf(15), modified.maxZoom());
        assertEquals(newFields, modified.fields());

        // Original should be unchanged (immutability)
        assertNull(original.description());
        assertNull(original.minZoom());
        assertNull(original.maxZoom());
        assertEquals(originalFields, original.fields());
    }

    @Test
    void testBuilderValidation() {
        Map<String, String> fields = Map.of("name", "String");
        VectorLayer layer = VectorLayer.of("test", fields);

        // Test zoom range validation in builder methods
        assertThrows(IllegalArgumentException.class, () -> layer.withZoomRange(-1, 10));
        assertThrows(IllegalArgumentException.class, () -> layer.withZoomRange(10, 31));
        assertThrows(IllegalArgumentException.class, () -> layer.withZoomRange(15, 5));

        // Test fields validation in builder method
        assertThrows(IllegalArgumentException.class, () -> layer.withFields(null));
    }

    @Test
    void testJSONDeserialization() throws Exception {
        String json =
                """
            {
                "id": "roads",
                "fields": {
                    "name": "String",
                    "highway": "String",
                    "oneway": "Boolean",
                    "lanes": "Number"
                },
                "description": "Road network data",
                "minzoom": 6,
                "maxzoom": 18,
                "unknown_property": "should be ignored"
            }
            """;

        VectorLayer layer = objectMapper.readValue(json, VectorLayer.class);

        assertEquals("roads", layer.id());
        assertEquals("Road network data", layer.description());
        assertEquals(Integer.valueOf(6), layer.minZoom());
        assertEquals(Integer.valueOf(18), layer.maxZoom());

        assertNotNull(layer.fields());
        assertEquals(4, layer.fields().size());
        assertEquals("String", layer.fields().get("name"));
        assertEquals("String", layer.fields().get("highway"));
        assertEquals("Boolean", layer.fields().get("oneway"));
        assertEquals("Number", layer.fields().get("lanes"));
    }

    @Test
    void testEmptyFields() throws Exception {
        // Test with empty fields map (valid per TileJSON spec)
        Map<String, String> emptyFields = Map.of();
        VectorLayer layer = VectorLayer.of("buildings", emptyFields);

        assertEquals("buildings", layer.id());
        assertTrue(layer.fields().isEmpty());

        // Test serialization/deserialization
        String json = objectMapper.writeValueAsString(layer);
        VectorLayer deserialized = objectMapper.readValue(json, VectorLayer.class);

        assertEquals(layer.id(), deserialized.id());
        assertTrue(deserialized.fields().isEmpty());
    }

    @Test
    void testFieldTypes() throws Exception {
        // Test various field types commonly used in vector tiles
        Map<String, String> fields = Map.of(
                "id", "Number",
                "name", "String",
                "visible", "Boolean",
                "area", "Number",
                "tags", "String",
                "custom_type", "CustomType" // Should allow any string value
                );

        VectorLayer layer = VectorLayer.of("features", fields);

        // Test serialization preserves all field types
        String json = objectMapper.writeValueAsString(layer);
        VectorLayer deserialized = objectMapper.readValue(json, VectorLayer.class);

        assertEquals(fields, deserialized.fields());
        assertEquals("Number", deserialized.fields().get("id"));
        assertEquals("String", deserialized.fields().get("name"));
        assertEquals("Boolean", deserialized.fields().get("visible"));
        assertEquals("CustomType", deserialized.fields().get("custom_type"));
    }
}
