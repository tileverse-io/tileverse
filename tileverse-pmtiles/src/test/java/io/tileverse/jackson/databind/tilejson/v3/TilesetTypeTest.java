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
import org.junit.jupiter.api.Test;

/**
 * Tests for TilesetType enum following TileJSON v3.0.0 specification.
 */
class TilesetTypeTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testEnumValues() {
        assertEquals("baselayer", TilesetType.BASELAYER.getValue());
        assertEquals("overlay", TilesetType.OVERLAY.getValue());

        assertEquals("baselayer", TilesetType.BASELAYER.toString());
        assertEquals("overlay", TilesetType.OVERLAY.toString());
    }

    @Test
    void testFromValueCaseSensitivity() {
        // Test exact matches
        assertEquals(TilesetType.BASELAYER, TilesetType.fromValue("baselayer"));
        assertEquals(TilesetType.OVERLAY, TilesetType.fromValue("overlay"));

        // Test case insensitivity
        assertEquals(TilesetType.BASELAYER, TilesetType.fromValue("BASELAYER"));
        assertEquals(TilesetType.BASELAYER, TilesetType.fromValue("BaseLayer"));
        assertEquals(TilesetType.OVERLAY, TilesetType.fromValue("OVERLAY"));
        assertEquals(TilesetType.OVERLAY, TilesetType.fromValue("Overlay"));

        // Test with whitespace
        assertEquals(TilesetType.BASELAYER, TilesetType.fromValue("  baselayer  "));
        assertEquals(TilesetType.OVERLAY, TilesetType.fromValue("  overlay  "));
    }

    @Test
    void testFromValueUnknownValues() {
        // Test unknown values return null (forward compatibility)
        assertNull(TilesetType.fromValue("unknown"));
        assertNull(TilesetType.fromValue("custom"));
        assertNull(TilesetType.fromValue("baselayers")); // plural
        assertNull(TilesetType.fromValue("overlays")); // plural
        assertNull(TilesetType.fromValue(""));
        assertNull(TilesetType.fromValue("   "));
        assertNull(TilesetType.fromValue(null));
    }

    @Test
    void testJacksonSerialization() throws Exception {
        // Test serialization of enum values
        String baselayerJson = objectMapper.writeValueAsString(TilesetType.BASELAYER);
        assertEquals("\"baselayer\"", baselayerJson);

        String overlayJson = objectMapper.writeValueAsString(TilesetType.OVERLAY);
        assertEquals("\"overlay\"", overlayJson);
    }

    @Test
    void testJacksonDeserialization() throws Exception {
        // Test deserialization of known values
        TilesetType baselayer = objectMapper.readValue("\"baselayer\"", TilesetType.class);
        assertEquals(TilesetType.BASELAYER, baselayer);

        TilesetType overlay = objectMapper.readValue("\"overlay\"", TilesetType.class);
        assertEquals(TilesetType.OVERLAY, overlay);

        // Test case insensitivity in JSON
        TilesetType baselayerUpper = objectMapper.readValue("\"BASELAYER\"", TilesetType.class);
        assertEquals(TilesetType.BASELAYER, baselayerUpper);
    }

    @Test
    void testJacksonDeserializationUnknownValues() throws Exception {
        // Test unknown values deserialize to null (forward compatibility)
        TilesetType unknown = objectMapper.readValue("\"unknown_type\"", TilesetType.class);
        assertNull(unknown);

        TilesetType empty = objectMapper.readValue("\"\"", TilesetType.class);
        assertNull(empty);
    }

    @Test
    void testJacksonInObjectContext() throws Exception {
        // Test TilesetType within a JSON object context
        String json =
                """
            {
                "name": "Test Map",
                "type": "baselayer"
            }
            """;

        // Simple record to test context
        record TestMetadata(String name, TilesetType type) {}

        TestMetadata metadata = objectMapper.readValue(json, TestMetadata.class);
        assertEquals("Test Map", metadata.name());
        assertEquals(TilesetType.BASELAYER, metadata.type());

        // Test serialization back
        String serialized = objectMapper.writeValueAsString(metadata);
        assertTrue(serialized.contains("\"type\":\"baselayer\""));
    }

    @Test
    void testJacksonInObjectContextUnknownValue() throws Exception {
        // Test unknown value in object context (should be null)
        String json =
                """
            {
                "name": "Test Map",
                "type": "unknown_type"
            }
            """;

        record TestMetadata(String name, TilesetType type) {}

        TestMetadata metadata = objectMapper.readValue(json, TestMetadata.class);
        assertEquals("Test Map", metadata.name());
        assertNull(metadata.type());
    }

    @Test
    void testAllEnumValues() {
        // Ensure we have exactly the expected enum values
        TilesetType[] values = TilesetType.values();
        assertEquals(2, values.length);

        boolean hasBaselayer = false;
        boolean hasOverlay = false;

        for (TilesetType type : values) {
            switch (type) {
                case BASELAYER -> hasBaselayer = true;
                case OVERLAY -> hasOverlay = true;
            }
        }

        assertTrue(hasBaselayer, "Should have BASELAYER enum value");
        assertTrue(hasOverlay, "Should have OVERLAY enum value");
    }

    @Test
    void testRoundTripSerialization() throws Exception {
        // Test that we can serialize and deserialize without data loss
        for (TilesetType type : TilesetType.values()) {
            String json = objectMapper.writeValueAsString(type);
            TilesetType deserialized = objectMapper.readValue(json, TilesetType.class);
            assertEquals(type, deserialized, "Round-trip serialization failed for " + type);
        }
    }
}
