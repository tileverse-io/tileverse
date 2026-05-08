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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** Tests for TileJSON v3.0.0 specification compliance. */
class TileJSONTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testMinimalTileJSON() throws Exception {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        String json = objectMapper.writeValueAsString(tileJSON);
        TileJSON deserialized = objectMapper.readValue(json, TileJSON.class);
        assertThat(deserialized).isEqualTo(tileJSON);
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

        String json = objectMapper.writeValueAsString(tileJSON);
        TileJSON deserialized = objectMapper.readValue(json, TileJSON.class);

        assertThat(deserialized).isEqualTo(tileJSON);
    }

    @Test
    void testRequiredFieldValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        List<String> validTiles = List.of("https://example.com/{z}/{x}/{y}.pbf");
        List<VectorLayer> validLayers = List.of(layer);
        List<String> emptyTiles = List.of();
        List<String> nullTileUrl = java.util.Arrays.asList((String) null);
        List<String> blankTileUrl = List.of("");

        assertThatThrownBy(() -> TileJSON.of(null, validTiles, validLayers))
                .as("null tilejson")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TileJSON.of("", validTiles, validLayers))
                .as("empty tilejson")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TileJSON.of("3.0.0", null, validLayers))
                .as("null tiles")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TileJSON.of("3.0.0", emptyTiles, validLayers))
                .as("empty tiles")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TileJSON.of("3.0.0", nullTileUrl, validLayers))
                .as("null tile URL")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TileJSON.of("3.0.0", blankTileUrl, validLayers))
                .as("blank tile URL")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TileJSON.of("3.0.0", validTiles, null))
                .as("null vector layers")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBoundsValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        assertThatThrownBy(() -> tileJSON.withBounds(10, -85, 10, 85))
                .as("west == east")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withBounds(10, -85, -10, 85))
                .as("west > east")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> tileJSON.withBounds(-180, 85, 180, 85))
                .as("south == north")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withBounds(-180, 85, 180, -85))
                .as("south > north")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> tileJSON.withBounds(-181, -85, 180, 85))
                .as("west < -180")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withBounds(-180, -91, 180, 85))
                .as("south < -90")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withBounds(-180, -85, 181, 85))
                .as("east > 180")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withBounds(-180, -85, 180, 91))
                .as("north > 90")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCenterValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        assertThatThrownBy(() -> tileJSON.withCenter(-181, 0, null))
                .as("longitude < -180")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withCenter(181, 0, null))
                .as("longitude > 180")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> tileJSON.withCenter(0, -91, null))
                .as("latitude < -90")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withCenter(0, 91, null))
                .as("latitude > 90")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> tileJSON.withCenter(0, 0, -1.0))
                .as("zoom < 0")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withCenter(0, 0, 31.0))
                .as("zoom > 30")
                .isInstanceOf(IllegalArgumentException.class);

        // valid centers
        assertDoesNotThrow(() -> tileJSON.withCenter(-180, -90, null));
        assertDoesNotThrow(() -> tileJSON.withCenter(180, 90, null));
        assertDoesNotThrow(() -> tileJSON.withCenter(0, 0, 0.0));
        assertDoesNotThrow(() -> tileJSON.withCenter(0, 0, 30.0));
    }

    @Test
    void testZoomValidation() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "string"));
        TileJSON tileJSON = TileJSON.of("3.0.0", List.of("https://example.com/{z}/{x}/{y}.pbf"), List.of(layer));

        assertThatThrownBy(() -> tileJSON.withZoomRange(-1, 10))
                .as("minZoom < 0")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withZoomRange(0, 31))
                .as("maxZoom > 30")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tileJSON.withZoomRange(10, 5))
                .as("minZoom > maxZoom")
                .isInstanceOf(IllegalArgumentException.class);

        // valid zoom ranges
        assertDoesNotThrow(() -> tileJSON.withZoomRange(0, 0));
        assertDoesNotThrow(() -> tileJSON.withZoomRange(0, 30));
        assertDoesNotThrow(() -> tileJSON.withZoomRange(null, 10));
        assertDoesNotThrow(() -> tileJSON.withZoomRange(5, null));
    }

    @Test
    void testJSONDeserialization() throws Exception {
        String json = """
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

        // all builder methods
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
