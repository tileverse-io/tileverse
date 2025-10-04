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
package io.tileverse.jackson.databind.pmtiles.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tileverse.jackson.databind.tilejson.v3.TilesetType;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import io.tileverse.pmtiles.PMTilesReader;
import io.tileverse.pmtiles.PMTilesTestData;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for PMTiles metadata object model following PMTiles v3 specification.
 */
class PMTilesMetadataTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @TempDir
    static Path tmpFolder;

    @Test
    void testSimpleMetadataDeserialization() throws Exception {
        String json =
                """
            {
                "name": "Test Tileset",
                "description": "A test tileset for unit testing",
                "version": "1.0.0",
                "attribution": "© Test Contributors",
                "type": "baselayer"
            }
            """;

        PMTilesMetadata metadata = objectMapper.readValue(json, PMTilesMetadata.class);

        assertEquals("Test Tileset", metadata.name());
        assertEquals("A test tileset for unit testing", metadata.description());
        assertEquals("1.0.0", metadata.version());
        assertEquals("© Test Contributors", metadata.attribution());
        assertEquals(TilesetType.BASELAYER, metadata.type());
        assertNull(metadata.vectorLayers());
        assertNull(metadata.extras());
    }

    @Test
    void testVectorTilesetMetadata() throws Exception {
        String json =
                """
            {
                "name": "Vector Tileset",
                "description": "A vector tileset with layers",
                "version": "2.0.0",
                "attribution": "© OpenStreetMap contributors",
                "type": "overlay",
                "vector_layers": [
                    {
                        "id": "buildings",
                        "description": "Building footprints",
                        "minzoom": 10,
                        "maxzoom": 18,
                        "fields": {
                            "height": "Number",
                            "name": "String",
                            "type": "String"
                        }
                    },
                    {
                        "id": "roads",
                        "description": "Road network",
                        "minzoom": 0,
                        "maxzoom": 18,
                        "fields": {
                            "name": "String",
                            "highway": "String",
                            "oneway": "Boolean"
                        }
                    }
                ]
            }
            """;

        PMTilesMetadata metadata = objectMapper.readValue(json, PMTilesMetadata.class);

        assertEquals("Vector Tileset", metadata.name());
        assertEquals("A vector tileset with layers", metadata.description());
        assertEquals("2.0.0", metadata.version());
        assertEquals("© OpenStreetMap contributors", metadata.attribution());
        assertEquals(TilesetType.OVERLAY, metadata.type());

        // Test vector layers (required for MVT tilesets)
        assertNotNull(metadata.vectorLayers());
        assertEquals(2, metadata.vectorLayers().size());

        VectorLayer buildings = metadata.vectorLayers().get(0);
        assertEquals("buildings", buildings.id());
        assertEquals("Building footprints", buildings.description());
        assertEquals(Integer.valueOf(10), buildings.minZoom());
        assertEquals(Integer.valueOf(18), buildings.maxZoom());
        assertNotNull(buildings.fields());
        assertEquals("Number", buildings.fields().get("height"));
        assertEquals("String", buildings.fields().get("name"));
        assertEquals("String", buildings.fields().get("type"));

        VectorLayer roads = metadata.vectorLayers().get(1);
        assertEquals("roads", roads.id());
        assertEquals("Road network", roads.description());
        assertEquals(Integer.valueOf(0), roads.minZoom());
        assertEquals(Integer.valueOf(18), roads.maxZoom());
        assertNotNull(roads.fields());
        assertEquals("String", roads.fields().get("name"));
        assertEquals("String", roads.fields().get("highway"));
        assertEquals("Boolean", roads.fields().get("oneway"));
    }

    @Test
    void testMetadataWithUnknownProperties() throws Exception {
        String json =
                """
            {
                "name": "Test Tileset",
                "description": "Test description",
                "type": "baselayer",
                "unknown_property": "should be ignored",
                "custom_field": {
                    "nested": "value"
                },
                "bounds": [-180, -85, 180, 85],
                "center": [0, 0, 5],
                "minzoom": 0,
                "maxzoom": 14
            }
            """;

        // Should not throw an exception due to unknown properties
        PMTilesMetadata metadata = objectMapper.readValue(json, PMTilesMetadata.class);

        assertEquals("Test Tileset", metadata.name());
        assertEquals("Test description", metadata.description());
        assertEquals(TilesetType.BASELAYER, metadata.type());
        // Unknown properties are ignored, but doesn't cause deserialization to fail
    }

    @Test
    void testMetadataSerialization() throws Exception {
        PMTilesMetadata original = PMTilesMetadata.of("Test Tileset", "Test description", "© Test")
                .withVersion("1.0.0")
                .withType(TilesetType.BASELAYER)
                .withVectorLayers(List.of(VectorLayer.of("test", Map.of("name", "String"), "Test layer")
                        .withZoomRange(0, 14)));

        // Serialize to JSON
        String json = objectMapper.writeValueAsString(original);
        assertNotNull(json);
        assertTrue(json.contains("Test Tileset"));
        assertTrue(json.contains("baselayer"));

        // Deserialize back
        PMTilesMetadata deserialized = objectMapper.readValue(json, PMTilesMetadata.class);

        assertEquals(original.name(), deserialized.name());
        assertEquals(original.description(), deserialized.description());
        assertEquals(original.version(), deserialized.version());
        assertEquals(original.attribution(), deserialized.attribution());
        assertEquals(original.type(), deserialized.type());

        // Check vector layer
        assertNotNull(deserialized.vectorLayers());
        assertEquals(1, deserialized.vectorLayers().size());
        VectorLayer layer = deserialized.vectorLayers().get(0);
        assertEquals("test", layer.id());
        assertEquals("Test layer", layer.description());
    }

    @Test
    void testTilesetTypeEnum() throws Exception {
        // Test baselayer type
        String baselayerJson =
                """
            {
                "name": "Base Map",
                "type": "baselayer"
            }
            """;

        PMTilesMetadata baselayerMetadata = objectMapper.readValue(baselayerJson, PMTilesMetadata.class);
        assertEquals(TilesetType.BASELAYER, baselayerMetadata.type());

        // Test overlay type
        String overlayJson =
                """
            {
                "name": "Overlay Map",
                "type": "overlay"
            }
            """;

        PMTilesMetadata overlayMetadata = objectMapper.readValue(overlayJson, PMTilesMetadata.class);
        assertEquals(TilesetType.OVERLAY, overlayMetadata.type());

        // Test unknown type (should be null for forward compatibility)
        String unknownJson =
                """
            {
                "name": "Unknown Map",
                "type": "unknown_type"
            }
            """;

        PMTilesMetadata unknownMetadata = objectMapper.readValue(unknownJson, PMTilesMetadata.class);
        assertNull(unknownMetadata.type());

        // Test serialization
        PMTilesMetadata original = PMTilesMetadata.of("Test").withType(TilesetType.BASELAYER);
        String json = objectMapper.writeValueAsString(original);
        assertTrue(json.contains("\"type\":\"baselayer\""));

        PMTilesMetadata deserialized = objectMapper.readValue(json, PMTilesMetadata.class);
        assertEquals(TilesetType.BASELAYER, deserialized.type());
    }

    @Test
    void testBuilderMethods() {
        PMTilesMetadata metadata = PMTilesMetadata.of("Original Name")
                .withDescription("Test description")
                .withVersion("1.0")
                .withAttribution("© Test")
                .withType(TilesetType.OVERLAY);

        assertEquals("Original Name", metadata.name());
        assertEquals("Test description", metadata.description());
        assertEquals("1.0", metadata.version());
        assertEquals("© Test", metadata.attribution());
        assertEquals(TilesetType.OVERLAY, metadata.type());
    }

    @Test
    void testVectorLayerBuilderMethods() {
        VectorLayer layer = VectorLayer.of("test", Map.of("name", "String", "count", "Number"))
                .withDescription("Test layer")
                .withZoomRange(0, 10);

        assertEquals("test", layer.id());
        assertEquals("Test layer", layer.description());
        assertEquals(Integer.valueOf(0), layer.minZoom());
        assertEquals(Integer.valueOf(10), layer.maxZoom());
        assertNotNull(layer.fields());
        assertEquals("String", layer.fields().get("name"));
        assertEquals("Number", layer.fields().get("count"));
    }

    @Test
    void testEmptyMetadata() {
        PMTilesMetadata empty = PMTilesMetadata.of(null);
        assertNull(empty.name());
        assertNull(empty.description());
        assertNull(empty.version());
        assertNull(empty.attribution());
        assertNull(empty.vectorLayers());
        assertNull(empty.type());
        assertNull(empty.extras());
    }

    @Test
    void testAndorraMetadata() throws Exception {
        // Use the same test file as PMTilesReaderTest
        PMTilesReader pmtilesReader = new PMTilesReader(PMTilesTestData.andorra(tmpFolder));
        PMTilesMetadata metadata = pmtilesReader.getMetadata();
        assertNotNull(metadata);

        // Verify basic metadata fields that should be present per PMTiles spec
        assertEquals("Shortbread", metadata.name());
        assertEquals(
                "A basic, lean, general-purpose vector tile schema for OpenStreetMap data. See https://shortbread.geofabrik.de/",
                metadata.description());
        assertEquals(
                "<a href=\"https://www.openstreetmap.org/copyright\" target=\"_blank\">&copy; OpenStreetMap contributors</a>",
                metadata.attribution());

        // Verify type
        assertEquals(TilesetType.BASELAYER, metadata.type());

        // Verify vector layers (required for MVT tilesets)
        assertNotNull(metadata.vectorLayers());
        assertEquals(21, metadata.vectorLayers().size());

        // Check specific vector layers from the pmtiles show output
        Map<String, VectorLayer> layerMap =
                metadata.vectorLayers().stream().collect(Collectors.toMap(VectorLayer::id, Function.identity()));

        // Verify some key layers exist
        assertTrue(layerMap.containsKey("addresses"), "Should have addresses layer");
        assertTrue(layerMap.containsKey("boundaries"), "Should have boundaries layer");
        assertTrue(layerMap.containsKey("buildings"), "Should have buildings layer");
        assertTrue(layerMap.containsKey("streets"), "Should have streets layer");

        // Verify a specific layer's structure
        VectorLayer addresses = layerMap.get("addresses");
        assertNotNull(addresses, "addresses layer should exist");
        assertEquals(Integer.valueOf(14), addresses.minZoom());
        assertEquals(Integer.valueOf(14), addresses.maxZoom());
        assertNotNull(addresses.fields());
        assertEquals("String", addresses.fields().get("housename"));
        assertEquals("String", addresses.fields().get("housenumber"));
    }
}
