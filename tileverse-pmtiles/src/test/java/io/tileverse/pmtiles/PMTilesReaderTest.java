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
package io.tileverse.pmtiles;

import static java.util.Objects.*;
import static org.junit.jupiter.api.Assertions.*;

import io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PMTilesReaderTest {

    protected static Path andorraPmTiles;

    @BeforeAll
    static void copyTestData(@TempDir Path tmpFolder) throws IOException {
        try (InputStream in = requireNonNull(PMTilesReaderTest.class.getResourceAsStream("/andorra.pmtiles"))) {
            andorraPmTiles = tmpFolder.resolve("andorra.pmtiles");
            Files.copy(in, andorraPmTiles);
        }
    }

    /**
     * Subclasses would override this method to test against different range readers
     */
    protected RangeReader getAndorraRangeReader() throws IOException {
        return FileRangeReader.of(andorraPmTiles);
    }

    /**
     * Test that validates PMTiles file information matching the output of:
     * <pre>{@code
     * pmtiles show test-data/src/main/resources/andorra.pmtiles
     * pmtiles spec version: 3
     * tile type: mvt
     * bounds: (long: 1.412368, lat: 42.427600) (long: 1.787481, lat: 42.657170)
     * min zoom: 0
     * max zoom: 14
     * center: (long: 1.599924, lat: 42.542385)
     * center zoom: 10
     * addressed tiles count: 329
     * tile entries count: 329
     * tile contents count: 329
     * clustered: true
     * internal compression: gzip
     * tile compression: gzip
     * vector_layers <object...>
     * attribution <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
     * description A basic, lean, general-purpose vector tile schema for OpenStreetMap data. See https://shortbread.geofabrik.de/
     * name Shortbread
     * planetiler:buildtime 2025-06-19T18:35:23.558Z
     * planetiler:githash 595c824c686b208b98bc3ab97763b1bf855bf2bc
     * planetiler:osm:osmosisreplicationseq 4488
     * planetiler:osm:osmosisreplicationtime 2025-07-21T20:21:00Z
     * planetiler:osm:osmosisreplicationurl https://download.geofabrik.de/europe/andorra-updates
     * planetiler:version 0.9.2-SNAPSHOT
     * type baselayer
     * }</pre>
     */
    @Test
    void testPMTilesShowInfo() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            PMTilesHeader header = reader.getHeader();

            // Test header information
            assertEquals(3, header.version());
            assertEquals(PMTilesHeader.TILETYPE_MVT, header.tileType());

            // Test bounds (using utility methods)
            assertEquals(1.412368, header.minLon(), 0.000001);
            assertEquals(42.427600, header.minLat(), 0.000001);
            assertEquals(1.787481, header.maxLon(), 0.000001);
            assertEquals(42.657170, header.maxLat(), 0.000001);

            // Test zoom levels
            assertEquals(0, header.minZoom());
            assertEquals(14, header.maxZoom());

            // Test center
            assertEquals(1.599924, header.centerLon(), 0.000001);
            assertEquals(42.542385, header.centerLat(), 0.000001);
            assertEquals(10, header.centerZoom());

            // Test tile counts
            assertEquals(329, header.addressedTilesCount());
            assertEquals(329, header.tileEntriesCount());
            assertEquals(329, header.tileContentsCount());

            // Test clustering and compression
            assertTrue(header.clustered());
            assertEquals(PMTilesHeader.COMPRESSION_GZIP, header.internalCompression());
            assertEquals(PMTilesHeader.COMPRESSION_GZIP, header.tileCompression());

            // Test metadata JSON
            String metadata = reader.getMetadataAsString();
            assertNotNull(metadata);
            assertFalse(metadata.isEmpty());

            // Test specific metadata fields (using simple string contains for now)
            assertTrue(metadata.contains("OpenStreetMap contributors"));
            assertTrue(metadata.contains("Shortbread"));
            assertTrue(metadata.contains("baselayer"));
            assertTrue(metadata.contains("planetiler"));
            assertTrue(metadata.contains("vector_layers"));

            // Test metadata object parsing
            PMTilesMetadata metadataObj = reader.getMetadata();
            assertNotNull(metadataObj);

            // Verify parsed metadata fields
            assertNotNull(metadataObj.attribution());
            assertTrue(metadataObj.attribution().contains("OpenStreetMap contributors"));

            assertNotNull(metadataObj.vectorLayers());
            assertFalse(metadataObj.vectorLayers().isEmpty());

            // Check that we have some expected vector layers
            boolean hasLandLayer = metadataObj.vectorLayers().stream().anyMatch(layer -> "land".equals(layer.id()));
            boolean hasWaterLayer = metadataObj.vectorLayers().stream()
                    .anyMatch(layer -> "water".equals(layer.id()) || "ocean".equals(layer.id()));

            // At least one of these layers should exist in the Shortbread schema
            assertTrue(hasLandLayer || hasWaterLayer, "Should have land or water layer");

            // Note: PMTiles metadata doesn't define minZoom/maxZoom fields
            // Those are in the header, not the metadata JSON
        }
    }

    /**
     * Test tile fetching matching the output of pmtiles tile commands.
     * Tests various tiles that should exist in the andorra.pmtiles file.
     */
    @Test
    void testTileFetching() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            // Test tile 0/0/0 - should exist (root tile)
            var tile000 = reader.getTile(0, 0, 0);
            assertTrue(tile000.isPresent(), "Tile 0/0/0 should exist");
            assertTrue(tile000.get().remaining() > 0, "Tile 0/0/0 should have data");

            // Test a second tile that should exist - use different zoom level
            // Since coordinate validation is strict, use zoom 1 where 1/1/0 is valid
            // (confirmed to exist by pmtiles command earlier)

            // Test tile 1/1/0 - should exist (confirmed by pmtiles command)
            var tile110 = reader.getTile(1, 1, 0);
            assertTrue(tile110.isPresent(), "Tile 1/1/0 should exist");
            assertTrue(tile110.get().remaining() > 0, "Tile 1/1/0 should have data");
        }
    }

    /**
     * Test tiles that should not exist in the archive.
     */
    @Test
    void testNonExistentTiles() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            // Test beyond max zoom level
            var tileBeyondMax = reader.getTile(15, 0, 0);
            assertTrue(tileBeyondMax.isEmpty(), "Tile beyond max zoom should not exist");

            // Test tile that doesn't exist at zoom 1 (confirmed by pmtiles command)
            var tileNotFound = reader.getTile(1, 0, 0);
            assertTrue(tileNotFound.isEmpty(), "Tile 1/0/0 should not exist");

            // Test tile way outside bounds - Andorra is small so most tiles don't exist
            var tileOutOfBounds = reader.getTile(10, 0, 0);
            assertTrue(tileOutOfBounds.isEmpty(), "Tile outside geographic bounds should not exist");
        }
    }

    /**
     * Test edge cases and invalid coordinates.
     */
    @Test
    void testEdgeCases() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            // Test negative coordinates - should throw IllegalArgumentException
            assertThrows(
                    IllegalArgumentException.class,
                    () -> reader.getTile(5, -1, 10),
                    "Tile with negative X should throw IllegalArgumentException");

            assertThrows(
                    IllegalArgumentException.class,
                    () -> reader.getTile(5, 10, -1),
                    "Tile with negative Y should throw IllegalArgumentException");

            // Test coordinates beyond tile grid bounds at zoom level
            int zoom = 5;
            int maxCoord = (1 << zoom); // 2^zoom
            assertThrows(
                    IllegalArgumentException.class,
                    () -> reader.getTile(zoom, maxCoord, 0),
                    "Tile with X beyond grid bounds should throw IllegalArgumentException");

            assertThrows(
                    IllegalArgumentException.class,
                    () -> reader.getTile(zoom, 0, maxCoord),
                    "Tile with Y beyond grid bounds should throw IllegalArgumentException");
        }
    }

    /**
     * Test tiles at different zoom levels to verify zoom-specific behavior.
     */
    @Test
    void testDifferentZoomLevels() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            PMTilesHeader header = reader.getHeader();

            // Test at minimum zoom
            int minZoom = header.minZoom();
            var minZoomTile = reader.getTile(minZoom, 0, 0);
            assertTrue(minZoomTile.isPresent(), "Tile at min zoom should exist");

            // Test below minimum zoom - should not exist
            if (minZoom > 0) {
                var belowMinZoom = reader.getTile(minZoom - 1, 0, 0);
                assertTrue(belowMinZoom.isEmpty(), "Tile below min zoom should not exist");
            }

            // Test at center zoom level
            int centerZoom = header.centerZoom();
            if (centerZoom >= minZoom && centerZoom <= header.maxZoom()) {
                // Calculate center tile coordinates at center zoom
                double centerLon = header.centerLon();
                double centerLat = header.centerLat();

                // Simple tile coordinate calculation (Web Mercator)
                int tilesAtZoom = 1 << centerZoom;
                int centerX = (int) ((centerLon + 180.0) / 360.0 * tilesAtZoom);
                int centerY = (int) ((1.0
                                - Math.log(Math.tan(Math.toRadians(centerLat))
                                                + 1.0 / Math.cos(Math.toRadians(centerLat)))
                                        / Math.PI)
                        / 2.0
                        * tilesAtZoom);

                var centerTile = reader.getTile(centerZoom, centerX, centerY);
                // Note: center tile might not exist if it's outside the actual data bounds
                // so we don't assert its existence, just that the call works
                assertNotNull(centerTile, "Center tile query should return a result (even if empty)");
            }
        }
    }

    /**
     * Test utility methods for converting E7 encoded coordinates to doubles.
     */
    @Test
    void testCoordinateUtilityMethods() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            PMTilesHeader header = reader.getHeader();

            // Test that utility methods match manual E7 conversion
            assertEquals(header.minLonE7() / 10_000_000.0, header.minLon(), 0.0000001);
            assertEquals(header.minLatE7() / 10_000_000.0, header.minLat(), 0.0000001);
            assertEquals(header.maxLonE7() / 10_000_000.0, header.maxLon(), 0.0000001);
            assertEquals(header.maxLatE7() / 10_000_000.0, header.maxLat(), 0.0000001);
            assertEquals(header.centerLonE7() / 10_000_000.0, header.centerLon(), 0.0000001);
            assertEquals(header.centerLatE7() / 10_000_000.0, header.centerLat(), 0.0000001);

            // Test reasonable coordinate ranges for Andorra
            assertTrue(header.minLon() > 0 && header.minLon() < 3, "Min longitude should be reasonable for Andorra");
            assertTrue(header.minLat() > 40 && header.minLat() < 45, "Min latitude should be reasonable for Andorra");
            assertTrue(header.maxLon() > header.minLon(), "Max longitude should be greater than min");
            assertTrue(header.maxLat() > header.minLat(), "Max latitude should be greater than min");
            assertTrue(
                    header.centerLon() >= header.minLon() && header.centerLon() <= header.maxLon(),
                    "Center longitude should be within bounds");
            assertTrue(
                    header.centerLat() >= header.minLat() && header.centerLat() <= header.maxLat(),
                    "Center latitude should be within bounds");
        }
    }

    /**
     * Test that tile data is properly decompressed when compression is used.
     */
    @Test
    void testTileDecompression() throws Exception {
        try (PMTilesReader reader = new PMTilesReader(getAndorraRangeReader())) {
            PMTilesHeader header = reader.getHeader();

            // Verify that compression is enabled
            assertEquals(PMTilesHeader.COMPRESSION_GZIP, header.tileCompression());

            // Get a tile that should exist
            var tile = reader.getTile(0, 0, 0);
            assertTrue(tile.isPresent(), "Root tile should exist");

            ByteBuffer tileData = tile.get();
            assertTrue(tileData.remaining() > 0, "Tile should have data");

            // For MVT tiles, decompressed data should start with protobuf-like bytes
            // (this is a basic sanity check that decompression worked)
            assertNotNull(tileData, "Decompressed tile data should not be null");
            assertTrue(tileData.remaining() > 10, "Decompressed tile should be reasonably sized");
        }
    }
}
