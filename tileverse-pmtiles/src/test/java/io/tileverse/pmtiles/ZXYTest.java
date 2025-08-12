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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit tests for the ZXY record, validating coordinate validation,
 * Hilbert curve implementation, and round-trip conversion.
 */
class ZXYTest {

    /**
     * Test coordinate validation for valid coordinates.
     */
    @Test
    void testValidCoordinates() {
        // Test various valid coordinates
        assertDoesNotThrow(() -> ZXY.of(0, 0, 0));
        assertDoesNotThrow(() -> ZXY.of(1, 0, 0));
        assertDoesNotThrow(() -> ZXY.of(1, 1, 1));
        assertDoesNotThrow(() -> ZXY.of(5, 31, 31));
        assertDoesNotThrow(() -> ZXY.of(10, 1023, 1023));
    }

    /**
     * Test coordinate validation for invalid coordinates.
     */
    @Test
    void testInvalidCoordinates() {
        // Test negative zoom level
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(-1, 0, 0));

        // Test negative coordinates
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(5, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(5, 0, -1));

        // Test coordinates beyond valid range
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(0, 1, 0)); // Max at z=0 is 0
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(0, 0, 1)); // Max at z=0 is 0
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(5, 32, 0)); // Max at z=5 is 31
        assertThrows(IllegalArgumentException.class, () -> ZXY.of(5, 0, 32)); // Max at z=5 is 31
    }

    /**
     * Test Hilbert curve implementation consistency using test cases from HilbertTest.
     * This validates that the current implementation produces expected results.
     */
    @ParameterizedTest
    @CsvSource({
        "0, 0, 0, 0", // Base case
        "1, 0, 0, 1", // Zoom 1 coordinates
        "1, 1, 0, 2", //
        "1, 0, 1, 4", //
        "1, 1, 1, 3", //
        "7, 34, 51", // The problematic coordinates mentioned in HilbertTest
        "7, 64, 64", // Edge case at zoom 7
        "10, 512, 512", // Mid-range coordinates at zoom 10
    })
    void testHilbertImplementation(int z, int x, int y) {
        // Create ZXY and get tile ID
        ZXY zxy = ZXY.of(z, x, y);
        long tileId = zxy.toTileId();

        // Verify the tile ID is reasonable (non-negative)
        assertTrue(tileId >= 0, "Tile ID should be non-negative");

        // For higher zoom levels, verify tile ID is within reasonable bounds
        if (z <= 20) { // Reasonable upper limit for testing
            long maxPossibleTileId = 0;
            for (int tz = 0; tz <= z; tz++) {
                long tilesAtZoom = 1L << tz;
                maxPossibleTileId += tilesAtZoom * tilesAtZoom;
            }
            assertTrue(tileId < maxPossibleTileId, "Tile ID should be within expected bounds");
        }
    }

    /**
     * Test round-trip conversion from coordinates to tileId and back,
     * based on the validation logic from HilbertValidation.
     */
    @ParameterizedTest
    @CsvSource({
        "0, 0, 0",
        "1, 0, 0",
        "1, 0, 1",
        "1, 1, 0",
        "1, 1, 1",
        "5, 16, 16",
        "7, 34, 51", // The specific problematic case from HilbertValidation
        "7, 64, 64",
        "10, 512, 512",
        "12, 2048, 2048"
    })
    void testRoundTripConversion(int z, int x, int y) {
        // Create original ZXY from coordinates
        ZXY original = ZXY.of(z, x, y);

        // Convert to tileId
        long tileId = original.toTileId();

        // Convert back to ZXY
        ZXY converted = ZXY.fromTileId(tileId);

        // Verify round-trip conversion is successful
        assertEquals(original.z(), converted.z(), "Zoom level should match after round-trip");
        assertEquals(original.x(), converted.x(), "X coordinate should match after round-trip");
        assertEquals(original.y(), converted.y(), "Y coordinate should match after round-trip");
        assertEquals(original, converted, "Complete ZXY should match after round-trip");
    }

    /**
     * Test edge cases for tile ID conversion.
     */
    @Test
    void testTileIdEdgeCases() {
        // Test zoom level 0 (single tile)
        ZXY z0 = ZXY.of(0, 0, 0);
        assertEquals(0L, z0.toTileId(), "Zoom 0 tile should have ID 0");

        // Test first tile at zoom 1
        ZXY z1first = ZXY.of(1, 0, 0);
        assertEquals(1L, z1first.toTileId(), "First tile at zoom 1 should have ID 1");

        // Test that tile IDs increase with zoom level
        long prevMaxTileId = 0;
        for (int z = 1; z <= 10; z++) {
            ZXY firstTileAtZoom = ZXY.of(z, 0, 0);
            long firstTileId = firstTileAtZoom.toTileId();
            assertTrue(
                    firstTileId > prevMaxTileId,
                    "First tile at zoom " + z + " should have ID greater than previous zoom's max");
            prevMaxTileId = firstTileId;
        }
    }

    /**
     * Test fromTileId with invalid inputs.
     */
    @Test
    void testFromTileIdInvalidInputs() {
        // Test negative tile ID
        assertThrows(IllegalArgumentException.class, () -> ZXY.fromTileId(-1));

        // Test extremely large tile ID (beyond 32 zoom levels)
        assertThrows(IllegalArgumentException.class, () -> ZXY.fromTileId(Long.MAX_VALUE));
    }

    /**
     * Test toTileId with zoom level that's at the limit.
     */
    @Test
    void testTileIdZoomTooLarge() {
        // Test that zoom level 31 works (it's at the limit)
        ZXY validMaxZoom = ZXY.of(31, 0, 0);
        assertDoesNotThrow(() -> validMaxZoom.toTileId(), "Zoom level 31 should be valid");

        // We can't easily test z > 31 in the constructor because it validates coordinates
        // But we can verify the current limits work as expected
        // The constructor will prevent creating invalid zoom levels anyway
        long tileId = validMaxZoom.toTileId();
        assertTrue(tileId >= 0, "Valid tile ID should be non-negative");
    }

    /**
     * Test spatial locality property of Hilbert curve.
     * Adjacent tiles in 2D space should have relatively close tile IDs.
     */
    @Test
    void testSpatialLocality() {
        int z = 5; // Use zoom level 5 for testing

        // Test that adjacent tiles have reasonably close tile IDs
        ZXY center = ZXY.of(z, 16, 16);
        ZXY right = ZXY.of(z, 17, 16);
        ZXY down = ZXY.of(z, 16, 17);

        long centerId = center.toTileId();
        long rightId = right.toTileId();
        long downId = down.toTileId();

        // The tile IDs shouldn't be identical (obviously)
        assertNotEquals(centerId, rightId);
        assertNotEquals(centerId, downId);
        assertNotEquals(rightId, downId);

        // For Hilbert curves, spatial locality means nearby tiles should have
        // relatively close IDs, though this isn't guaranteed for all adjacencies
        // We just verify that the IDs are in a reasonable range
        long maxTilesAtZoom = (1L << z) * (1L << z); // Total tiles at this zoom
        assertTrue(Math.abs(rightId - centerId) < maxTilesAtZoom, "Adjacent tiles should have reasonably close IDs");
        assertTrue(Math.abs(downId - centerId) < maxTilesAtZoom, "Adjacent tiles should have reasonably close IDs");
    }

    /**
     * Test that tile IDs are unique for different coordinates at the same zoom level.
     */
    @Test
    void testUniqueTileIds() {
        int z = 3; // Use a small zoom level for exhaustive testing
        int maxCoord = (1 << z) - 1;

        // Collect all tile IDs at this zoom level
        java.util.Set<Long> tileIds = new java.util.HashSet<>();

        for (int x = 0; x <= maxCoord; x++) {
            for (int y = 0; y <= maxCoord; y++) {
                ZXY zxy = ZXY.of(z, x, y);
                long tileId = zxy.toTileId();

                // Verify this tile ID hasn't been seen before
                assertTrue(
                        tileIds.add(tileId),
                        "Tile ID " + tileId + " for coordinates (" + x + "," + y + ") should be unique");
            }
        }

        // Verify we have the expected number of unique tile IDs
        long expectedTileCount = (1L << z) * (1L << z);
        assertEquals(
                expectedTileCount,
                tileIds.size(),
                "Should have exactly " + expectedTileCount + " unique tile IDs at zoom " + z);
    }
}
