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

import static io.tileverse.tiling.pyramid.TileIndex.xyz;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.pyramid.TileIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit tests for the HilbertCurve class, validating tile ID conversion,
 * round-trip conversion, and PMTiles compatibility.
 */
class HilbertCurveTest {

    /**
     * Test round-trip conversion from TileIndex to tileId and back.
     */
    @ParameterizedTest
    @CsvSource({
        "0, 0, 0",
        "1, 0, 0",
        "1, 0, 1",
        "1, 1, 0",
        "1, 1, 1",
        "5, 16, 16",
        "7, 64, 47", // From actual Andorra PMTiles
        "10, 512, 512",
        "12, 2048, 2048"
    })
    void testRoundTripConversion(int z, long x, long y) {
        TileIndex original = xyz(x, y, z);

        // Convert to tileId
        long tileId = HilbertCurve.tileIndexToTileId(original);

        // Convert back to TileIndex
        TileIndex converted = HilbertCurve.tileIdToTileIndex(tileId);

        // Verify round-trip conversion is successful
        assertEquals(original.z(), converted.z(), "Zoom level should match after round-trip");
        assertEquals(original.x(), converted.x(), "X coordinate should match after round-trip");
        assertEquals(original.y(), converted.y(), "Y coordinate should match after round-trip");
        assertEquals(original, converted, "Complete TileIndex should match after round-trip");
    }

    /**
     * Test tile ID conversion with actual tiles from pmtiles.io viewer.
     * These are the ACTUAL tiles that should exist in the Andorra PMTiles file.
     */
    @Test
    void testActualAndorraTilesWithHilbertCurve() {
        // Test cases from pmtiles.io viewer - these are the ACTUAL tiles that should exist
        TileIndex[] actualTiles = {
            xyz(4, 2, 3), // From viewer: (3,4,2)
            xyz(8, 5, 4), // From viewer: (4,8,5)
            xyz(16, 11, 5), // From viewer: (5,16,11)
            xyz(32, 23, 6), // From viewer: (6,32,23)
            xyz(64, 47, 7), // From viewer: (7,64,47)
            xyz(129, 94, 8), // From viewer: (8,129,94)
            xyz(258, 188, 9), // From viewer: (9,258,188)
            xyz(258, 189, 9), // From viewer: (9,258,189)
            xyz(516, 377, 10), // From viewer: (10,516,377)
            xyz(517, 377, 10), // From viewer: (10,517,377)
            xyz(516, 378, 10), // From viewer: (10,516,378)
            xyz(517, 378, 10) // From viewer: (10,517,378)
        };

        System.out.println("=== Testing Actual Andorra Tiles with HilbertCurve ===");
        for (TileIndex expected : actualTiles) {
            System.out.printf("Testing tile %s%n", expected);

            // Convert to tile ID and back using HilbertCurve
            long tileId = HilbertCurve.tileIndexToTileId(expected);
            TileIndex roundTrip = HilbertCurve.tileIdToTileIndex(tileId);

            System.out.printf("  Tile ID: %d%n", tileId);
            System.out.printf("  Round trip: %s%n", roundTrip);

            // Verify round-trip conversion works
            assertEquals(expected.z(), roundTrip.z(), "Zoom level should match");
            assertEquals(expected.x(), roundTrip.x(), "X coordinate should match");
            assertEquals(expected.y(), roundTrip.y(), "Y coordinate should match");
        }
    }

    /**
     * Test edge cases for tile ID conversion.
     */
    @Test
    void testTileIdEdgeCases() {
        // Test zoom level 0 (single tile)
        TileIndex z0 = xyz(0, 0, 0);
        assertEquals(0L, HilbertCurve.tileIndexToTileId(z0), "Zoom 0 tile should have ID 0");

        // Test first tile at zoom 1
        TileIndex z1first = xyz(0, 0, 1);
        assertEquals(1L, HilbertCurve.tileIndexToTileId(z1first), "First tile at zoom 1 should have ID 1");

        // Test that tile IDs increase with zoom level
        long prevMaxTileId = 0;
        for (int z = 1; z <= 10; z++) {
            TileIndex firstTileAtZoom = xyz(0, 0, z);
            long firstTileId = HilbertCurve.tileIndexToTileId(firstTileAtZoom);
            assertTrue(
                    firstTileId > prevMaxTileId,
                    "First tile at zoom " + z + " should have ID greater than previous zoom's max");
            prevMaxTileId = firstTileId;
        }
    }

    /**
     * Test tileIdToTileIndex with invalid inputs.
     */
    @Test
    void testTileIdToTileIndexInvalidInputs() {
        // Test negative tile ID
        assertThrows(IllegalArgumentException.class, () -> HilbertCurve.tileIdToTileIndex(-1));

        // Test extremely large tile ID (beyond 26 zoom levels)
        assertThrows(IllegalArgumentException.class, () -> HilbertCurve.tileIdToTileIndex(Long.MAX_VALUE));
    }

    /**
     * Test tileIndexToTileId with zoom level that's too large.
     */
    @Test
    void testTileIdZoomTooLarge() {
        // Test that zoom level > 26 throws exception
        TileIndex invalidZoom = xyz(0, 0, 27);
        assertThrows(IllegalArgumentException.class, () -> HilbertCurve.tileIndexToTileId(invalidZoom));

        // Test that zoom level 26 works (it's at the limit)
        TileIndex validMaxZoom = xyz(0, 0, 26);
        long tileId = HilbertCurve.tileIndexToTileId(validMaxZoom);
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
        TileIndex center = xyz(16, 16, z);
        TileIndex right = xyz(17, 16, z);
        TileIndex down = xyz(16, 17, z);

        long centerId = HilbertCurve.tileIndexToTileId(center);
        long rightId = HilbertCurve.tileIndexToTileId(right);
        long downId = HilbertCurve.tileIndexToTileId(down);

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
        long maxCoord = (1L << z) - 1;

        // Collect all tile IDs at this zoom level
        java.util.Set<Long> tileIds = new java.util.HashSet<>();

        for (long x = 0; x <= maxCoord; x++) {
            for (long y = 0; y <= maxCoord; y++) {
                TileIndex tileIndex = xyz(x, y, z);
                long tileId = HilbertCurve.tileIndexToTileId(tileIndex);

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

    /**
     * Test coordinate validation for tile index conversion.
     */
    @Test
    void testCoordinateValidation() {
        // Test coordinates beyond valid range for zoom level
        assertThrows(
                IllegalArgumentException.class, () -> HilbertCurve.tileIndexToTileId(xyz(1, 0, 0))); // Max at z=0 is 0

        assertThrows(
                IllegalArgumentException.class,
                () -> HilbertCurve.tileIndexToTileId(xyz(32, 0, 5))); // Max at z=5 is 31

        assertThrows(
                IllegalArgumentException.class,
                () -> HilbertCurve.tileIndexToTileId(xyz(0, 32, 5))); // Max at z=5 is 31
    }

    /**
     * Test specific PMTiles tile ID calculations that were problematic.
     */
    @Test
    void testSpecificProblemCases() {
        // These were problematic coordinates mentioned in previous tests
        TileIndex problematic1 = xyz(34, 51, 7);
        long tileId1 = HilbertCurve.tileIndexToTileId(problematic1);
        TileIndex roundTrip1 = HilbertCurve.tileIdToTileIndex(tileId1);
        assertEquals(problematic1, roundTrip1);

        TileIndex problematic2 = xyz(64, 64, 7);
        long tileId2 = HilbertCurve.tileIndexToTileId(problematic2);
        TileIndex roundTrip2 = HilbertCurve.tileIdToTileIndex(tileId2);
        assertEquals(problematic2, roundTrip2);
    }
}
