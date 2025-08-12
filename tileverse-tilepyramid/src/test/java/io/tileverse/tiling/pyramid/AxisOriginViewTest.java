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
package io.tileverse.tiling.pyramid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Comprehensive tests for AxisOriginView coordinate transformations.
 * Tests all possible axis origin conversions and edge cases.
 */
class AxisOriginViewTest {

    @Test
    void testNoTransformationWhenSameAxisOrigin() {
        TileRange original = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.LOWER_LEFT);
        TileRange view = original.withAxisOrigin(AxisOrigin.LOWER_LEFT);

        // Should return the same instance when no transformation needed
        assertSame(original, view);
    }

    @Test
    void testLowerLeftToUpperLeftTransformation() {
        // Test with zoom level 2 (4x4 grid: coordinates 0-3)
        TileRange lowerLeft = TileRange.of(1, 1, 2, 2, 2, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = lowerLeft.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // At zoom 2: tiles per side = 2^2 = 4, so max coordinate = 3
        // Y-flip transformation: newY = (tilesPerSide - 1) - oldY = 3 - oldY
        // LOWER_LEFT (1,1,2,2) -> UPPER_LEFT (1,1,2,2) with Y coordinates flipped
        // minY: 3 - 2 = 1, maxY: 3 - 1 = 2
        assertEquals(1, upperLeft.minx());
        assertEquals(1, upperLeft.miny()); // 3 - 2 = 1
        assertEquals(2, upperLeft.maxx());
        assertEquals(2, upperLeft.maxy()); // 3 - 1 = 2
        assertEquals(2, upperLeft.zoomLevel());
        assertEquals(AxisOrigin.UPPER_LEFT, upperLeft.axisOrigin());

        // Verify it's an AxisOriginView
        assertTrue(upperLeft instanceof AxisOriginView);
    }

    @Test
    void testUpperLeftToLowerLeftTransformation() {
        // Test reverse transformation
        TileRange upperLeft = TileRange.of(1, 1, 2, 2, 2, AxisOrigin.UPPER_LEFT);
        TileRange lowerLeft = upperLeft.withAxisOrigin(AxisOrigin.LOWER_LEFT);

        // Same transformation logic applies in reverse
        assertEquals(1, lowerLeft.minx());
        assertEquals(1, lowerLeft.miny()); // 3 - 2 = 1
        assertEquals(2, lowerLeft.maxx());
        assertEquals(2, lowerLeft.maxy()); // 3 - 1 = 2
        assertEquals(2, lowerLeft.zoomLevel());
        assertEquals(AxisOrigin.LOWER_LEFT, lowerLeft.axisOrigin());
    }

    @Test
    void testRoundTripTransformation() {
        TileRange original = TileRange.of(5, 10, 15, 20, 4, AxisOrigin.LOWER_LEFT);

        // Transform to all other axis origins and back
        TileRange upperLeft = original.withAxisOrigin(AxisOrigin.UPPER_LEFT);
        TileRange backToLowerLeft = upperLeft.withAxisOrigin(AxisOrigin.LOWER_LEFT);

        // Should have same coordinates after round trip
        assertEquals(original.minx(), backToLowerLeft.minx());
        assertEquals(original.miny(), backToLowerLeft.miny());
        assertEquals(original.maxx(), backToLowerLeft.maxx());
        assertEquals(original.maxy(), backToLowerLeft.maxy());
        assertEquals(original.zoomLevel(), backToLowerLeft.zoomLevel());
        assertEquals(original.axisOrigin(), backToLowerLeft.axisOrigin());
    }

    @Test
    void testLeftToRightAxisTransformations() {
        // Test X-axis flipping transformations
        TileRange lowerLeft = TileRange.of(1, 1, 2, 2, 2, AxisOrigin.LOWER_LEFT);
        TileRange lowerRight = lowerLeft.withAxisOrigin(AxisOrigin.LOWER_RIGHT);

        // At zoom 2: tiles per side = 4, so max coordinate = 3
        // X-flip: newX = (tilesPerSide - 1) - oldX = 3 - oldX
        // minX: 3 - 2 = 1, maxX: 3 - 1 = 2
        assertEquals(1, lowerRight.minx()); // 3 - 2 = 1
        assertEquals(1, lowerRight.miny()); // Y unchanged for LOWER_* -> LOWER_*
        assertEquals(2, lowerRight.maxx()); // 3 - 1 = 2
        assertEquals(2, lowerRight.maxy()); // Y unchanged
        assertEquals(AxisOrigin.LOWER_RIGHT, lowerRight.axisOrigin());
    }

    @Test
    void testAllAxisOriginCombinations() {
        TileRange original = TileRange.of(0, 0, 1, 1, 1, AxisOrigin.LOWER_LEFT);

        // Test transformation to each axis origin
        AxisOrigin[] origins = AxisOrigin.values();
        for (AxisOrigin target : origins) {
            TileRange transformed = original.withAxisOrigin(target);
            assertEquals(target, transformed.axisOrigin());
            assertEquals(original.zoomLevel(), transformed.zoomLevel());

            // Verify spans don't change (area is preserved)
            assertEquals(original.spanX(), transformed.spanX());
            assertEquals(original.spanY(), transformed.spanY());
            assertEquals(original.count(), transformed.count());
        }
    }

    @Test
    void testChainedTransformations() {
        TileRange original = TileRange.of(2, 3, 5, 7, 3, AxisOrigin.LOWER_LEFT);

        // Chain multiple transformations
        TileRange chained = original.withAxisOrigin(AxisOrigin.UPPER_LEFT)
                .withAxisOrigin(AxisOrigin.UPPER_RIGHT)
                .withAxisOrigin(AxisOrigin.LOWER_RIGHT)
                .withAxisOrigin(AxisOrigin.LOWER_LEFT);

        // Should return to original coordinates
        assertEquals(original.minx(), chained.minx());
        assertEquals(original.miny(), chained.miny());
        assertEquals(original.maxx(), chained.maxx());
        assertEquals(original.maxy(), chained.maxy());
        assertEquals(original.zoomLevel(), chained.zoomLevel());
        assertEquals(original.axisOrigin(), chained.axisOrigin());
    }

    @Test
    void testEdgeCaseZeroCoordinates() {
        // Test with coordinates at origin
        TileRange origin = TileRange.of(0, 0, 0, 0, 1, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = origin.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // At zoom 1: tiles per side = 2, max coordinate = 1
        // (0,0) in LOWER_LEFT -> (0,1) in UPPER_LEFT
        assertEquals(0, upperLeft.minx());
        assertEquals(1, upperLeft.miny()); // 1 - 0 = 1
        assertEquals(0, upperLeft.maxx());
        assertEquals(1, upperLeft.maxy());
    }

    @Test
    void testEdgeCaseMaxCoordinates() {
        // Test with coordinates at maximum for zoom level
        int zoom = 2;
        long maxCoord = (1L << zoom) - 1; // 3 for zoom level 2

        TileRange maxRange = TileRange.of(maxCoord, maxCoord, maxCoord, maxCoord, zoom, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = maxRange.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // Max coordinate (3,3) in LOWER_LEFT -> (3,0) in UPPER_LEFT
        assertEquals(maxCoord, upperLeft.minx());
        assertEquals(0, upperLeft.miny()); // 3 - 3 = 0
        assertEquals(maxCoord, upperLeft.maxx());
        assertEquals(0, upperLeft.maxy());
    }

    @Test
    void testHighZoomLevel() {
        // Test with higher zoom level to ensure no overflow
        int zoom = 10;
        TileRange range = TileRange.of(100, 200, 300, 400, zoom, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = range.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        long tilesPerSide = 1L << zoom; // 1024 for zoom 10
        long maxCoord = tilesPerSide - 1; // 1023

        // Verify Y coordinates are flipped correctly
        assertEquals(100, upperLeft.minx()); // X unchanged
        assertEquals(maxCoord - 400, upperLeft.miny()); // 1023 - 400 = 623
        assertEquals(300, upperLeft.maxx()); // X unchanged
        assertEquals(maxCoord - 200, upperLeft.maxy()); // 1023 - 200 = 823
    }

    @Test
    void testSpanAndCountPreservation() {
        TileRange original = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.LOWER_LEFT);

        for (AxisOrigin target : AxisOrigin.values()) {
            TileRange transformed = original.withAxisOrigin(target);

            // Spans and counts must be preserved
            assertEquals(
                    original.spanX(),
                    transformed.spanX(),
                    "X span should be preserved for transformation to " + target);
            assertEquals(
                    original.spanY(),
                    transformed.spanY(),
                    "Y span should be preserved for transformation to " + target);
            assertEquals(
                    original.count(),
                    transformed.count(),
                    "Tile count should be preserved for transformation to " + target);
        }
    }

    @Test
    void testMetaTileCountPreservation() {
        TileRange original = TileRange.of(0, 0, 7, 7, 3, AxisOrigin.LOWER_LEFT);

        for (AxisOrigin target : AxisOrigin.values()) {
            TileRange transformed = original.withAxisOrigin(target);

            // Meta-tile counts should be preserved
            assertEquals(
                    original.countMetaTiles(2, 2),
                    transformed.countMetaTiles(2, 2),
                    "2x2 meta-tile count should be preserved for transformation to " + target);
            assertEquals(
                    original.countMetaTiles(4, 4),
                    transformed.countMetaTiles(4, 4),
                    "4x4 meta-tile count should be preserved for transformation to " + target);
        }
    }

    @Test
    void testCornerMethodsAxisOriginAware() {
        TileRange lowerLeft = TileRange.of(1, 1, 2, 2, 2, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = lowerLeft.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // Corner methods should return different coordinates based on axis origin
        assertNotEquals(lowerLeft.lowerLeft(), upperLeft.lowerLeft());
        assertNotEquals(lowerLeft.upperRight(), upperLeft.upperRight());

        // But should still represent the same logical corners in their respective coordinate systems
        TileIndex llCorner = upperLeft.lowerLeft();
        TileIndex urCorner = upperLeft.upperRight();

        // Verify coordinates are within the transformed range
        assertTrue(llCorner.x() >= upperLeft.minx() && llCorner.x() <= upperLeft.maxx());
        assertTrue(llCorner.y() >= upperLeft.miny() && llCorner.y() <= upperLeft.maxy());
        assertTrue(urCorner.x() >= upperLeft.minx() && urCorner.x() <= upperLeft.maxx());
        assertTrue(urCorner.y() >= upperLeft.miny() && urCorner.y() <= upperLeft.maxy());
    }

    @Test
    void testContainsWithTransformedCoordinates() {
        TileRange lowerLeft = TileRange.of(1, 0, 2, 1, 2, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = lowerLeft.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // At zoom 2: tilesPerSide = 4, maxCoord = 3
        // LOWER_LEFT (1,0,2,1) -> UPPER_LEFT (1,2,2,3) after Y-flip
        // minY: 3-1=2, maxY: 3-0=3

        assertEquals(1, upperLeft.minx());
        assertEquals(2, upperLeft.miny()); // 3 - 1 = 2
        assertEquals(2, upperLeft.maxx());
        assertEquals(3, upperLeft.maxy()); // 3 - 0 = 3

        // Test contains with coordinates in the original system
        TileIndex originalTile = TileIndex.of(1, 0, 2); // Bottom-left tile in LOWER_LEFT
        assertTrue(lowerLeft.contains(originalTile));

        // Test contains with coordinates in the transformed system
        TileIndex transformedTile =
                TileIndex.of(1, 3, 2); // Top-left tile in UPPER_LEFT (corresponds to 1,0 in LOWER_LEFT)
        assertTrue(upperLeft.contains(transformedTile));

        // Cross-system containment should be false (different coordinate systems)
        assertFalse(upperLeft.contains(originalTile)); // (1,0) not in UPPER_LEFT range (1,2,2,3)
        assertFalse(lowerLeft.contains(transformedTile)); // (1,3) not in LOWER_LEFT range (1,0,2,1)
    }

    @Test
    void testEqualsAndHashCodeWithAxisOriginView() {
        TileRange original = TileRange.of(5, 10, 15, 20, 4, AxisOrigin.LOWER_LEFT);
        TileRange view = original.withAxisOrigin(AxisOrigin.UPPER_LEFT);
        TileRange backToOriginal = view.withAxisOrigin(AxisOrigin.LOWER_LEFT);

        // Original and round-trip should be equal
        assertEquals(original, backToOriginal);
        assertEquals(original.hashCode(), backToOriginal.hashCode());

        // View with different axis origin should not equal original
        assertNotEquals(original, view);
        assertNotEquals(original.hashCode(), view.hashCode());
    }

    @Test
    void testToStringContainsAxisOriginInfo() {
        TileRange original = TileRange.of(1, 2, 3, 4, 5, AxisOrigin.LOWER_LEFT);
        TileRange view = original.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        String viewString = view.toString();
        assertTrue(viewString.contains("AxisOriginView"), "toString should indicate it's an AxisOriginView");
        assertTrue(viewString.contains("UPPER_LEFT"), "toString should show target axis origin");
    }

    @Test
    void testTraversalMethodsWorkWithAxisOriginView() {
        TileRange lowerLeft = TileRange.of(0, 0, 1, 1, 1, AxisOrigin.LOWER_LEFT);
        TileRange upperLeft = lowerLeft.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // Test that traversal methods work correctly with transformed coordinates
        TileIndex first = upperLeft.first();
        TileIndex last = upperLeft.last();

        assertNotNull(first);
        assertNotNull(last);
        assertTrue(upperLeft.contains(first));
        assertTrue(upperLeft.contains(last));

        // Test next/prev work with the view
        assertTrue(upperLeft.next(first).isPresent());
        assertTrue(upperLeft.prev(last).isPresent());
    }
}
