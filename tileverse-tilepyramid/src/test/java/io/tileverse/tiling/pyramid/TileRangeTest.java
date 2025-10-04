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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Test class for TileRange functionality combining 2D and 3D operations.
 */
class TileRangeTest {

    @Test
    void testConstructionFromCoordinates() {
        TileRange range = TileRange.of(10, 20, 30, 40, 5);
        assertEquals(5, range.zoomLevel());
        assertEquals(10, range.minx());
        assertEquals(20, range.miny());
        assertEquals(30, range.maxx());
        assertEquals(40, range.maxy());
    }

    @Test
    void testConstructionFromTileIndices() {
        TileIndex lowerLeft = TileIndex.xyz(10, 20, 7);
        TileIndex upperRight = TileIndex.xyz(30, 40, 7);
        TileRange range = TileRange.of(lowerLeft, upperRight);

        assertEquals(7, range.zoomLevel());
        assertEquals(10, range.minx());
        assertEquals(20, range.miny());
        assertEquals(30, range.maxx());
        assertEquals(40, range.maxy());
        assertEquals(lowerLeft, range.bottomLeft());
        assertEquals(upperRight, range.topRight());
    }

    @Test
    void testInvalidRange() {
        // Test invalid ranges where lowerLeft > upperRight
        assertThrows(IllegalArgumentException.class, () -> TileRange.of(30, 20, 10, 40, 5));
        assertThrows(IllegalArgumentException.class, () -> TileRange.of(10, 40, 30, 20, 5));
    }

    @Test
    void testSpanCalculations() {
        TileRange range = TileRange.of(0, 0, 9, 9, 2);

        assertEquals(10, range.spanX()); // 0-9 inclusive = 10
        assertEquals(10, range.spanY()); // 0-9 inclusive = 10
        assertEquals(100, range.count()); // 10x10 = 100
    }

    @Test
    void testTileRangeProperties() {
        TileRange range = TileRange.of(0, 0, 2, 1, 3); // 3x2 = 6 tiles at zoom 3

        assertEquals(6, range.count());
        assertEquals(3, range.spanX());
        assertEquals(2, range.spanY());
        assertEquals(3, range.zoomLevel());

        // Verify corner coordinates
        assertEquals(TileIndex.xyz(0, 0, 3), range.bottomLeft());
        assertEquals(TileIndex.xyz(2, 1, 3), range.topRight());
    }

    @Test
    void testCompareTo() {
        TileRange range1 = TileRange.of(0, 0, 5, 5, 1);
        TileRange range2 = TileRange.of(0, 0, 5, 5, 1);
        TileRange range3 = TileRange.of(0, 0, 5, 5, 2); // higher zoom
        TileRange range4 = TileRange.of(1, 0, 5, 5, 1); // same zoom, different bounds

        assertEquals(0, range1.compareTo(range2));
        assertTrue(range1.compareTo(range3) < 0); // lower zoom comes first
        assertTrue(range3.compareTo(range1) > 0);
        assertTrue(range1.compareTo(range4) < 0); // same zoom, compare by bounds
    }

    @Test
    void testFactoryMethodConstructor() {
        TileRange range = TileRange.of(10, 20, 30, 40, 5);

        assertEquals(5, range.zoomLevel());
        assertEquals(10, range.minx());
        assertEquals(20, range.miny());
        assertEquals(30, range.maxx());
        assertEquals(40, range.maxy());
        assertEquals(TileIndex.xyz(10, 20, 5), range.bottomLeft());
        assertEquals(TileIndex.xyz(30, 40, 5), range.topRight());
    }

    @Test
    void testSingleTileRange() {
        TileRange range = TileRange.of(5, 10, 5, 10, 3); // Single tile

        assertEquals(1, range.spanX());
        assertEquals(1, range.spanY());
        assertEquals(1, range.count());

        // Verify corner coordinates for single tile
        assertEquals(TileIndex.xyz(5, 10, 3), range.bottomLeft());
        assertEquals(TileIndex.xyz(5, 10, 3), range.topRight());
    }

    @Test
    void testNegativeCoordinates() {
        TileRange range = TileRange.of(-10, -20, -5, -15, 2);

        assertEquals(-10, range.minx());
        assertEquals(-20, range.miny());
        assertEquals(-5, range.maxx());
        assertEquals(-15, range.maxy());
        assertEquals(6, range.spanX()); // -10 to -5 = 6 tiles
        assertEquals(6, range.spanY()); // -20 to -15 = 6 tiles
        assertEquals(36, range.count());
    }

    @Test
    void testEqualsAndHashCode() {
        TileRange range1 = TileRange.of(10, 20, 30, 40, 5);
        TileRange range2 = TileRange.of(10, 20, 30, 40, 5);
        TileRange range3 = TileRange.of(10, 20, 30, 40, 6); // different zoom
        TileRange range4 = TileRange.of(11, 20, 30, 40, 5); // different coords

        // Test equality
        assertEquals(range1, range2);
        assertNotEquals(range1, range3);
        assertNotEquals(range1, range4);
        assertNotEquals(range1, null);

        // Test hash code consistency
        assertEquals(range1.hashCode(), range2.hashCode());

        // Test static methods
        assertTrue(TileRange.equals(range1, range2));
        assertFalse(TileRange.equals(range1, range3));
        assertFalse(TileRange.equals(range1, null));
        assertFalse(TileRange.equals(null, range1));
        assertTrue(TileRange.equals(null, null));

        assertEquals(TileRange.hashCode(range1), TileRange.hashCode(range2));
        assertEquals(0, TileRange.hashCode(null));
    }

    @Test
    void testCornerOfOriginDefault() {
        // Default axis origin should be LOWER_LEFT
        TileRange range = TileRange.of(0, 0, 3, 3, 2);
        assertEquals(CornerOfOrigin.BOTTOM_LEFT, range.cornerOfOrigin());
    }

    @Test
    void testCornerOfOriginExplicit() {
        // Explicit axis origin
        TileRange range = TileRange.of(0, 0, 3, 3, 2, CornerOfOrigin.TOP_LEFT);
        assertEquals(CornerOfOrigin.TOP_LEFT, range.cornerOfOrigin());
    }

    @Test
    void testCornerOfOriginTransformation() {
        // Create a 4x4 tile range at zoom level 2 (2^2 = 4 tiles per side)
        TileRange lowerLeftRange = TileRange.of(0, 0, 3, 3, 2, CornerOfOrigin.BOTTOM_LEFT);

        // Convert to upper-left origin
        TileRange upperLeftRange = lowerLeftRange.withCornerOfOrigin(CornerOfOrigin.TOP_LEFT);

        // In a 4x4 grid (zoom 2), coordinates should flip:
        // LOWER_LEFT (0,0) -> UPPER_LEFT (0,3)
        // LOWER_LEFT (3,3) -> UPPER_LEFT (3,0)
        assertEquals(CornerOfOrigin.TOP_LEFT, upperLeftRange.cornerOfOrigin());
        assertEquals(0, upperLeftRange.minx()); // X doesn't change
        assertEquals(0, upperLeftRange.miny()); // (2^2-1) - 3 = 0
        assertEquals(3, upperLeftRange.maxx()); // X doesn't change
        assertEquals(3, upperLeftRange.maxy()); // (2^2-1) - 0 = 3

        // Convert back should restore original coordinates
        TileRange backToLowerLeft = upperLeftRange.withCornerOfOrigin(CornerOfOrigin.BOTTOM_LEFT);
        assertEquals(lowerLeftRange.minx(), backToLowerLeft.minx());
        assertEquals(lowerLeftRange.miny(), backToLowerLeft.miny());
        assertEquals(lowerLeftRange.maxx(), backToLowerLeft.maxx());
        assertEquals(lowerLeftRange.maxy(), backToLowerLeft.maxy());
        assertEquals(CornerOfOrigin.BOTTOM_LEFT, backToLowerLeft.cornerOfOrigin());
    }

    @Test
    void testCornerOfOriginNoTransformation() {
        // Converting to same origin should return same instance
        TileRange range = TileRange.of(0, 0, 3, 3, 2, CornerOfOrigin.BOTTOM_LEFT);
        TileRange sameOrigin = range.withCornerOfOrigin(CornerOfOrigin.BOTTOM_LEFT);

        assertTrue(range == sameOrigin); // Same instance
    }

    @Test
    void testTilePyramidAxisConsistency() {
        // Create ranges with different axis origins
        TileRange lowerLeftRange = TileRange.of(0, 0, 1, 1, 0, CornerOfOrigin.BOTTOM_LEFT);
        TileRange upperLeftRange = TileRange.of(0, 0, 1, 1, 1, CornerOfOrigin.TOP_LEFT);

        // Builder should convert all ranges to pyramid's axis origin
        TilePyramid pyramid = TilePyramid.builder()
                .cornerOfOrigin(CornerOfOrigin.TOP_LEFT)
                .level(lowerLeftRange)
                .level(upperLeftRange)
                .build();

        // All levels should have UPPER_LEFT axis origin
        assertEquals(CornerOfOrigin.TOP_LEFT, pyramid.cornerOfOrigin());
        for (TileRange level : pyramid.levels()) {
            assertEquals(CornerOfOrigin.TOP_LEFT, level.cornerOfOrigin());
        }
    }

    @Test
    void testCornerMethodsLowerLeft() {
        // Test all four corner methods with LOWER_LEFT axis origin (default)
        TileRange range = TileRange.of(10, 20, 30, 40, 5, CornerOfOrigin.BOTTOM_LEFT);

        // Lower-left corner (minx, miny) for LOWER_LEFT
        TileIndex lowerLeft = range.bottomLeft();
        assertEquals(10, lowerLeft.x());
        assertEquals(20, lowerLeft.y());
        assertEquals(5, lowerLeft.z());

        // Upper-right corner (maxx, maxy) for LOWER_LEFT
        TileIndex upperRight = range.topRight();
        assertEquals(30, upperRight.x());
        assertEquals(40, upperRight.y());
        assertEquals(5, upperRight.z());

        // Lower-right corner (maxx, miny) for LOWER_LEFT
        TileIndex lowerRight = range.bottomRight();
        assertEquals(30, lowerRight.x());
        assertEquals(20, lowerRight.y());
        assertEquals(5, lowerRight.z());

        // Upper-left corner (minx, maxy) for LOWER_LEFT
        TileIndex upperLeft = range.topLeft();
        assertEquals(10, upperLeft.x());
        assertEquals(40, upperLeft.y());
        assertEquals(5, upperLeft.z());
    }

    @Test
    void testCornerMethodsUpperLeft() {
        // Test corner methods with UPPER_LEFT axis origin (like PMTiles)
        TileRange range = TileRange.of(10, 20, 30, 40, 5, CornerOfOrigin.TOP_LEFT);

        // Lower-left corner: In UPPER_LEFT, "lower" means higher Y values
        // So lower-left = (minx, maxy)
        TileIndex lowerLeft = range.bottomLeft();
        assertEquals(10, lowerLeft.x());
        assertEquals(40, lowerLeft.y()); // maxy for UPPER_LEFT
        assertEquals(5, lowerLeft.z());

        // Upper-right corner: In UPPER_LEFT, "upper" means lower Y values
        // So upper-right = (maxx, miny)
        TileIndex upperRight = range.topRight();
        assertEquals(30, upperRight.x());
        assertEquals(20, upperRight.y()); // miny for UPPER_LEFT
        assertEquals(5, upperRight.z());

        // Lower-right corner: (maxx, maxy) for UPPER_LEFT
        TileIndex lowerRight = range.bottomRight();
        assertEquals(30, lowerRight.x());
        assertEquals(40, lowerRight.y()); // maxy for UPPER_LEFT
        assertEquals(5, lowerRight.z());

        // Upper-left corner: (minx, miny) for UPPER_LEFT
        TileIndex upperLeft = range.topLeft();
        assertEquals(10, upperLeft.x());
        assertEquals(20, upperLeft.y()); // miny for UPPER_LEFT
        assertEquals(5, upperLeft.z());
    }

    @Test
    void testFirstLastMethods() {
        // Test first() and last() methods for traversal
        TileRange lowerLeftRange = TileRange.of(10, 20, 30, 40, 5, CornerOfOrigin.BOTTOM_LEFT);
        TileRange upperLeftRange = TileRange.of(10, 20, 30, 40, 5, CornerOfOrigin.TOP_LEFT);

        // For LOWER_LEFT: first() equals lowerLeft(), last() equals upperRight()
        assertEquals(lowerLeftRange.bottomLeft(), lowerLeftRange.first());
        assertEquals(lowerLeftRange.topRight(), lowerLeftRange.last());

        // For UPPER_LEFT: first() equals upperLeft() (top-left), last() equals lowerRight() (bottom-right)
        assertEquals(upperLeftRange.topLeft(), upperLeftRange.first());
        assertEquals(upperLeftRange.bottomRight(), upperLeftRange.last());

        // Verify the actual coordinates for LOWER_LEFT
        TileIndex lowerLeftFirst = lowerLeftRange.first();
        assertEquals(10, lowerLeftFirst.x()); // minx, miny
        assertEquals(20, lowerLeftFirst.y());

        TileIndex lowerLeftLast = lowerLeftRange.last();
        assertEquals(30, lowerLeftLast.x()); // maxx, maxy
        assertEquals(40, lowerLeftLast.y());

        // Verify the actual coordinates for UPPER_LEFT
        TileIndex upperLeftFirst = upperLeftRange.first();
        assertEquals(10, upperLeftFirst.x()); // minx, miny (top-left in UPPER_LEFT)
        assertEquals(20, upperLeftFirst.y());

        TileIndex upperLeftLast = upperLeftRange.last();
        assertEquals(30, upperLeftLast.x()); // maxx, maxy (bottom-right in UPPER_LEFT)
        assertEquals(40, upperLeftLast.y());
    }

    @Test
    void testNextPrevMethods() {
        // Test with a small 2x2 range for easy verification
        TileRange range = TileRange.of(10, 20, 11, 21, 5, CornerOfOrigin.BOTTOM_LEFT);

        // Expected traversal order for LOWER_LEFT (left-to-right, bottom-to-top):
        // (10,20) -> (11,20) -> (10,21) -> (11,21)

        TileIndex first = range.first(); // (10,20)
        assertEquals(10, first.x());
        assertEquals(20, first.y());

        // Test next() progression
        Optional<TileIndex> next1 = range.next(first);
        assertTrue(next1.isPresent());
        assertEquals(11, next1.get().x()); // (11,20)
        assertEquals(20, next1.get().y());

        Optional<TileIndex> next2 = range.next(next1.get());
        assertTrue(next2.isPresent());
        assertEquals(10, next2.get().x()); // (10,21) - wrap to next row
        assertEquals(21, next2.get().y());

        Optional<TileIndex> next3 = range.next(next2.get());
        assertTrue(next3.isPresent());
        assertEquals(11, next3.get().x()); // (11,21)
        assertEquals(21, next3.get().y());

        // Should be the last tile
        assertEquals(range.last(), next3.get());

        // Next from last should be empty
        Optional<TileIndex> next4 = range.next(next3.get());
        assertTrue(next4.isEmpty());

        // Test prev() progression (reverse)
        Optional<TileIndex> prev1 = range.prev(next3.get());
        assertTrue(prev1.isPresent());
        assertEquals(next2.get(), prev1.get()); // (10,21)

        Optional<TileIndex> prev2 = range.prev(prev1.get());
        assertTrue(prev2.isPresent());
        assertEquals(next1.get(), prev2.get()); // (11,20)

        Optional<TileIndex> prev3 = range.prev(prev2.get());
        assertTrue(prev3.isPresent());
        assertEquals(first, prev3.get()); // (10,20)

        // Prev from first should be empty
        Optional<TileIndex> prev4 = range.prev(prev3.get());
        assertTrue(prev4.isEmpty());
    }

    @Test
    void testNextPrevUpperLeft() {
        // Test with UPPER_LEFT axis origin (like PMTiles)
        TileRange range = TileRange.of(10, 20, 11, 21, 5, CornerOfOrigin.TOP_LEFT);

        // Expected traversal order for UPPER_LEFT (left-to-right, top-to-bottom):
        // first() = (minx, miny) = (10,20) for UPPER_LEFT
        // last() = (maxx, maxy) = (11,21) for UPPER_LEFT
        // Traversal: (10,20) -> (11,20) -> (10,21) -> (11,21)

        TileIndex first = range.first(); // (minx, miny) = (10,20)
        assertEquals(10, first.x());
        assertEquals(20, first.y());

        // Test complete traversal
        Optional<TileIndex> next1 = range.next(first);
        assertTrue(next1.isPresent());
        assertEquals(11, next1.get().x()); // (11,20) - increment X first
        assertEquals(20, next1.get().y());

        Optional<TileIndex> next2 = range.next(next1.get());
        assertTrue(next2.isPresent());
        assertEquals(10, next2.get().x()); // (10,21) - wrap to next row, increment Y
        assertEquals(21, next2.get().y());

        Optional<TileIndex> next3 = range.next(next2.get());
        assertTrue(next3.isPresent());
        assertEquals(11, next3.get().x()); // (11,21)
        assertEquals(21, next3.get().y());

        // Should be the last tile
        assertEquals(range.last(), next3.get());

        // Next from last should be empty
        assertTrue(range.next(next3.get()).isEmpty());
    }

    @Test
    void testNextPrevEdgeCases() {
        TileRange range = TileRange.of(10, 20, 11, 21, 5, CornerOfOrigin.BOTTOM_LEFT);

        // Test with null
        assertThrows(NullPointerException.class, () -> range.next(null));
        assertThrows(NullPointerException.class, () -> range.prev(null));

        // Test with wrong zoom level
        TileIndex wrongZoom = TileIndex.xyz(10, 20, 6);
        assertTrue(range.next(wrongZoom).isEmpty());
        assertTrue(range.prev(wrongZoom).isEmpty());

        // Test with tile outside range
        TileIndex outsideRange = TileIndex.xyz(5, 20, 5);
        assertTrue(range.next(outsideRange).isEmpty());
        assertTrue(range.prev(outsideRange).isEmpty());

        // Test with tile outside Y range
        TileIndex outsideY = TileIndex.xyz(10, 25, 5);
        assertTrue(range.next(outsideY).isEmpty());
        assertTrue(range.prev(outsideY).isEmpty());
    }

    @Test
    void testIntersection() {
        // Test overlapping ranges
        TileRange range1 = TileRange.of(5, 10, 15, 20, 8, CornerOfOrigin.TOP_LEFT);
        TileRange range2 = TileRange.of(10, 15, 25, 30, 8, CornerOfOrigin.TOP_LEFT);

        TileRange intersection = range1.intersection(range2).orElseThrow();

        assertEquals(8, intersection.zoomLevel());
        assertEquals(10, intersection.minx()); // max(5, 10)
        assertEquals(15, intersection.miny()); // max(10, 15)
        assertEquals(15, intersection.maxx()); // min(15, 25)
        assertEquals(20, intersection.maxy()); // min(20, 30)
        assertEquals(CornerOfOrigin.TOP_LEFT, intersection.cornerOfOrigin());

        // Test non-overlapping ranges
        TileRange range3 = TileRange.of(0, 0, 5, 5, 8, CornerOfOrigin.TOP_LEFT);
        TileRange range4 = TileRange.of(10, 10, 15, 15, 8, CornerOfOrigin.TOP_LEFT);

        assertThat(range3.intersection(range4)).isEmpty();

        // Test identical ranges
        TileRange range5 = TileRange.of(10, 20, 30, 40, 5, CornerOfOrigin.BOTTOM_LEFT);
        TileRange identicalIntersection = range5.intersection(range5).orElseThrow();
        assertEquals(range5.minx(), identicalIntersection.minx());
        assertEquals(range5.miny(), identicalIntersection.miny());
        assertEquals(range5.maxx(), identicalIntersection.maxx());
        assertEquals(range5.maxy(), identicalIntersection.maxy());

        // Test null intersection
        assertThrows(NullPointerException.class, () -> range5.intersection(null));
    }

    @Test
    void testIntersectionDifferentZoomLevels() {
        TileRange range1 = TileRange.of(0, 0, 10, 10, 5, CornerOfOrigin.TOP_LEFT);
        TileRange range2 = TileRange.of(5, 5, 15, 15, 6, CornerOfOrigin.TOP_LEFT); // Different zoom level

        assertThrows(IllegalArgumentException.class, () -> {
            range1.intersection(range2);
        });
    }

    @Test
    void testCompleteTraversalLowerLeft() {
        // Test that next() visits every tile in the range for LOWER_LEFT
        TileRange range = TileRange.of(10, 20, 12, 22, 5, CornerOfOrigin.BOTTOM_LEFT); // 3x3 = 9 tiles

        // Collect all tiles via traversal
        Set<TileIndex> visited = new HashSet<>();
        TileIndex current = range.first();
        visited.add(current);

        while (true) {
            Optional<TileIndex> next = range.next(current);
            if (next.isEmpty()) {
                break;
            }
            current = next.get();
            visited.add(current);
        }

        // Should have visited exactly range.count() tiles
        assertEquals(range.count(), visited.size(), "Should visit all tiles in range");
        assertEquals(9, visited.size(), "3x3 range should have 9 tiles");

        // Should have visited last tile
        assertTrue(visited.contains(range.last()), "Should visit the last tile");

        // Verify all tiles are within the range bounds
        for (TileIndex tile : visited) {
            assertTrue(tile.x() >= range.minx() && tile.x() <= range.maxx(), "X coordinate should be in range");
            assertTrue(tile.y() >= range.miny() && tile.y() <= range.maxy(), "Y coordinate should be in range");
            assertEquals(5, tile.z(), "Z coordinate should match range zoom level");
        }

        // Verify we have exactly the expected coordinates for LOWER_LEFT traversal
        // Expected order: (10,20)->(11,20)->(12,20)->(10,21)->(11,21)->(12,21)->(10,22)->(11,22)->(12,22)
        Set<TileIndex> expected = Set.of(
                TileIndex.xyz(10, 20, 5),
                TileIndex.xyz(11, 20, 5),
                TileIndex.xyz(12, 20, 5),
                TileIndex.xyz(10, 21, 5),
                TileIndex.xyz(11, 21, 5),
                TileIndex.xyz(12, 21, 5),
                TileIndex.xyz(10, 22, 5),
                TileIndex.xyz(11, 22, 5),
                TileIndex.xyz(12, 22, 5));
        assertEquals(expected, visited, "Should visit exactly the expected tiles");
    }

    @Test
    void testCompleteTraversalUpperLeft() {
        // Test that next() visits every tile in the range for UPPER_LEFT
        TileRange range = TileRange.of(10, 20, 12, 22, 5, CornerOfOrigin.TOP_LEFT); // 3x3 = 9 tiles

        // Collect all tiles via traversal
        Set<TileIndex> visited = new HashSet<>();
        TileIndex current = range.first();
        visited.add(current);

        while (true) {
            Optional<TileIndex> next = range.next(current);
            if (next.isEmpty()) {
                break;
            }
            current = next.get();
            visited.add(current);
        }

        // Should have visited exactly range.count() tiles
        assertEquals(range.count(), visited.size(), "Should visit all tiles in range");
        assertEquals(9, visited.size(), "3x3 range should have 9 tiles");

        // Should have visited last tile
        assertTrue(visited.contains(range.last()), "Should visit the last tile");

        // Verify all tiles are within the range bounds
        for (TileIndex tile : visited) {
            assertTrue(tile.x() >= range.minx() && tile.x() <= range.maxx(), "X coordinate should be in range");
            assertTrue(tile.y() >= range.miny() && tile.y() <= range.maxy(), "Y coordinate should be in range");
            assertEquals(5, tile.z(), "Z coordinate should match range zoom level");
        }

        // Verify we have exactly the expected coordinates for UPPER_LEFT traversal
        // For UPPER_LEFT: first()=(10,22), last()=(12,20)
        // Expected order: (10,22)->(11,22)->(12,22)->(10,21)->(11,21)->(12,21)->(10,20)->(11,20)->(12,20)
        Set<TileIndex> expected = Set.of(
                TileIndex.xyz(10, 20, 5),
                TileIndex.xyz(11, 20, 5),
                TileIndex.xyz(12, 20, 5),
                TileIndex.xyz(10, 21, 5),
                TileIndex.xyz(11, 21, 5),
                TileIndex.xyz(12, 21, 5),
                TileIndex.xyz(10, 22, 5),
                TileIndex.xyz(11, 22, 5),
                TileIndex.xyz(12, 22, 5));
        assertEquals(expected, visited, "Should visit exactly the expected tiles");
    }

    @Test
    void testCompleteTraversalLargerRange() {
        // Test with a larger range to catch edge cases
        TileRange range = TileRange.of(100, 200, 104, 206, 8, CornerOfOrigin.TOP_LEFT); // 5x7 = 35 tiles

        // Count tiles via traversal
        int visitedCount = 0;
        TileIndex current = range.first();
        visitedCount++;

        while (true) {
            Optional<TileIndex> next = range.next(current);
            if (next.isEmpty()) {
                break;
            }
            current = next.get();
            visitedCount++;
        }

        // Should have visited exactly range.count() tiles
        assertEquals(range.count(), visitedCount, "Should visit all tiles in larger range");
        assertEquals(35, visitedCount, "5x7 range should have 35 tiles");

        // Should end at the last tile
        assertEquals(range.last(), current, "Should end at the last tile");
    }

    @Test
    void testReverseTraversalUpperLeft() {
        // Test that prev() visits every tile in reverse for UPPER_LEFT
        TileRange range = TileRange.of(10, 20, 12, 22, 5, CornerOfOrigin.TOP_LEFT); // 3x3 = 9 tiles

        // Collect all tiles via reverse traversal
        Set<TileIndex> visited = new HashSet<>();
        TileIndex current = range.last();
        visited.add(current);

        while (true) {
            Optional<TileIndex> prev = range.prev(current);
            if (prev.isEmpty()) {
                break;
            }
            current = prev.get();
            visited.add(current);
        }

        // Should have visited exactly range.count() tiles
        assertEquals(range.count(), visited.size(), "Should visit all tiles in reverse");
        assertEquals(9, visited.size(), "3x3 range should have 9 tiles");

        // Should have visited first tile
        assertTrue(visited.contains(range.first()), "Should visit the first tile in reverse");

        // Should end at the first tile
        assertEquals(range.first(), current, "Should end at the first tile in reverse");
    }
}
