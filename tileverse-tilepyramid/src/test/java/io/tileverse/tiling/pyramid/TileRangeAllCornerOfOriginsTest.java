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

import static io.tileverse.tiling.common.CornerOfOrigin.BOTTOM_LEFT;
import static io.tileverse.tiling.common.CornerOfOrigin.TOP_LEFT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Comprehensive tests for TileRange functionality across all axis origins.
 * Uses simple coordinate ranges that are easy to understand and verify.
 */
class TileRangeAllCornerOfOriginsTest {

    /**
     * Test basic TileRange creation and properties for all axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testTileRangeCreationAllOrigins(CornerOfOrigin cornerOfOrigin) {
        TileRange range = TileRange.of(5, 10, 15, 20, 8, cornerOfOrigin);

        assertEquals(8, range.zoomLevel());
        assertEquals(5, range.minx());
        assertEquals(10, range.miny());
        assertEquals(15, range.maxx());
        assertEquals(20, range.maxy());
        assertEquals(cornerOfOrigin, range.cornerOfOrigin());
        assertEquals(11 * 11, range.count()); // (15-5+1) * (20-10+1)
    }

    /**
     * Test first() and last() methods return correct start/end positions for each axis origin.
     */
    @Test
    void testFirstLastPositionsAllOrigins() {
        // Simple 2x2 range: (0,0) to (1,1)

        // UPPER_LEFT: Start top-left, end bottom-right
        TileRange upperLeft = TileRange.of(0, 0, 1, 1, 5, TOP_LEFT);
        assertEquals(TileIndex.xyz(0, 0, 5), upperLeft.first()); // Top-left
        assertEquals(TileIndex.xyz(1, 1, 5), upperLeft.last()); // Bottom-right

        // LOWER_LEFT: Start bottom-left, end top-right
        TileRange lowerLeft = TileRange.of(0, 0, 1, 1, 5, BOTTOM_LEFT);
        assertEquals(TileIndex.xyz(0, 0, 5), lowerLeft.first()); // Bottom-left
        assertEquals(TileIndex.xyz(1, 1, 5), lowerLeft.last()); // Top-right
    }

    /**
     * Test complete traversal for each axis origin to ensure correct order.
     * Uses simple 2x2 grid to verify traversal patterns.
     */
    @Test
    void testCompleteTraversalAllOrigins() {
        // Test 2x2 grid traversal patterns

        // UPPER_LEFT: Should go (0,0) -> (1,0) -> (0,1) -> (1,1)
        TileRange upperLeft = TileRange.of(0, 0, 1, 1, 3, TOP_LEFT);
        List<TileIndex> upperLeftTraversal = collectTraversal(upperLeft);
        assertThat(upperLeftTraversal)
                .containsExactly(
                        TileIndex.xyz(0, 0, 3), // Top-left
                        TileIndex.xyz(1, 0, 3), // Top-right
                        TileIndex.xyz(0, 1, 3), // Bottom-left
                        TileIndex.xyz(1, 1, 3) // Bottom-right
                        );

        // LOWER_LEFT: Should go (0,0) -> (1,0) -> (0,1) -> (1,1)
        TileRange lowerLeft = TileRange.of(0, 0, 1, 1, 3, BOTTOM_LEFT);
        List<TileIndex> lowerLeftTraversal = collectTraversal(lowerLeft);
        assertThat(lowerLeftTraversal)
                .containsExactly(
                        TileIndex.xyz(0, 0, 3), // Bottom-left
                        TileIndex.xyz(1, 0, 3), // Bottom-right
                        TileIndex.xyz(0, 1, 3), // Top-left
                        TileIndex.xyz(1, 1, 3) // Top-right
                        );
    }

    /**
     * Test reverse traversal using prev() for all axis origins.
     */
    @Test
    void testReverseTraversalAllOrigins() {
        // Test reverse traversal on 2x2 grid

        // UPPER_LEFT: Uniform reverse traversal - should go (1,1) -> (0,1) -> (1,0) -> (0,0)
        TileRange upperLeft = TileRange.of(0, 0, 1, 1, 4, TOP_LEFT);
        List<TileIndex> upperLeftReverse = collectReverseTraversal(upperLeft);
        assertThat(upperLeftReverse)
                .containsExactly(
                        TileIndex.xyz(1, 1, 4), // (maxx, maxy)
                        TileIndex.xyz(0, 1, 4), // (maxx-1, maxy)
                        TileIndex.xyz(1, 0, 4), // (maxx, maxy-1)
                        TileIndex.xyz(0, 0, 4) // (minx, miny)
                        );

        // LOWER_LEFT: Uniform reverse traversal - should go (1,1) -> (0,1) -> (1,0) -> (0,0)
        TileRange lowerLeft = TileRange.of(0, 0, 1, 1, 4, BOTTOM_LEFT);
        List<TileIndex> lowerLeftReverse = collectReverseTraversal(lowerLeft);
        assertThat(lowerLeftReverse)
                .containsExactly(
                        TileIndex.xyz(1, 1, 4), // (maxx, maxy)
                        TileIndex.xyz(0, 1, 4), // (maxx-1, maxy)
                        TileIndex.xyz(1, 0, 4), // (maxx, maxy-1)
                        TileIndex.xyz(0, 0, 4) // (minx, miny)
                        );
    }

    /**
     * Test next() and prev() methods work correctly at boundaries.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testBoundaryTransitionsAllOrigins(CornerOfOrigin cornerOfOrigin) {
        TileRange range = TileRange.of(10, 20, 12, 22, 6, cornerOfOrigin); // 3x3 grid

        TileIndex first = range.first();
        TileIndex second = range.next(first).orElseThrow();
        TileIndex backToFirst = range.prev(second).orElseThrow();

        assertEquals(first, backToFirst, "prev(next(first)) should return first");

        TileIndex last = range.last();
        TileIndex secondLast = range.prev(last).orElseThrow();
        TileIndex backToLast = range.next(secondLast).orElseThrow();

        assertEquals(last, backToLast, "next(prev(last)) should return last");
    }

    /**
     * Test intersection behavior for different axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testIntersectionAllOrigins(CornerOfOrigin cornerOfOrigin) {
        TileRange rangeA = TileRange.of(0, 0, 10, 10, 5, cornerOfOrigin);
        TileRange rangeB = TileRange.of(5, 5, 15, 15, 5, cornerOfOrigin);

        Optional<TileRange> intersection = rangeA.intersection(rangeB);
        assertTrue(intersection.isPresent());

        TileRange result = intersection.get();
        assertEquals(cornerOfOrigin, result.cornerOfOrigin());
        assertEquals(5, result.minx());
        assertEquals(5, result.miny());
        assertEquals(10, result.maxx());
        assertEquals(10, result.maxy());
        assertEquals(5, result.zoomLevel());
        assertEquals(36, result.count()); // 6x6 grid
    }

    /**
     * Test contains() method for different axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testContainsAllOrigins(CornerOfOrigin cornerOfOrigin) {
        TileRange range = TileRange.of(5, 10, 15, 20, 7, cornerOfOrigin);

        // Test points inside range
        assertTrue(range.contains(TileIndex.xyz(5, 10, 7))); // Min corner
        assertTrue(range.contains(TileIndex.xyz(15, 20, 7))); // Max corner
        assertTrue(range.contains(TileIndex.xyz(10, 15, 7))); // Center

        // Test points outside range
        assertFalse(range.contains(TileIndex.xyz(4, 10, 7))); // Outside X
        assertFalse(range.contains(TileIndex.xyz(5, 9, 7))); // Outside Y
        assertFalse(range.contains(TileIndex.xyz(5, 10, 6))); // Wrong zoom
    }

    /**
     * Test min() and max() always return simple coordinate bounds regardless of axis origin.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testMinMaxAllOrigins(CornerOfOrigin cornerOfOrigin) {
        TileRange range = TileRange.of(3, 7, 13, 17, 9, cornerOfOrigin);

        // min() and max() should always return the same values regardless of axis origin
        assertEquals(TileIndex.xyz(3, 7, 9), range.min());
        assertEquals(TileIndex.xyz(13, 17, 9), range.max());

        // But first() and last() should vary by axis origin
        TileIndex first = range.first();
        TileIndex last = range.last();

        assertNotEquals(first, last);
        assertEquals(9, first.z());
        assertEquals(9, last.z());
    }

    /**
     * Collect complete forward traversal of a tile range.
     */
    private List<TileIndex> collectTraversal(TileRange range) {
        List<TileIndex> result = new ArrayList<>();
        TileIndex current = range.first();
        result.add(current);

        while (true) {
            Optional<TileIndex> next = range.next(current);
            if (next.isEmpty()) break;
            current = next.get();
            result.add(current);
        }

        return result;
    }

    /**
     * Collect complete reverse traversal of a tile range.
     */
    private List<TileIndex> collectReverseTraversal(TileRange range) {
        List<TileIndex> result = new ArrayList<>();
        TileIndex current = range.last();
        result.add(current);

        while (true) {
            Optional<TileIndex> prev = range.prev(current);
            if (prev.isEmpty()) break;
            current = prev.get();
            result.add(current);
        }

        return result;
    }
}
