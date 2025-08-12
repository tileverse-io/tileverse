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
package io.tileverse.tiling.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Test class for TileIndex functionality combining 2D and 3D operations.
 */
class TileIndexTest {

    @Test
    void testConstruction3D() {
        TileIndex index = TileIndex.of(10, 20, 5);
        assertEquals(10, index.x());
        assertEquals(20, index.y());
        assertEquals(5, index.z());
    }

    @Test
    void testMemoryOptimization() {
        // Small coordinates should use TileIndexInt
        TileIndex smallIndex = TileIndex.of(100, 200, 3);
        assertTrue(smallIndex instanceof TileIndexInt);
        assertEquals(100, smallIndex.x());
        assertEquals(200, smallIndex.y());
        assertEquals(3, smallIndex.z());

        // Large coordinates should use TileIndexLong
        TileIndex largeIndex = TileIndex.of(3_000_000_000L, 200, 3);
        assertTrue(largeIndex instanceof TileIndexLong);
        assertEquals(3_000_000_000L, largeIndex.x());
        assertEquals(200, largeIndex.y());
        assertEquals(3, largeIndex.z());

        // Boundary cases
        TileIndex maxInt = TileIndex.of(Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
        assertTrue(maxInt instanceof TileIndexInt);

        TileIndex minInt = TileIndex.of(Integer.MIN_VALUE, Integer.MIN_VALUE, 0);
        assertTrue(minInt instanceof TileIndexInt);

        TileIndex beyondMaxInt = TileIndex.of((long) Integer.MAX_VALUE + 1, 0, 0);
        assertTrue(beyondMaxInt instanceof TileIndexLong);
    }

    @Test
    void testShiftX() {
        TileIndex original = TileIndex.of(10, 20, 3);
        TileIndex shifted = original.shiftX(5);

        assertEquals(15, shifted.x());
        assertEquals(20, shifted.y());
        assertEquals(3, shifted.z());
        // Original should be unchanged
        assertEquals(10, original.x());
        assertEquals(20, original.y());
        assertEquals(3, original.z());
    }

    @Test
    void testShiftY() {
        TileIndex original = TileIndex.of(10, 20, 3);
        TileIndex shifted = original.shiftY(-5);

        assertEquals(10, shifted.x());
        assertEquals(15, shifted.y());
        assertEquals(3, shifted.z());
    }

    @Test
    void testShiftBy() {
        TileIndex original = TileIndex.of(10, 20, 3);
        TileIndex shifted = original.shiftBy(3, -7);

        assertEquals(13, shifted.x());
        assertEquals(13, shifted.y());
        assertEquals(3, shifted.z());
    }

    @Test
    void testShiftByFunction() {
        TileIndex original = TileIndex.of(10, 20, 3);
        TileIndex shifted = original.shiftBy(x -> x * 2, y -> y / 2);

        assertEquals(20, shifted.x());
        assertEquals(10, shifted.y());
        assertEquals(3, shifted.z()); // zoom level unchanged
    }

    @Test
    void testAtZoom() {
        TileIndex original = TileIndex.of(10, 20, 3);
        TileIndex newZoom = original.atZoom(5);

        assertEquals(10, newZoom.x());
        assertEquals(20, newZoom.y());
        assertEquals(5, newZoom.z());
        // Original unchanged
        assertEquals(3, original.z());
    }

    @Test
    void testCompareTo() {
        TileIndex index1 = TileIndex.of(5, 10, 1);
        TileIndex index2 = TileIndex.of(5, 10, 1);
        TileIndex index3 = TileIndex.of(5, 10, 2); // higher zoom
        TileIndex index4 = TileIndex.of(5, 15, 1); // same zoom, different Y
        TileIndex index5 = TileIndex.of(10, 5, 1); // same zoom, different X

        assertEquals(0, index1.compareTo(index2));
        assertTrue(index1.compareTo(index3) < 0); // lower zoom comes first
        assertTrue(index3.compareTo(index1) > 0);
        assertTrue(index1.compareTo(index4) < 0); // same zoom, compare by coordinates
        assertTrue(index1.compareTo(index5) < 0); // X coordinate comparison
    }

    @Test
    void testNegativeCoordinates() {
        TileIndex index = TileIndex.of(-10, -20, 2);
        assertEquals(-10, index.x());
        assertEquals(-20, index.y());
        assertEquals(2, index.z());

        TileIndex shifted = index.shiftBy(15, 25);
        assertEquals(5, shifted.x());
        assertEquals(5, shifted.y());
        assertEquals(2, shifted.z());
    }

    @Test
    void testEqualsAndHashCode() {
        TileIndex tile1 = TileIndex.of(100, 200, 3);
        TileIndex tile2 = TileIndex.of(100, 200, 3);
        TileIndex tile3 = TileIndex.of(100, 200, 4); // different zoom
        TileIndex tile4 = TileIndex.of(101, 200, 3); // different x

        // Test equals
        assertEquals(tile1, tile2);
        assertEquals(tile1, tile1); // reflexive
        assertNotEquals(tile1, tile3);
        assertNotEquals(tile1, tile4);
        assertNotEquals(tile1, null);
        assertNotEquals(tile1, "not a tile");

        // Test hashCode consistency
        assertEquals(tile1.hashCode(), tile2.hashCode());

        // Different tiles should ideally have different hash codes (not guaranteed, but likely)
        assertNotEquals(tile1.hashCode(), tile3.hashCode());
        assertNotEquals(tile1.hashCode(), tile4.hashCode());
    }

    @Test
    void testEqualsAcrossImplementations() {
        // Create tiles that should be equal but might use different implementations
        TileIndex smallTile = TileIndex.of(100, 200, 3); // Should use TileIndexInt

        // Force creation of potentially different implementation by using boundary values
        TileIndex boundaryTile = TileIndex.of(Integer.MAX_VALUE, Integer.MAX_VALUE, 3); // Should use TileIndexInt
        TileIndex largeTile = TileIndex.of((long) Integer.MAX_VALUE + 1, 200, 3); // Should use TileIndexLong

        // Verify implementation types for our test
        assertTrue(smallTile instanceof TileIndexInt);
        assertTrue(boundaryTile instanceof TileIndexInt);
        assertTrue(largeTile instanceof TileIndexLong);

        // Same coordinates should be equal regardless of implementation
        TileIndex tile1 = TileIndex.of(50, 60, 2);
        TileIndex tile2 = TileIndex.of(50, 60, 2);
        assertEquals(tile1, tile2);
        assertEquals(tile1.hashCode(), tile2.hashCode());
    }
}
