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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Test class for TilePyramid functionality.
 */
class TilePyramidTest {

    @Test
    void testEmptyPyramid() {
        TilePyramid empty = TilePyramid.empty();
        assertTrue(empty.isEmpty());
        assertEquals(0, empty.count());
        assertTrue(empty.levels().isEmpty());
    }

    @Test
    void testSingleRangePyramid() {
        TileRange range = TileRange.of(0, 0, 3, 3, 0);
        TilePyramid pyramid = TilePyramid.of(range);

        assertFalse(pyramid.isEmpty());
        assertEquals(16, pyramid.count()); // 4x4 = 16 tiles
        assertEquals(0, pyramid.minZoomLevel());
        assertEquals(0, pyramid.maxZoomLevel());
        assertEquals(1, pyramid.levels().size());
        assertTrue(pyramid.level(0).isPresent());
        assertFalse(pyramid.level(1).isPresent());
    }

    @Test
    void testMultiLevelPyramid() {
        SortedSet<TileRange> levels = new TreeSet<>();
        levels.add(TileRange.of(0, 0, 1, 1, 0)); // zoom 0: 2x2 = 4 tiles
        levels.add(TileRange.of(0, 0, 3, 3, 1)); // zoom 1: 4x4 = 16 tiles
        levels.add(TileRange.of(0, 0, 7, 7, 2)); // zoom 2: 8x8 = 64 tiles

        TilePyramid pyramid = TilePyramid.of(levels);

        assertFalse(pyramid.isEmpty());
        assertEquals(84, pyramid.count()); // 4 + 16 + 64 = 84
        assertEquals(0, pyramid.minZoomLevel());
        assertEquals(2, pyramid.maxZoomLevel());
        assertEquals(3, pyramid.levels().size());

        assertTrue(pyramid.level(0).isPresent());
        assertTrue(pyramid.level(1).isPresent());
        assertTrue(pyramid.level(2).isPresent());
        assertFalse(pyramid.level(3).isPresent());
    }

    @Test
    void testLevelsOrder() {
        List<TileRange> levels = List.of(
                // Add in reverse order to test sorting
                TileRange.of(0L, 0L, 7L, 7L, 2), TileRange.of(0L, 0L, 1L, 1L, 0), TileRange.of(0L, 0L, 3L, 3L, 1));

        TilePyramid pyramid = TilePyramid.of(levels);

        List<Integer> actualZoomLevels =
                pyramid.levels().stream().map(TileRange::zoomLevel).toList();

        assertThat(actualZoomLevels).isEqualTo(List.of(0, 1, 2));
    }

    @Test
    void testSubset() {
        SortedSet<TileRange> levels = new TreeSet<>();
        IntStream.range(0, 10).forEach(z -> levels.add(TileRange.of(0, 0, (1L << z) - 1, (1L << z) - 1, z)));

        TilePyramid fullPyramid = TilePyramid.of(levels);
        assertEquals(0, fullPyramid.minZoomLevel());
        assertEquals(9, fullPyramid.maxZoomLevel());

        // Test fromLevel
        TilePyramid fromLevel3 = fullPyramid.fromLevel(3);
        assertEquals(3, fromLevel3.minZoomLevel());
        assertEquals(9, fromLevel3.maxZoomLevel());
        assertEquals(7, fromLevel3.levels().size());

        // Test toLevel
        TilePyramid toLevel6 = fullPyramid.toLevel(6);
        assertEquals(0, toLevel6.minZoomLevel());
        assertEquals(6, toLevel6.maxZoomLevel());
        assertEquals(7, toLevel6.levels().size());

        // Test subset
        TilePyramid subset = fullPyramid.subset(2, 5);
        assertEquals(2, subset.minZoomLevel());
        assertEquals(5, subset.maxZoomLevel());
        assertEquals(4, subset.levels().size());
    }

    @Test
    void testSubsetInvalidArguments() {
        TilePyramid pyramid = TilePyramid.of(TileRange.of(0, 0, 1, 1, 0));

        assertThrows(IllegalArgumentException.class, () -> pyramid.subset(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> pyramid.subset(1, 0));
    }

    @Test
    void testAsTiles() {
        List<TileRange> levels = List.of(
                TileRange.of(0, 0, 1, 1, 0), // 4 tiles
                TileRange.of(0, 0, 1, 1, 1)); // 4 tiles

        TilePyramid pyramid = TilePyramid.of(levels);

        // Test that asTiles() returns a view with individual tiles
        TilePyramid asTiles = pyramid.asTiles();
        assertEquals(pyramid.count(), asTiles.count()); // Same tile count
        assertEquals(pyramid.levels().size(), asTiles.levels().size()); // Same number of levels

        // For regular TileRange instances, asTiles should return the same ranges
        for (int i = 0; i < pyramid.levels().size(); i++) {
            TileRange original = pyramid.levels().get(i);
            TileRange asTile = asTiles.levels().get(i);
            assertEquals(original, asTile); // Should be the same for regular TileRange
        }
    }

    @Test
    void testAsMetaTilesAndAsTilesSymmetry() {
        // Create a pyramid with meta-tiles
        MetaTileRange metaRange = MetaTileRange.of(0, 0, 2, 2, 1, 3, 3); // 3x3 meta-tiles
        TilePyramid metaPyramid = TilePyramid.of(metaRange);

        // Convert to individual tiles
        TilePyramid individualTiles = metaPyramid.asTiles();

        // Convert back to meta-tiles
        TilePyramid backToMeta = individualTiles.asMetaTiles(3, 3);

        // Should cover the same tile space (though bounds might be slightly different due to alignment)
        assertEquals(metaPyramid.count(), backToMeta.asTiles().count());

        // Test the symmetric transformation with regular tiles
        TileRange regularRange = TileRange.of(0, 0, 8, 8, 2); // 9x9 tiles, divisible by 3x3
        TilePyramid regularPyramid = TilePyramid.of(regularRange);

        TilePyramid asMetaTiles = regularPyramid.asMetaTiles(3, 3);
        TilePyramid backToTiles = asMetaTiles.asTiles();

        // For perfectly aligned ranges, should be identical
        assertEquals(regularPyramid.count(), backToTiles.count());
    }

    @Test
    void testAsMetaTiles() {
        TileRange range = TileRange.of(0, 0, 9, 9, 0); // 10x10 = 100 tiles at zoom 0
        TilePyramid pyramid = TilePyramid.of(range);

        testAsMetaTiles(pyramid, 1, 1);
        testAsMetaTiles(pyramid, 2, 2);
        testAsMetaTiles(pyramid, 3, 3);
        testAsMetaTiles(pyramid, 5, 5);

        IllegalArgumentException ex;
        ex = assertThrows(IllegalArgumentException.class, () -> pyramid.asMetaTiles(-1, 1));
        assertThat(ex.getMessage()).contains("width must be > 0");

        ex = assertThrows(IllegalArgumentException.class, () -> pyramid.asMetaTiles(1, -1));
        assertThat(ex.getMessage()).contains("height must be > 0");
    }

    private TilePyramid testAsMetaTiles(TilePyramid pyramid, int metaWidth, int metaHeight) {
        TilePyramid metaTiles = pyramid.asMetaTiles(metaWidth, metaHeight);

        // Check that metaTiles.count() returns the correct number of meta-tiles
        long metaTileCount = metaTiles.count();
        long expectedMetaTileCount = pyramid.countMetaTiles(metaWidth, metaHeight);
        assertEquals(expectedMetaTileCount, metaTileCount);

        // Check that metaTiles.asTiles().count() returns pyramid.count()
        long tilesFromMetaTiles = metaTiles.asTiles().count();
        assertEquals(pyramid.count(), tilesFromMetaTiles);

        return metaTiles;
    }

    @Test
    void testCountMetaTiles() {
        TileRange range = TileRange.of(0, 0, 9, 9, 0); // 10x10 = 100 tiles at zoom 0
        TilePyramid pyramid = TilePyramid.of(range);

        // 10x10 grid with 3x3 meta-tiles should give us 16 meta-tiles
        // (4x4 grid of meta-tiles to cover the entire 10x10 space)
        assertEquals(16, pyramid.countMetaTiles(3, 3));

        // 10x10 grid with 5x5 meta-tiles should give us 4 meta-tiles
        assertEquals(4, pyramid.countMetaTiles(5, 5));
    }
}
