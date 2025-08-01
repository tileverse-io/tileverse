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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
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
        TileIndex lowerLeft = TileIndex.of(10, 20, 7);
        TileIndex upperRight = TileIndex.of(30, 40, 7);
        TileRange range = TileRange.of(lowerLeft, upperRight);

        assertEquals(7, range.zoomLevel());
        assertEquals(10, range.minx());
        assertEquals(20, range.miny());
        assertEquals(30, range.maxx());
        assertEquals(40, range.maxy());
        assertEquals(lowerLeft, range.lowerLeft());
        assertEquals(upperRight, range.upperRight());
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
    void testMetaTileCounting() {
        TileRange range = TileRange.of(0, 0, 9, 9, 1); // 10x10 = 100 tiles

        // 10x10 grid with 3x3 meta-tiles should give us 16 meta-tiles
        // (4x4 grid of meta-tiles to cover the entire 10x10 space)
        assertEquals(16, range.countMetaTiles(3, 3));

        // 10x10 grid with 5x5 meta-tiles should give us 4 meta-tiles
        assertEquals(4, range.countMetaTiles(5, 5));

        // Single tile meta-tiles equals total tiles
        assertEquals(range.count(), range.countMetaTiles(1, 1));
    }

    @Test
    void testMetaTileCountingInvalidParameters() {
        TileRange range = TileRange.of(0, 0, 9, 9, 1);

        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(1, -1));
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(0, 1));
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(1, 0));
    }

    @Test
    void testTileRangeProperties() {
        TileRange range = TileRange.of(0, 0, 2, 1, 3); // 3x2 = 6 tiles at zoom 3

        assertEquals(6, range.count());
        assertEquals(3, range.spanX());
        assertEquals(2, range.spanY());
        assertEquals(3, range.zoomLevel());

        // Verify corner coordinates
        assertEquals(TileIndex.of(0, 0, 3), range.lowerLeft());
        assertEquals(TileIndex.of(2, 1, 3), range.upperRight());
    }

    @Test
    void testMetaTileConversion() {
        TileRange range = TileRange.of(0, 0, 7, 7, 1); // 8x8 tiles at zoom 1

        // Test meta-tile counting functionality
        long metaTileCount = range.countMetaTiles(3, 3);

        // 8x8 grid with 3x3 meta-tiles should give us 9 meta-tiles
        // (3x3 grid of meta-tiles to cover the entire 8x8 space)
        assertEquals(9, metaTileCount);

        // Verify that meta-tile functionality works correctly
        assertEquals(4, range.countMetaTiles(4, 4));
        assertEquals(range.count(), range.countMetaTiles(1, 1));
    }

    @Test
    void testMetaTileCountingInvalidParametersAdditional() {
        TileRange range = TileRange.of(0, 0, 9, 9, 1);

        // Additional validation for countMetaTiles method
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(1, -1));
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(0, 1));
        assertThrows(IllegalArgumentException.class, () -> range.countMetaTiles(1, 0));
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
        assertEquals(TileIndex.of(10, 20, 5), range.lowerLeft());
        assertEquals(TileIndex.of(30, 40, 5), range.upperRight());
    }

    @Test
    void testSingleTileRange() {
        TileRange range = TileRange.of(5, 10, 5, 10, 3); // Single tile

        assertEquals(1, range.spanX());
        assertEquals(1, range.spanY());
        assertEquals(1, range.count());

        // Verify corner coordinates for single tile
        assertEquals(TileIndex.of(5, 10, 3), range.lowerLeft());
        assertEquals(TileIndex.of(5, 10, 3), range.upperRight());
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
    void testDifferentImplementationsEquality() {
        // Create a regular TileRange and a MetaTileRange with same coordinates but different tile sizes
        TileRange regularRange = TileRange.of(0, 0, 1, 1, 5);
        MetaTileRange metaRange2x2 = MetaTileRange.of(0, 0, 1, 1, 5, 2, 2);
        MetaTileRange metaRange1x1 = MetaTileRange.of(0, 0, 1, 1, 5, 1, 1);

        // Regular range and 2x2 meta-tile should NOT be equal (different tile spaces)
        assertNotEquals(regularRange, metaRange2x2);
        assertNotEquals(metaRange2x2, regularRange);
        assertNotEquals(regularRange.hashCode(), metaRange2x2.hashCode());
        assertFalse(TileRange.equals(regularRange, metaRange2x2));

        // Regular range and 1x1 meta-tile SHOULD be equal (same tile space)
        assertEquals(regularRange, metaRange1x1);
        assertEquals(metaRange1x1, regularRange);
        assertEquals(regularRange.hashCode(), metaRange1x1.hashCode());
        assertTrue(TileRange.equals(regularRange, metaRange1x1));

        // Two instances of the same type with same coords should be equal
        MetaTileRange metaRange2x2_copy = MetaTileRange.of(0, 0, 1, 1, 5, 2, 2);
        assertEquals(metaRange2x2, metaRange2x2_copy);
        assertEquals(metaRange2x2.hashCode(), metaRange2x2_copy.hashCode());

        // 1x1 and 2x2 meta-tiles with same coords should NOT be equal
        assertNotEquals(metaRange1x1, metaRange2x2);
        assertNotEquals(metaRange1x1.hashCode(), metaRange2x2.hashCode());
        assertFalse(TileRange.equals(metaRange1x1, metaRange2x2));

        // Two 1x1 meta-tiles with same coords should be equal
        MetaTileRange metaRange1x1_copy = MetaTileRange.of(0, 0, 1, 1, 5, 1, 1);
        assertEquals(metaRange1x1, metaRange1x1_copy);
        assertEquals(metaRange1x1.hashCode(), metaRange1x1_copy.hashCode());
        assertTrue(TileRange.equals(metaRange1x1, metaRange1x1_copy));
    }

    @Test
    void testAsMetaTiles() {
        TileRange range = TileRange.of(0, 0, 7, 7, 5); // 8x8 tiles

        // Convert to 2x2 meta-tiles
        TileRange metaRange = range.asMetaTiles(2, 2);
        assertEquals(5, metaRange.zoomLevel());
        assertEquals(0, metaRange.minx()); // 0/2 = 0
        assertEquals(0, metaRange.miny()); // 0/2 = 0
        assertEquals(3, metaRange.maxx()); // 7/2 = 3
        assertEquals(3, metaRange.maxy()); // 7/2 = 3

        // Verify it's actually a MetaTileRange implementation (implementation detail)
        assertTrue(metaRange instanceof MetaTileRange);
        MetaTileRange actualMeta = (MetaTileRange) metaRange;
        assertEquals(2, actualMeta.tilesWide());
        assertEquals(2, actualMeta.tilesHigh());

        // Test invalid parameters
        assertThrows(IllegalArgumentException.class, () -> range.asMetaTiles(-1, 2));
        assertThrows(IllegalArgumentException.class, () -> range.asMetaTiles(2, -1));
        assertThrows(IllegalArgumentException.class, () -> range.asMetaTiles(0, 2));
        assertThrows(IllegalArgumentException.class, () -> range.asMetaTiles(2, 0));
    }

    @Test
    void testAsTiles() {
        // Test regular TileRange - should return itself
        TileRange regularRange = TileRange.of(5, 10, 15, 20, 3);
        TileRange asIndividual = regularRange.asTiles();
        assertEquals(regularRange, asIndividual);
        assertTrue(regularRange == asIndividual); // Same instance

        // Test MetaTileRange conversion
        MetaTileRange metaRange = MetaTileRange.of(1, 2, 2, 3, 4, 3, 2);
        TileRange converted = metaRange.asTiles();

        // Meta-tile (1,2) with 3x2 tiles starts at individual tile (3,4)
        // Meta-tile (2,3) with 3x2 tiles ends at individual tile (8,7)
        assertEquals(3, converted.minx()); // 1 * 3 = 3
        assertEquals(4, converted.miny()); // 2 * 2 = 4
        assertEquals(8, converted.maxx()); // (2+1) * 3 - 1 = 8
        assertEquals(7, converted.maxy()); // (3+1) * 2 - 1 = 7
        assertEquals(4, converted.zoomLevel());
    }

    @Test
    void testSymmetricTransformations() {
        TileRange original = TileRange.of(0, 0, 11, 11, 2); // 12x12 tiles

        // Convert to meta-tiles and back
        TileRange metaRange = original.asMetaTiles(3, 3);
        TileRange backToTiles = metaRange.asTiles();

        // Should cover the same or larger area (due to meta-tile boundary alignment)
        assertTrue(backToTiles.minx() <= original.minx());
        assertTrue(backToTiles.miny() <= original.miny());
        assertTrue(backToTiles.maxx() >= original.maxx());
        assertTrue(backToTiles.maxy() >= original.maxy());
        assertEquals(original.zoomLevel(), backToTiles.zoomLevel());

        // For perfectly aligned ranges, should be identical
        TileRange aligned = TileRange.of(0, 0, 8, 8, 2); // 9x9 tiles, divisible by 3x3
        TileRange alignedMeta = aligned.asMetaTiles(3, 3);
        TileRange alignedBack = alignedMeta.asTiles();
        assertEquals(aligned, alignedBack);
    }

    @Test
    void testMetaTileIntrospection() {
        // Regular TileRange
        TileRange regularRange = TileRange.of(0, 0, 7, 7, 2);
        assertFalse(regularRange.isMetaTiled());
        assertEquals(1, regularRange.metaWidth());
        assertEquals(1, regularRange.metaHeight());

        // MetaTileRange
        TileRange metaRange = regularRange.asMetaTiles(2, 3);
        assertTrue(metaRange.isMetaTiled());
        assertEquals(2, metaRange.metaWidth());
        assertEquals(3, metaRange.metaHeight());

        // Client can unwrap using asTiles()
        assertEquals(regularRange, metaRange.asTiles());
    }

    @Test
    void testHierarchicalMetaTiling() {
        // Start with a base tile range
        TileRange baseTiles = TileRange.of(0, 0, 23, 23, 3); // 24x24 tiles

        // Create first level of meta-tiles (4x4)
        TileRange level1Meta = baseTiles.asMetaTiles(4, 4);
        assertTrue(level1Meta.isMetaTiled());
        assertEquals(4, level1Meta.metaWidth());
        assertEquals(4, level1Meta.metaHeight());
        assertEquals(6, level1Meta.spanX()); // 24/4 = 6 meta-tiles
        assertEquals(6, level1Meta.spanY()); // 24/4 = 6 meta-tiles

        // Create second level of meta-tiles (2x2 individual tiles)
        TileRange level2Meta = level1Meta.asMetaTiles(2, 2);
        assertTrue(level2Meta.isMetaTiled());
        assertEquals(2, level2Meta.metaWidth()); // 2x2 individual tiles
        assertEquals(2, level2Meta.metaHeight());
        assertEquals(12, level2Meta.spanX()); // 24/2 = 12 meta-tiles
        assertEquals(12, level2Meta.spanY()); // 24/2 = 12 meta-tiles

        // Create third level of meta-tiles (3x3 individual tiles)
        TileRange level3Meta = level2Meta.asMetaTiles(3, 3);
        assertTrue(level3Meta.isMetaTiled());
        assertEquals(3, level3Meta.metaWidth());
        assertEquals(3, level3Meta.metaHeight());
        assertEquals(8, level3Meta.spanX()); // 24/3 = 8 meta-tiles
        assertEquals(8, level3Meta.spanY()); // 24/3 = 8 meta-tiles

        // Verify asTiles() works at all levels (gets back to base level)
        assertEquals(baseTiles, level1Meta.asTiles());
        assertEquals(baseTiles, level2Meta.asTiles());
        assertEquals(baseTiles, level3Meta.asTiles());

        // All views share the same source - they are different interpretations of the same base
        assertTrue(level1Meta instanceof MetaTileRange);
        assertTrue(level2Meta instanceof MetaTileRange);
        assertTrue(level3Meta instanceof MetaTileRange);
        assertEquals(baseTiles, ((MetaTileRange) level1Meta).getSource());
        assertEquals(baseTiles, ((MetaTileRange) level2Meta).getSource());
        assertEquals(baseTiles, ((MetaTileRange) level3Meta).getSource());
    }

    @Test
    void testSuperTiledPyramid() {
        // Create a pyramid and apply multiple levels of meta-tiling
        TileRange baseRange = TileRange.of(0, 0, 31, 31, 5); // 32x32 = 1024 base tiles
        TilePyramid basePyramid = TilePyramid.of(baseRange);

        // Create multiple levels of meta-tile views
        TilePyramid meta2x2 = basePyramid.asMetaTiles(2, 2); // 16x16 meta-tiles
        TilePyramid meta4x4 = meta2x2.asMetaTiles(2, 2); // 8x8 super-meta-tiles
        TilePyramid meta8x8 = meta4x4.asMetaTiles(2, 2); // 4x4 super-super-meta-tiles

        // Verify tile counts are preserved
        assertEquals(basePyramid.count(), meta2x2.asTiles().count());
        assertEquals(basePyramid.count(), meta4x4.asTiles().count());
        assertEquals(basePyramid.count(), meta8x8.asTiles().count());

        // Verify meta-tile counts (each view creates different meta-tile dimensions)
        assertEquals(256, meta2x2.count()); // 32/2=16, so 16x16 = 256 meta-tiles
        assertEquals(256, meta4x4.count()); // Still 2x2 meta-tiles of base, so 16x16 = 256
        assertEquals(256, meta8x8.count()); // Still 2x2 meta-tiles of base, so 16x16 = 256

        // Verify client can inspect and unwrap meta-tile properties
        TileRange level0Range = meta8x8.levels().get(0);
        assertTrue(level0Range.isMetaTiled());
        assertEquals(2, level0Range.metaWidth());
        assertEquals(2, level0Range.metaHeight());
        assertEquals(basePyramid.levels().get(0), level0Range.asTiles());
    }

    @Test
    void testSubsetViewPreservesZoomLevelsAfterAsTiles() {
        // Create a multi-level pyramid
        List<TileRange> levels = List.of(
                TileRange.of(0, 0, 7, 7, 0), // zoom 0: 8x8 = 64 tiles
                TileRange.of(0, 0, 15, 15, 1), // zoom 1: 16x16 = 256 tiles
                TileRange.of(0, 0, 31, 31, 2), // zoom 2: 32x32 = 1024 tiles
                TileRange.of(0, 0, 63, 63, 3), // zoom 3: 64x64 = 4096 tiles
                TileRange.of(0, 0, 127, 127, 4) // zoom 4: 128x128 = 16384 tiles
                );
        TilePyramid fullPyramid = TilePyramid.of(levels);

        // Create a meta-tile view
        TilePyramid metaPyramid = fullPyramid.asMetaTiles(4, 4);

        // Create a subset of the meta-tile pyramid (levels 1-3)
        TilePyramid metaSubset = metaPyramid.subset(1, 3);
        assertEquals(3, metaSubset.levels().size()); // Should have 3 levels
        assertEquals(1, metaSubset.minZoomLevel());
        assertEquals(3, metaSubset.maxZoomLevel());

        // Convert subset back to individual tiles - should preserve zoom level bounds
        TilePyramid tilesSubset = metaSubset.asTiles();
        assertEquals(3, tilesSubset.levels().size()); // Should still have 3 levels
        assertEquals(1, tilesSubset.minZoomLevel()); // Should still start at level 1
        assertEquals(3, tilesSubset.maxZoomLevel()); // Should still end at level 3

        // Verify the tile counts match the original levels 1-3
        TilePyramid originalSubset = fullPyramid.subset(1, 3);
        assertEquals(originalSubset.count(), tilesSubset.count());

        // Verify each level matches
        for (int z = 1; z <= 3; z++) {
            TileRange originalLevel = originalSubset.level(z).orElseThrow();
            TileRange tilesLevel = tilesSubset.level(z).orElseThrow();
            assertEquals(originalLevel, tilesLevel);
        }

        // Verify levels 0 and 4 are not present
        assertTrue(tilesSubset.level(0).isEmpty());
        assertTrue(tilesSubset.level(4).isEmpty());
    }

    @Test
    void testAxisOriginDefault() {
        // Default axis origin should be LOWER_LEFT
        TileRange range = TileRange.of(0, 0, 3, 3, 2);
        assertEquals(AxisOrigin.LOWER_LEFT, range.axisOrigin());
    }

    @Test
    void testAxisOriginExplicit() {
        // Explicit axis origin
        TileRange range = TileRange.of(0, 0, 3, 3, 2, AxisOrigin.UPPER_LEFT);
        assertEquals(AxisOrigin.UPPER_LEFT, range.axisOrigin());
    }

    @Test
    void testAxisOriginTransformation() {
        // Create a 4x4 tile range at zoom level 2 (2^2 = 4 tiles per side)
        TileRange lowerLeftRange = TileRange.of(0, 0, 3, 3, 2, AxisOrigin.LOWER_LEFT);

        // Convert to upper-left origin
        TileRange upperLeftRange = lowerLeftRange.withAxisOrigin(AxisOrigin.UPPER_LEFT);

        // In a 4x4 grid (zoom 2), coordinates should flip:
        // LOWER_LEFT (0,0) -> UPPER_LEFT (0,3)
        // LOWER_LEFT (3,3) -> UPPER_LEFT (3,0)
        assertEquals(AxisOrigin.UPPER_LEFT, upperLeftRange.axisOrigin());
        assertEquals(0, upperLeftRange.minx()); // X doesn't change
        assertEquals(0, upperLeftRange.miny()); // (2^2-1) - 3 = 0
        assertEquals(3, upperLeftRange.maxx()); // X doesn't change
        assertEquals(3, upperLeftRange.maxy()); // (2^2-1) - 0 = 3

        // Convert back should restore original coordinates
        TileRange backToLowerLeft = upperLeftRange.withAxisOrigin(AxisOrigin.LOWER_LEFT);
        assertEquals(lowerLeftRange.minx(), backToLowerLeft.minx());
        assertEquals(lowerLeftRange.miny(), backToLowerLeft.miny());
        assertEquals(lowerLeftRange.maxx(), backToLowerLeft.maxx());
        assertEquals(lowerLeftRange.maxy(), backToLowerLeft.maxy());
        assertEquals(AxisOrigin.LOWER_LEFT, backToLowerLeft.axisOrigin());
    }

    @Test
    void testAxisOriginNoTransformation() {
        // Converting to same origin should return same instance
        TileRange range = TileRange.of(0, 0, 3, 3, 2, AxisOrigin.LOWER_LEFT);
        TileRange sameOrigin = range.withAxisOrigin(AxisOrigin.LOWER_LEFT);

        assertTrue(range == sameOrigin); // Same instance
    }

    @Test
    void testTilePyramidAxisConsistency() {
        // Create ranges with different axis origins
        TileRange lowerLeftRange = TileRange.of(0, 0, 1, 1, 0, AxisOrigin.LOWER_LEFT);
        TileRange upperLeftRange = TileRange.of(0, 0, 1, 1, 1, AxisOrigin.UPPER_LEFT);

        // Builder should convert all ranges to pyramid's axis origin
        TilePyramid pyramid = TilePyramid.builder()
                .axisOrigin(AxisOrigin.UPPER_LEFT)
                .level(lowerLeftRange)
                .level(upperLeftRange)
                .build();

        // All levels should have UPPER_LEFT axis origin
        assertEquals(AxisOrigin.UPPER_LEFT, pyramid.axisOrigin());
        for (TileRange level : pyramid.levels()) {
            assertEquals(AxisOrigin.UPPER_LEFT, level.axisOrigin());
        }
    }

    @Test
    void testCornerMethodsLowerLeft() {
        // Test all four corner methods with LOWER_LEFT axis origin (default)
        TileRange range = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.LOWER_LEFT);

        // Lower-left corner (minx, miny) for LOWER_LEFT
        TileIndex lowerLeft = range.lowerLeft();
        assertEquals(10, lowerLeft.x());
        assertEquals(20, lowerLeft.y());
        assertEquals(5, lowerLeft.z());

        // Upper-right corner (maxx, maxy) for LOWER_LEFT
        TileIndex upperRight = range.upperRight();
        assertEquals(30, upperRight.x());
        assertEquals(40, upperRight.y());
        assertEquals(5, upperRight.z());

        // Lower-right corner (maxx, miny) for LOWER_LEFT
        TileIndex lowerRight = range.lowerRight();
        assertEquals(30, lowerRight.x());
        assertEquals(20, lowerRight.y());
        assertEquals(5, lowerRight.z());

        // Upper-left corner (minx, maxy) for LOWER_LEFT
        TileIndex upperLeft = range.upperLeft();
        assertEquals(10, upperLeft.x());
        assertEquals(40, upperLeft.y());
        assertEquals(5, upperLeft.z());
    }

    @Test
    void testCornerMethodsUpperLeft() {
        // Test corner methods with UPPER_LEFT axis origin (like PMTiles)
        TileRange range = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.UPPER_LEFT);

        // Lower-left corner: In UPPER_LEFT, "lower" means higher Y values
        // So lower-left = (minx, maxy)
        TileIndex lowerLeft = range.lowerLeft();
        assertEquals(10, lowerLeft.x());
        assertEquals(40, lowerLeft.y()); // maxy for UPPER_LEFT
        assertEquals(5, lowerLeft.z());

        // Upper-right corner: In UPPER_LEFT, "upper" means lower Y values
        // So upper-right = (maxx, miny)
        TileIndex upperRight = range.upperRight();
        assertEquals(30, upperRight.x());
        assertEquals(20, upperRight.y()); // miny for UPPER_LEFT
        assertEquals(5, upperRight.z());

        // Lower-right corner: (maxx, maxy) for UPPER_LEFT
        TileIndex lowerRight = range.lowerRight();
        assertEquals(30, lowerRight.x());
        assertEquals(40, lowerRight.y()); // maxy for UPPER_LEFT
        assertEquals(5, lowerRight.z());

        // Upper-left corner: (minx, miny) for UPPER_LEFT
        TileIndex upperLeft = range.upperLeft();
        assertEquals(10, upperLeft.x());
        assertEquals(20, upperLeft.y()); // miny for UPPER_LEFT
        assertEquals(5, upperLeft.z());
    }

    @Test
    void testFirstLastMethods() {
        // Test first() and last() methods for traversal
        TileRange lowerLeftRange = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.LOWER_LEFT);
        TileRange upperLeftRange = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.UPPER_LEFT);

        // first() should equal lowerLeft() for both axis origins
        assertEquals(lowerLeftRange.lowerLeft(), lowerLeftRange.first());
        assertEquals(upperLeftRange.lowerLeft(), upperLeftRange.first());

        // last() should equal upperRight() for both axis origins
        assertEquals(lowerLeftRange.upperRight(), lowerLeftRange.last());
        assertEquals(upperLeftRange.upperRight(), upperLeftRange.last());

        // Verify the actual coordinates for LOWER_LEFT
        TileIndex lowerLeftFirst = lowerLeftRange.first();
        assertEquals(10, lowerLeftFirst.x()); // minx, miny
        assertEquals(20, lowerLeftFirst.y());

        TileIndex lowerLeftLast = lowerLeftRange.last();
        assertEquals(30, lowerLeftLast.x()); // maxx, maxy
        assertEquals(40, lowerLeftLast.y());

        // Verify the actual coordinates for UPPER_LEFT
        TileIndex upperLeftFirst = upperLeftRange.first();
        assertEquals(10, upperLeftFirst.x()); // minx, maxy (visual lower-left)
        assertEquals(40, upperLeftFirst.y());

        TileIndex upperLeftLast = upperLeftRange.last();
        assertEquals(30, upperLeftLast.x()); // maxx, miny (visual upper-right)
        assertEquals(20, upperLeftLast.y());
    }

    @Test
    void testNextPrevMethods() {
        // Test with a small 2x2 range for easy verification
        TileRange range = TileRange.of(10, 20, 11, 21, 5, AxisOrigin.LOWER_LEFT);

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
        TileRange range = TileRange.of(10, 20, 11, 21, 5, AxisOrigin.UPPER_LEFT);

        // Expected traversal order for UPPER_LEFT (left-to-right, top-to-bottom):
        // Note: first() = lowerLeft() = (10,21) for UPPER_LEFT
        // last() = upperRight() = (11,20) for UPPER_LEFT
        // Traversal: (10,21) -> (11,21) -> (10,20) -> (11,20)

        TileIndex first = range.first(); // lowerLeft in UPPER_LEFT = (10,21)
        assertEquals(10, first.x());
        assertEquals(21, first.y());

        // Test complete traversal
        Optional<TileIndex> next1 = range.next(first);
        assertTrue(next1.isPresent());
        assertEquals(11, next1.get().x()); // (11,21)
        assertEquals(21, next1.get().y());

        Optional<TileIndex> next2 = range.next(next1.get());
        assertTrue(next2.isPresent());
        assertEquals(10, next2.get().x()); // (10,20) - wrap to next row
        assertEquals(20, next2.get().y());

        Optional<TileIndex> next3 = range.next(next2.get());
        assertTrue(next3.isPresent());
        assertEquals(11, next3.get().x()); // (11,20)
        assertEquals(20, next3.get().y());

        // Should be the last tile
        assertEquals(range.last(), next3.get());

        // Next from last should be empty
        assertTrue(range.next(next3.get()).isEmpty());
    }

    @Test
    void testNextPrevEdgeCases() {
        TileRange range = TileRange.of(10, 20, 11, 21, 5, AxisOrigin.LOWER_LEFT);

        // Test with null
        assertTrue(range.next(null).isEmpty());
        assertTrue(range.prev(null).isEmpty());

        // Test with wrong zoom level
        TileIndex wrongZoom = TileIndex.of(10, 20, 6);
        assertTrue(range.next(wrongZoom).isEmpty());
        assertTrue(range.prev(wrongZoom).isEmpty());

        // Test with tile outside range
        TileIndex outsideRange = TileIndex.of(5, 20, 5);
        assertTrue(range.next(outsideRange).isEmpty());
        assertTrue(range.prev(outsideRange).isEmpty());

        // Test with tile outside Y range
        TileIndex outsideY = TileIndex.of(10, 25, 5);
        assertTrue(range.next(outsideY).isEmpty());
        assertTrue(range.prev(outsideY).isEmpty());
    }

    @Test
    void testIntersection() {
        // Test overlapping ranges
        TileRange range1 = TileRange.of(5, 10, 15, 20, 8, AxisOrigin.UPPER_LEFT);
        TileRange range2 = TileRange.of(10, 15, 25, 30, 8, AxisOrigin.UPPER_LEFT);

        TileRange intersection = range1.intersection(range2).orElseThrow();

        assertEquals(8, intersection.zoomLevel());
        assertEquals(10, intersection.minx()); // max(5, 10)
        assertEquals(15, intersection.miny()); // max(10, 15)
        assertEquals(15, intersection.maxx()); // min(15, 25)
        assertEquals(20, intersection.maxy()); // min(20, 30)
        assertEquals(AxisOrigin.UPPER_LEFT, intersection.axisOrigin());

        // Test non-overlapping ranges
        TileRange range3 = TileRange.of(0, 0, 5, 5, 8, AxisOrigin.UPPER_LEFT);
        TileRange range4 = TileRange.of(10, 10, 15, 15, 8, AxisOrigin.UPPER_LEFT);

        assertThat(range3.intersection(range4)).isEmpty();

        // Test identical ranges
        TileRange range5 = TileRange.of(10, 20, 30, 40, 5, AxisOrigin.LOWER_LEFT);
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
        TileRange range1 = TileRange.of(0, 0, 10, 10, 5, AxisOrigin.UPPER_LEFT);
        TileRange range2 = TileRange.of(5, 5, 15, 15, 6, AxisOrigin.UPPER_LEFT); // Different zoom level

        assertThrows(IllegalArgumentException.class, () -> {
            range1.intersection(range2);
        });
    }
}
