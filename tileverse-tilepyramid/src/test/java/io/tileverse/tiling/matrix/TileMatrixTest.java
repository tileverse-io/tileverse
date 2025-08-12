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
package io.tileverse.tiling.matrix;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.pyramid.AxisOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TileMatrixTest {

    @Test
    void testTileMatrixCreation() {
        TileRange range = TileRange.of(0, 0, 3, 3, 5, AxisOrigin.UPPER_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156.543)
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        assertThat(matrix)
                .isNotNull()
                .hasFieldOrPropertyWithValue("zoomLevel", 5)
                .hasFieldOrPropertyWithValue("resolution", 156.543)
                .hasFieldOrPropertyWithValue("crsId", "EPSG:3857")
                .hasFieldOrPropertyWithValue("resolution", 156.543)
                .hasFieldOrPropertyWithValue("tileWidth", 256)
                .hasFieldOrPropertyWithValue("tileHeight", 256);

        assertThat(matrix.tileRange()).isEqualTo(range);
    }

    @Test
    void testTileExtent() {
        TileRange range = TileRange.of(0, 0, 1, 1, 0, AxisOrigin.UPPER_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156543.03)
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        Tile tile = matrix.tile(TileIndex.of(0, 0, 0)).orElseThrow();
        Extent tileExtent = tile.extent();

        assertNotNull(tileExtent);
        // For UPPER_LEFT origin at zoom 0, tile (0,0) should cover the full world extent
        // At zoom 0, there's only 1 tile covering the entire world
        assertEquals(-20037508.34, tileExtent.minX(), 1.0);
        assertEquals(-20037508.34, tileExtent.minY(), 1.0);
        assertEquals(20037508.34, tileExtent.maxX(), 1.0);
        assertEquals(20037508.34, tileExtent.maxY(), 1.0);
    }

    @Test
    void testCoordinateToTile() {
        TileRange range = TileRange.of(0, 0, 1, 1, 0, AxisOrigin.UPPER_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156543.03)
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        // Test coordinate in upper-left quadrant should map to tile (0,0)
        Coordinate coord = Coordinate.of(-10000000, 10000000);
        Optional<Tile> tileOpt = matrix.coordinateToTile(coord);

        assertTrue(tileOpt.isPresent());
        Tile tile = tileOpt.get();
        assertEquals(0, tile.x());
        assertEquals(0, tile.y());
        assertEquals(0, tile.z());
    }

    @Test
    void testExtentToRange() {
        TileRange range = TileRange.of(0, 0, 3, 3, 2, AxisOrigin.UPPER_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(39135.76)
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        // Test extent that covers part of the matrix
        Extent testExtent = Extent.of(-10000000, -10000000, 10000000, 10000000);
        TileRange resultRange = matrix.extentToRange(testExtent).orElseThrow();

        assertNotNull(resultRange);
        assertEquals(2, resultRange.zoomLevel());
        assertTrue(resultRange.count() > 0);
        assertTrue(range.intersection(resultRange).orElseThrow().count() > 0); // Should intersect with matrix
    }

    @Test
    void testContains() {
        TileRange range = TileRange.of(5, 5, 10, 10, 8, AxisOrigin.UPPER_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156.543)
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        assertTrue(matrix.contains(TileIndex.of(5, 5, 8)));
        assertTrue(matrix.contains(TileIndex.of(10, 10, 8)));
        assertTrue(matrix.contains(TileIndex.of(7, 7, 8)));

        assertFalse(matrix.contains(TileIndex.of(4, 5, 8))); // Outside X range
        assertFalse(matrix.contains(TileIndex.of(5, 4, 8))); // Outside Y range
        assertFalse(matrix.contains(TileIndex.of(5, 5, 9))); // Wrong zoom level
    }

    @Test
    void testWithTileRange() {
        TileRange originalRange = TileRange.of(0, 0, 10, 10, 5, AxisOrigin.UPPER_LEFT);
        TileMatrix original = TileMatrix.builder()
                .tileRange(originalRange)
                .resolution(156.543)
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        TileRange newRange = TileRange.of(2, 2, 8, 8, 5, AxisOrigin.UPPER_LEFT);
        TileMatrix modified = original.withTileRange(newRange);

        assertEquals(newRange, modified.tileRange());
        assertEquals(original.resolution(), modified.resolution());
        assertEquals(original.origin(), modified.origin());
        assertEquals(original.crsId(), modified.crsId());
        assertNotEquals(original.extent(), modified.extent()); // Extent should be recalculated
    }

    @Test
    void testIntersection() {
        TileRange range = TileRange.of(0, 0, 7, 7, 3, AxisOrigin.UPPER_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(9783.94) // Zoom level 3
                .origin(Coordinate.of(-20037508.34, 20037508.34))
                .extent(Extent.of(-20037508.34, -20037508.34, 20037508.34, 20037508.34))
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        // Test intersection with a smaller extent
        Extent smallExtent = Extent.of(-10000000, -10000000, 10000000, 10000000);
        TileMatrix intersected = matrix.intersection(smallExtent).orElseThrow();

        assertNotNull(intersected);
        assertEquals(3, intersected.zoomLevel());
        assertTrue(intersected.tileRange().count() > 0);
        assertTrue(intersected.tileRange().count() < matrix.tileRange().count());

        // Test intersection with non-intersecting extent
        Extent noIntersection = Extent.of(30000000, 30000000, 40000000, 40000000);
        Optional<TileMatrix> empty = matrix.intersection(noIntersection);
        assertThat(empty).isEmpty();

        // Test full intersection
        Extent fullExtent = Extent.of(-25000000, -25000000, 25000000, 25000000);
        TileMatrix full = matrix.intersection(fullExtent).orElseThrow();
        assertEquals(matrix.tileRange().count(), full.tileRange().count());
    }

    @Test
    void testValidation() {
        assertThrows(IllegalArgumentException.class, () -> {
            TileMatrix.builder()
                    .tileRange(null) // Null range
                    .resolution(156.543)
                    .origin(Coordinate.of(0, 0))
                    .extent(Extent.of(0, 0, 1, 1))
                    .crs("EPSG:3857")
                    .build();
        });

        assertThrows(IllegalArgumentException.class, () -> {
            TileMatrix.builder()
                    .tileRange(TileRange.of(0, 0, 1, 1, 0))
                    .resolution(0) // Invalid resolution
                    .origin(Coordinate.of(0, 0))
                    .extent(Extent.of(0, 0, 1, 1))
                    .crs("EPSG:3857")
                    .build();
        });

        assertThrows(IllegalArgumentException.class, () -> {
            TileMatrix.builder()
                    .tileRange(TileRange.of(0, 0, 1, 1, 0))
                    .resolution(156.543)
                    .origin(Coordinate.of(0, 0))
                    .extent(Extent.of(0, 0, 1, 1))
                    .crs("") // Empty CRS
                    .build();
        });
    }
}
