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

import static io.tileverse.tiling.common.BoundingBox2D.extent;
import static io.tileverse.tiling.common.CornerOfOrigin.TOP_LEFT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TileMatrixTest {

    @Test
    void testTileMatrixCreation() {
        TileRange range = TileRange.of(0, 0, 3, 3, 5, TOP_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156.543)
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
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
    void testCoordinateToTile() {
        TileRange range = TileRange.of(0, 0, 1, 1, 0, TOP_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156543.03)
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
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
        TileRange range = TileRange.of(0, 0, 3, 3, 2, TOP_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(39135.76)
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        // Test extent that covers part of the matrix
        BoundingBox2D testExtent = extent(-10000000, -10000000, 10000000, 10000000);
        TileRange resultRange = matrix.extentToRange(testExtent).orElseThrow();

        assertNotNull(resultRange);
        assertEquals(2, resultRange.zoomLevel());
        assertTrue(resultRange.count() > 0);
        assertTrue(range.intersection(resultRange).orElseThrow().count() > 0); // Should intersect with matrix
    }

    @Test
    void testTiles() {
        TileRange range = TileRange.of(0, 0, 3, 3, 2, TOP_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(1)
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        assertThat(matrix.tileCount()).isEqualTo(16);
        assertThat(matrix.tiles()).hasSize(16);
    }

    @Test
    void testContains() {
        TileRange range = TileRange.of(5, 5, 10, 10, 8, TOP_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(156.543)
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        assertTrue(matrix.contains(TileIndex.xyz(5, 5, 8)));
        assertTrue(matrix.contains(TileIndex.xyz(10, 10, 8)));
        assertTrue(matrix.contains(TileIndex.xyz(7, 7, 8)));

        assertFalse(matrix.contains(TileIndex.xyz(4, 5, 8))); // Outside X range
        assertFalse(matrix.contains(TileIndex.xyz(5, 4, 8))); // Outside Y range
        assertFalse(matrix.contains(TileIndex.xyz(5, 5, 9))); // Wrong zoom level
    }

    @Test
    void testWithTileRange() {
        TileRange originalRange = TileRange.of(0, 0, 10, 10, 5, TOP_LEFT);
        TileMatrix original = TileMatrix.builder()
                .tileRange(originalRange)
                .resolution(156.543)
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        TileRange newRange = TileRange.of(2, 2, 8, 8, 5, TOP_LEFT);
        TileMatrix modified = original.withTileRange(newRange);

        assertEquals(newRange, modified.tileRange());
        assertEquals(original.resolution(), modified.resolution());
        assertEquals(original.pointOfOrigin(), modified.pointOfOrigin());
        assertEquals(original.crsId(), modified.crsId());
        assertNotEquals(original.boundingBox(), modified.boundingBox()); // Extent should be recalculated
    }

    @Test
    void testIntersection() {
        TileRange range = TileRange.of(0, 0, 7, 7, 3, TOP_LEFT);
        TileMatrix matrix = TileMatrix.builder()
                .tileRange(range)
                .resolution(9783.94) // Zoom level 3
                .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                .crs("EPSG:3857")
                .tileSize(256, 256)
                .build();

        // Test intersection with a smaller extent
        BoundingBox2D smallExtent = extent(-10000000, -10000000, 10000000, 10000000);
        TileMatrix intersected = matrix.intersection(smallExtent).orElseThrow();

        assertNotNull(intersected);
        assertEquals(3, intersected.zoomLevel());
        assertTrue(intersected.tileRange().count() > 0);
        assertTrue(intersected.tileRange().count() < matrix.tileRange().count());

        // Test intersection with non-intersecting extent
        BoundingBox2D noIntersection = extent(30000000, 30000000, 40000000, 40000000);
        Optional<TileMatrix> empty = matrix.intersection(noIntersection);
        assertThat(empty).isEmpty();

        // Test full intersection
        BoundingBox2D fullExtent = extent(-25000000, -25000000, 25000000, 25000000);
        TileMatrix full = matrix.intersection(fullExtent).orElseThrow();
        assertEquals(matrix.tileRange().count(), full.tileRange().count());
    }

    @Test
    void testValidation() {
        assertThrows(IllegalArgumentException.class, () -> {
            TileMatrix.builder()
                    .tileRange(null) // Null range
                    .resolution(156.543)
                    .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                    .crs("EPSG:3857")
                    .build();
        });

        assertThrows(IllegalArgumentException.class, () -> {
            TileMatrix.builder()
                    .tileRange(TileRange.of(0, 0, 1, 1, 0))
                    .resolution(0) // Invalid resolution
                    .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                    .crs("EPSG:3857")
                    .build();
        });

        assertThrows(IllegalArgumentException.class, () -> {
            TileMatrix.builder()
                    .tileRange(TileRange.of(0, 0, 1, 1, 0))
                    .resolution(156.543)
                    .pointOfOrigin(DefaultTileMatrixSets.WebMercatorBounds.upperLeft())
                    .crs("") // Empty CRS
                    .build();
        });
    }
}
