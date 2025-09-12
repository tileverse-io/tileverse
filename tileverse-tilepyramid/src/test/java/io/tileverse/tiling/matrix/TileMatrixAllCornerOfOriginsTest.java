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
import static io.tileverse.tiling.common.CornerOfOrigin.BOTTOM_LEFT;
import static io.tileverse.tiling.common.CornerOfOrigin.TOP_LEFT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Comprehensive tests for TileMatrix functionality across all axis origins.
 * Uses simple, understandable spatial extents and tile ranges.
 */
class TileMatrixAllCornerOfOriginsTest {

    // Simple test parameters for easy understanding
    private static final BoundingBox2D SIMPLE_EXTENT = extent(-100, -100, 100, 100); // 200x200 world
    private static final double SIMPLE_RESOLUTION = 10.0; // 10 map units per pixel
    private static final int TILE_SIZE = 10; // 10x10 pixel tiles = 100x100 map units each

    /**
     * Test TileMatrix creation and basic properties for all axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testTileMatrixCreationAllOrigins(CornerOfOrigin origin) {
        TileRange range = TileRange.of(0, 0, 1, 1, 2, origin); // 2x2 tile grid at zoom 2
        TileMatrix matrix = createSimpleMatrix(range);

        assertThat(matrix)
                .isNotNull()
                .hasFieldOrPropertyWithValue("zoomLevel", 2)
                .hasFieldOrPropertyWithValue("resolution", SIMPLE_RESOLUTION)
                .hasFieldOrPropertyWithValue("tileWidth", TILE_SIZE)
                .hasFieldOrPropertyWithValue("tileHeight", TILE_SIZE);

        assertThat(matrix.tileRange()).isEqualTo(range);
        assertThat(matrix.tileRange().cornerOfOrigin()).isEqualTo(origin);
    }

    /**
     * Test tile extent calculation for different axis origins.
     * Each axis origin should map the same abstract tile coordinates to different geographic locations.
     */
    @Test
    void testTileExtentsByCornerOfOrigin() {
        // Test tile (0,0) at zoom 1 - should cover different geographic quadrants per axis origin
        int zoom = 1;
        TileIndex tile00 = TileIndex.xyz(0, 0, zoom);

        // UPPER_LEFT: (0,0) should be northwest quadrant
        TileMatrix upperLeftMatrix = createSimpleMatrix(TileRange.of(0, 0, 1, 1, zoom, TOP_LEFT));
        Tile upperLeftTile = upperLeftMatrix.tile(tile00).orElseThrow();
        BoundingBox2D upperLeftExtent = upperLeftTile.extent();

        // LOWER_LEFT: (0,0) should be southwest quadrant
        TileMatrix lowerLeftMatrix = createSimpleMatrix(TileRange.of(0, 0, 1, 1, zoom, BOTTOM_LEFT));
        Tile lowerLeftTile = lowerLeftMatrix.tile(tile00).orElseThrow();
        BoundingBox2D lowerLeftExtent = lowerLeftTile.extent();

        // Verify each quadrant
        // UPPER_LEFT: northwest quadrant (-100 to 0, 0 to 100)
        assertThat(upperLeftExtent.minX()).isEqualTo(-100.0);
        assertThat(upperLeftExtent.maxX()).isEqualTo(0.0);
        assertThat(upperLeftExtent.minY()).isEqualTo(0.0);
        assertThat(upperLeftExtent.maxY()).isEqualTo(100.0);

        // LOWER_LEFT: southwest quadrant (-100 to 0, -100 to 0)
        assertThat(lowerLeftExtent.minX()).isEqualTo(-100.0);
        assertThat(lowerLeftExtent.maxX()).isEqualTo(0.0);
        assertThat(lowerLeftExtent.minY()).isEqualTo(-100.0);
        assertThat(lowerLeftExtent.maxY()).isEqualTo(0.0);
    }

    /**
     * Test coordinate-to-tile mapping for different axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testCoordinateToTileAllOrigins(CornerOfOrigin origin) {
        TileRange range = TileRange.of(0, 0, 1, 1, 1, origin); // 2x2 grid at zoom 1
        TileMatrix matrix = createSimpleMatrix(range);

        // Test coordinate (50, 50) - should always be in positive quadrant
        Coordinate coord = Coordinate.of(50.0, 50.0);
        Optional<Tile> tileOpt = matrix.coordinateToTile(coord);

        assertTrue(tileOpt.isPresent());
        Tile tile = tileOpt.get();
        assertEquals(1, tile.z());

        // The tile coordinates should vary by axis origin
        switch (origin) {
            case TOP_LEFT -> {
                assertEquals(1, tile.x()); // East side
                assertEquals(0, tile.y()); // North side (low Y in UPPER_LEFT)
            }
            case BOTTOM_LEFT -> {
                assertEquals(1, tile.x()); // East side
                assertEquals(1, tile.y()); // North side (high Y in LOWER_LEFT)
            }
        }
    }

    /**
     * Test extent-to-range mapping for different axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testExtentToRangeAllOrigins(CornerOfOrigin origin) {
        TileRange matrixRange = TileRange.of(0, 0, 3, 3, 2, origin); // 4x4 grid
        TileMatrix matrix = createSimpleMatrix(matrixRange);

        // Query a subset extent: northeast quadrant (0 to 100, 0 to 100)
        BoundingBox2D queryExtent = extent(0, 0, 100, 100);
        Optional<TileRange> resultOpt = matrix.extentToRange(queryExtent);

        assertTrue(resultOpt.isPresent());
        TileRange result = resultOpt.get();

        assertEquals(2, result.zoomLevel());
        assertEquals(origin, result.cornerOfOrigin());
        assertTrue(result.count() > 0);

        // Should intersect with original matrix range
        assertTrue(matrixRange.intersection(result).isPresent());
    }

    /**
     * Test tile intersection with spatial extents for different axis origins.
     */
    @ParameterizedTest
    @EnumSource(CornerOfOrigin.class)
    void testTileIntersectionAllOrigins(CornerOfOrigin origin) {
        TileRange range = TileRange.of(0, 0, 1, 1, 1, origin);
        TileMatrix matrix = createSimpleMatrix(range);

        // Test intersection with northeast quadrant
        BoundingBox2D northeastQuadrant = extent(0, 0, 100, 100);
        Optional<TileMatrix> intersected = matrix.intersection(northeastQuadrant);

        assertTrue(intersected.isPresent());
        TileMatrix result = intersected.get();
        assertEquals(1, result.zoomLevel());
        assertTrue(result.tileRange().count() > 0);
    }

    /**
     * Create a simple TileMatrix for testing with understandable parameters.
     */
    private TileMatrix createSimpleMatrix(TileRange range) {
        return TileMatrix.builder()
                .tileRange(range)
                .resolution(SIMPLE_RESOLUTION)
                .pointOfOrigin(range.cornerOfOrigin().pointOfOrigin(SIMPLE_EXTENT))
                .crs("EPSG:4326")
                .tileSize(TILE_SIZE, TILE_SIZE)
                .build();
    }
}
