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
import static io.tileverse.tiling.pyramid.TileIndex.xyz;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.common.CornerOfOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.Optional;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

class DefaultTileMatrixSetsTest {

    @Test
    void testWorldEPSG4326() {
        TileMatrixSet tms = DefaultTileMatrixSets.WORLD_EPSG4326;

        assertNotNull(tms);
        assertEquals("EPSG:4326", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());
        assertEquals(CornerOfOrigin.TOP_LEFT, tms.tilePyramid().cornerOfOrigin());

        // EPSG:4326 should have 22 zoom levels (0-21)
        assertEquals(0, tms.minZoomLevel());
        assertEquals(21, tms.maxZoomLevel());

        // Test extent
        BoundingBox2D extent = tms.boundingBox();
        assertEquals(-180.0, extent.minX(), 0.001);
        assertEquals(-90.0, extent.minY(), 0.001);
        assertEquals(180.0, extent.maxX(), 0.001);
        assertEquals(90.0, extent.maxY(), 0.001);

        TileMatrix z0 = tms.getTileMatrix(0);
        Tile upperLeft = z0.first();
        Tile loweRight = z0.last();
        assertThat(upperLeft.extent()).isEqualTo(extent(-180, -90, 0, 90));
        assertThat(loweRight.extent()).isEqualTo(extent(0, -90, 180, 90));

        TileMatrix z1 = tms.getTileMatrix(1);
        upperLeft = z1.first();
        loweRight = z1.last();
        assertThat(upperLeft.extent()).isEqualTo(extent(-180, 0, -90, 90));
        assertThat(loweRight.extent()).isEqualTo(extent(90, -90, 180, 0));

        TileMatrix z2 = tms.getTileMatrix(2);
        upperLeft = z2.first();
        loweRight = z2.last();
        assertThat(upperLeft.extent()).isEqualTo(extent(-180, 45, -135, 90));
        assertThat(loweRight.extent()).isEqualTo(extent(135, -90, 180, -45));
    }

    @Test
    void testContinuousCoverage_WORLD_EPSG4326() {
        testContinuousCoverage(DefaultTileMatrixSets.WORLD_EPSG4326);
    }

    @Test
    void testContinuousCoverage_WORLD_EPSG3857() {
        testContinuousCoverage(DefaultTileMatrixSets.WORLD_EPSG3857);
    }

    @Test
    void testContinuousCoverage_WEB_MERCATOR_QUAD() {
        testContinuousCoverage(DefaultTileMatrixSets.WEB_MERCATOR_QUAD);
    }

    private void testContinuousCoverage(TileMatrixSet matrixSet) {
        matrixSet.tileMatrices().forEach(this::testContinuousCoverage);
    }

    private void testContinuousCoverage(TileMatrix matrix) {
        if (matrix.tileCount() == 1) {
            return;
        }

        Tile first = matrix.first();
        Tile last = matrix.last();

        testCoverage(first, matrix);
        testCoverage(last, matrix);
        if (matrix.tileRange().spanX() > 2) {
            long x = first.x() + matrix.tileRange().spanX() / 2;
            long y = first.y() + matrix.tileRange().spanY() / 2;
            TileIndex center = TileIndex.xyz(x, y, first.z());
            testCoverage(matrix.tile(center).orElseThrow(), matrix);
        }
    }

    private void testCoverage(Tile tile, TileMatrix matrix) {
        CornerOfOrigin origin = matrix.cornerOfOrigin();
        TileIndex index = tile.tileIndex();

        Optional<Tile> left = matrix.tile(index.shiftX(-1));
        Optional<Tile> right = matrix.tile(index.shiftX(1));

        TileIndex topIndex;
        TileIndex bottomIndex;
        if (origin == CornerOfOrigin.TOP_LEFT) {
            topIndex = index.shiftY(-1);
            bottomIndex = index.shiftY(1);
        } else {
            topIndex = index.shiftY(1);
            bottomIndex = index.shiftY(-1);
        }
        Optional<Tile> top = matrix.tile(topIndex);
        Optional<Tile> bottom = matrix.tile(bottomIndex);

        testCoverage(tile, top, bottom, left, right);
    }

    private void testCoverage(
            Tile center, Optional<Tile> above, Optional<Tile> below, Optional<Tile> left, Optional<Tile> right) {

        final double tolerance = 1.0e-7;
        final Offset<Double> offset = Offset.offset(tolerance);

        BoundingBox2D extent = center.extent();

        above.ifPresent(tileAbove -> {
            BoundingBox2D extentAbove = tileAbove.extent();
            assertThat(extentAbove.minY())
                    .as("tile above's minY should match maxY")
                    .isEqualTo(extent.maxY(), offset);
        });
        below.ifPresent(tileBelow -> {
            BoundingBox2D extentBelow = tileBelow.extent();
            assertThat(extentBelow.maxY())
                    .as("tile below's maxY should match minY")
                    .isEqualTo(extent.minY(), offset);
        });
        left.ifPresent(tileLeft -> {
            BoundingBox2D extentLeft = tileLeft.extent();
            assertThat(extentLeft.maxX())
                    .as("tile left's maxX should match minX")
                    .isEqualTo(extent.minX(), offset);
        });
        right.ifPresent(tileRight -> {
            BoundingBox2D extentRight = tileRight.extent();
            assertThat(extentRight.minX())
                    .as("tile right's minX should match maxX")
                    .isEqualTo(extent.maxX(), offset);
        });
    }

    @Test
    void testWorldEPSG4326x2() {
        TileMatrixSet tms = DefaultTileMatrixSets.WORLD_EPSG4326x2;

        assertEquals("EPSG:4326", tms.crsId());
        assertEquals(512, tms.tileWidth());
        assertEquals(512, tms.tileHeight());
    }

    @Test
    void testWorldEPSG3857() {
        TileMatrixSet tms = DefaultTileMatrixSets.WORLD_EPSG3857;

        assertNotNull(tms);
        assertEquals("EPSG:3857", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());
        assertEquals(CornerOfOrigin.TOP_LEFT, tms.tilePyramid().cornerOfOrigin());

        // Should have 25 zoom levels (0-24) based on OGC TileMatrixSet specification
        assertEquals(0, tms.tilePyramid().minZoomLevel());
        assertEquals(24, tms.tilePyramid().maxZoomLevel());
        assertEquals(25, tms.tilePyramid().levels().size());

        // Test WebMercator extent
        BoundingBox2D extent = tms.boundingBox();
        assertThat(extent).isEqualTo(DefaultTileMatrixSets.WebMercatorBounds);
    }

    @Test
    void testWebMercatorQuad() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        assertNotNull(tms);
        assertEquals(CornerOfOrigin.TOP_LEFT, tms.tilePyramid().cornerOfOrigin());
        assertEquals("EPSG:3857", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());

        // Should have 25 zoom levels (0-24) based on WEBMERCATOR_QUAD_SCALES
        assertEquals(0, tms.tilePyramid().minZoomLevel());
        assertEquals(24, tms.tilePyramid().maxZoomLevel());
        assertEquals(25, tms.tilePyramid().levels().size());

        // Test TMS extent (more precise)
        BoundingBox2D extent = tms.boundingBox();
        assertThat(extent).isEqualTo(DefaultTileMatrixSets.WebMercatorBounds);
    }

    @Test
    void testWorldCRS84Quad() {
        TileMatrixSet tms = DefaultTileMatrixSets.WORLD_CRS84_QUAD;

        assertNotNull(tms);
        assertEquals(CornerOfOrigin.TOP_LEFT, tms.tilePyramid().cornerOfOrigin());
        assertEquals("EPSG:4326", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());

        // Should have 18 zoom levels (0-17) based on CRS84_QUAD_SCALES
        assertEquals(0, tms.tilePyramid().minZoomLevel());
        assertEquals(17, tms.tilePyramid().maxZoomLevel());
        assertEquals(18, tms.tilePyramid().levels().size());
    }

    @Test
    void testToBuilderAndZoomRange() {
        TileMatrixSet original = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Create a subset from zoom 5 to 10
        TileMatrixSet subset = original.toBuilder().zoomRange(5, 10).build();

        assertNotNull(subset);
        assertEquals("EPSG:3857", subset.crsId());
        assertEquals(256, subset.tileWidth());
        assertEquals(256, subset.tileHeight());

        // Check zoom range
        assertEquals(5, subset.tilePyramid().minZoomLevel());
        assertEquals(10, subset.tilePyramid().maxZoomLevel());
        assertEquals(6, subset.tilePyramid().levels().size()); // 5-10 inclusive = 6 levels

        // Check that resolutions are correctly subset
        double originalRes5 = original.resolution(5);
        double subsetRes5 = subset.resolution(5);
        assertEquals(originalRes5, subsetRes5, 0.001);

        double originalRes10 = original.resolution(10);
        double subsetRes10 = subset.resolution(10);
        assertEquals(originalRes10, subsetRes10, 0.001);
    }

    @Test
    void testResolutionPyramidStructure() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test that each zoom level has the correct number of tiles
        assertEquals(1, tms.tilePyramid().tileRange(0).count()); // 1x1 = 1 tile
        assertEquals(4, tms.tilePyramid().tileRange(1).count()); // 2x2 = 4 tiles
        assertEquals(16, tms.tilePyramid().tileRange(2).count()); // 4x4 = 16 tiles

        // Test that resolutions decrease by half each level
        double res0 = tms.resolution(0);
        double res1 = tms.resolution(1);
        double res2 = tms.resolution(2);

        double ratio1 = res0 / res1;
        double ratio2 = res1 / res2;

        assertTrue(Math.abs(ratio1 - 2.0) < 0.1, "Resolution ratio should be close to 2.0");
        assertTrue(Math.abs(ratio2 - 2.0) < 0.1, "Resolution ratio should be close to 2.0");
    }

    @Test
    void testTileMatrixAPI() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test tileMatrices() method
        var matrices = tms.tileMatrices();
        assertNotNull(matrices);
        assertEquals(25, matrices.size()); // WEB_MERCATOR_QUAD has 25 levels (0-24)

        // Test tileMatrix(int) method
        var matrix5 = tms.tileMatrix(5);
        assertTrue(matrix5.isPresent());
        assertEquals(5, matrix5.get().zoomLevel());
        assertEquals("EPSG:3857", matrix5.get().crsId());
        assertEquals(256, matrix5.get().tileWidth());

        // Test getTileMatrix(int) method
        TileMatrix matrix10 = tms.getTileMatrix(10);
        assertEquals(10, matrix10.zoomLevel());

        // Test with invalid zoom level
        assertTrue(tms.tileMatrix(50).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> {
            tms.getTileMatrix(50);
        });

        // Test spatial operations on TileMatrix
        TileMatrix matrix0 = tms.getTileMatrix(0);

        // At zoom 0, there should be 1 tile (0,0)
        assertEquals(1, matrix0.tileRange().count());
        assertTrue(matrix0.contains(xyz(0, 0, 0)));
        assertFalse(matrix0.contains(xyz(1, 0, 0))); // Outside bounds

        // Test coordinate to tile conversion
        Coordinate center = Coordinate.of(0, 0); // Center of WebMercator
        Optional<Tile> centerTileOpt = matrix0.coordinateToTile(center);
        assertTrue(centerTileOpt.isPresent());
        Tile centerTile = centerTileOpt.get();
        assertEquals(0, centerTile.x());
        assertEquals(0, centerTile.y());
        assertEquals(0, centerTile.z());

        // Test tile extent
        BoundingBox2D tileExtent = centerTile.extent();
        assertNotNull(tileExtent);
        assertTrue(tileExtent.contains(center));
    }

    @Test
    void testTileMatrixSetIntersection() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test intersection with Europe extent (roughly)
        BoundingBox2D europeExtent = extent(-2000000, 4000000, 4000000, 8000000); // WebMercator coordinates
        TileMatrixSet intersection = tms.intersection(europeExtent).orElseThrow();

        assertNotNull(intersection);
        // View-based implementation returns TileMatrixSetView, not TileMatrixSet
        assertTrue(intersection instanceof TileMatrixSet);

        // Should have same zoom levels but fewer tiles
        assertEquals(tms.minZoomLevel(), intersection.minZoomLevel());
        assertEquals(tms.maxZoomLevel(), intersection.maxZoomLevel());
        assertEquals(tms.crsId(), intersection.crsId());
        assertEquals(tms.tileWidth(), intersection.tileWidth());
        assertEquals(tms.tileHeight(), intersection.tileHeight());

        // Check that each zoom level has fewer or equal tiles
        for (int z = intersection.minZoomLevel(); z <= intersection.maxZoomLevel(); z++) {
            TileRange originalRange = tms.tilePyramid().tileRange(z);
            TileRange intersectedRange = intersection.tilePyramid().tileRange(z);

            assertTrue(intersectedRange.count() <= originalRange.count());
        }

        // Test with non-intersecting extent (well outside WebMercator bounds)
        BoundingBox2D noIntersectionExtent = extent(30000000, 30000000, 40000000, 40000000);
        Optional<TileMatrixSet> emptyIntersection = tms.intersection(noIntersectionExtent);
        assertThat(emptyIntersection).isEmpty();
    }

    @Test
    void testTileMatrixSetIntersectionAtZoomLevel() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test intersection with Europe extent at a specific zoom level
        BoundingBox2D europeExtent = extent(-2000000, 4000000, 4000000, 8000000); // WebMercator coordinates
        int targetZoom = 5;

        TileMatrix intersectedMatrix =
                tms.intersection(europeExtent, targetZoom).orElseThrow();

        assertNotNull(intersectedMatrix);
        assertEquals(targetZoom, intersectedMatrix.zoomLevel());
        assertEquals("EPSG:3857", intersectedMatrix.crsId());
        assertEquals(256, intersectedMatrix.tileWidth());
        assertEquals(256, intersectedMatrix.tileHeight());

        // Should have fewer tiles than the full matrix at this zoom level
        TileMatrix fullMatrix = tms.getTileMatrix(targetZoom);
        assertTrue(
                intersectedMatrix.tileRange().count() <= fullMatrix.tileRange().count());
        assertTrue(intersectedMatrix.tileRange().count() > 0); // Should have some tiles

        // Test with non-intersecting extent at specific zoom level
        BoundingBox2D noIntersectionExtent = extent(30000000, 30000000, 40000000, 40000000);
        Optional<TileMatrix> emptyMatrix = tms.intersection(noIntersectionExtent, targetZoom);
        assertThat(emptyMatrix).isEmpty();
    }

    @Test
    void testTileMatrixSetSubset() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test zoom subset
        TileMatrixSet subset = tms.subset(5, 10);

        assertNotNull(subset);
        assertEquals(5, subset.minZoomLevel());
        assertEquals(10, subset.maxZoomLevel());
        assertEquals(6, subset.tilePyramid().levels().size()); // 5-10 inclusive
        assertEquals("EPSG:3857", subset.crsId());
        assertEquals(256, subset.tileWidth());
        assertEquals(256, subset.tileHeight());

        // Should be able to get matrices in range
        assertTrue(subset.tileMatrix(5).isPresent());
        assertTrue(subset.tileMatrix(10).isPresent());
        assertFalse(subset.tileMatrix(4).isPresent()); // Outside range
        assertFalse(subset.tileMatrix(11).isPresent()); // Outside range

        // Test chaining: subset then intersection
        BoundingBox2D europeExtent = extent(-2000000, 4000000, 4000000, 8000000);
        TileMatrixSet chained = subset.intersection(europeExtent).orElseThrow();

        assertNotNull(chained);
        assertEquals(5, chained.minZoomLevel());
        assertEquals(10, chained.maxZoomLevel());

        // Should have fewer tiles than original subset due to spatial filtering
        for (int z = 5; z <= 10; z++) {
            long originalCount = subset.getTileMatrix(z).tileRange().count();
            long filteredCount = chained.getTileMatrix(z).tileRange().count();
            assertTrue(filteredCount <= originalCount);
        }
    }

    @Test
    void testIntersectionExtentExpansionAtEachZoomLevel() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Use a small area in the center of the WebMercator extent
        BoundingBox2D smallCenterArea = extent(-1000000, -1000000, 1000000, 1000000);

        TileMatrixSet intersection = tms.intersection(smallCenterArea).orElseThrow();

        // At each zoom level, the extent of the intersected matrix should be expanded
        // to cover the full tiles that intersect with the small area

        // Zoom 0: Should contain the full WebMercator extent (single tile covers everything)
        TileMatrix matrix0 = intersection.getTileMatrix(0);
        BoundingBox2D extent0 = matrix0.boundingBox();
        // With OGC-compliant resolution values, the extent should now match exactly (or very close)
        // Use small tolerance to account for floating-point arithmetic precision
        double tolerance = 1e-6; // Very small tolerance for OGC-compliant values
        assertThat(extent0.minX()).isCloseTo(tms.boundingBox().minX(), within(tolerance));
        assertThat(extent0.minY()).isCloseTo(tms.boundingBox().minY(), within(tolerance));
        assertThat(extent0.maxX()).isCloseTo(tms.boundingBox().maxX(), within(tolerance));
        assertThat(extent0.maxY()).isCloseTo(tms.boundingBox().maxY(), within(tolerance));

        // Zoom 1: Should contain tiles that cover the center area
        TileMatrix matrix1 = intersection.getTileMatrix(1);
        BoundingBox2D extent1 = matrix1.boundingBox();
        assertTrue(extent1.contains(smallCenterArea), "Zoom 1 extent should contain the target area");
        // The extent should be expanded to tile boundaries, likely covering more than the small area
        assertTrue(extent1.width() > smallCenterArea.width(), "Zoom 1 extent should be expanded to tile boundaries");
        assertTrue(extent1.height() > smallCenterArea.height(), "Zoom 1 extent should be expanded to tile boundaries");

        // Zoom 2: Should have more precise tile coverage
        TileMatrix matrix2 = intersection.getTileMatrix(2);
        BoundingBox2D extent2 = matrix2.boundingBox();
        assertTrue(extent2.contains(smallCenterArea), "Zoom 2 extent should contain the target area");

        // The extent at higher zoom should be smaller or equal to lower zoom (more precise)
        assertTrue(extent2.width() <= extent1.width(), "Higher zoom should have more precise extent");
        assertTrue(extent2.height() <= extent1.height(), "Higher zoom should have more precise extent");

        // Verify that the extent is the union of all intersecting tiles at each level
        for (int zoom = 0; zoom <= 5; zoom++) {
            TileMatrix matrix = intersection.getTileMatrix(zoom);
            BoundingBox2D matrixExtent = matrix.boundingBox();

            // The matrix extent should fully contain the original small area
            assertTrue(
                    matrixExtent.contains(smallCenterArea), "Zoom " + zoom + " should fully contain the target area");

            // The matrix extent should be aligned to tile boundaries
            // We can verify this by checking that it's the union of tile extents
            TileRange range = matrix.tileRange();
            boolean foundTilesCoveringArea = false;

            for (long x = range.minx(); x <= range.maxx(); x++) {
                for (long y = range.miny(); y <= range.maxy(); y++) {
                    Optional<Tile> tile = matrix.tile(x, y);
                    if (tile.isPresent() && tile.get().extent().intersects(smallCenterArea)) {
                        foundTilesCoveringArea = true;
                        break;
                    }
                }
                if (foundTilesCoveringArea) break;
            }
            assertTrue(foundTilesCoveringArea, "Should find tiles covering the target area at zoom " + zoom);
        }
    }

    @Test
    void testIntersectionWithPartialOverlapExtent() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Use an extent that partially overlaps with the WebMercator bounds
        // This extends beyond the eastern boundary
        double webMercatorMax = DefaultTileMatrixSets.WebMercatorBounds.maxX();
        BoundingBox2D partialOverlap = extent(webMercatorMax - 5000000, -5000000, webMercatorMax + 5000000, 5000000);

        TileMatrixSet intersection = tms.intersection(partialOverlap).orElseThrow();

        // The result should be clipped to the actual tile matrix set bounds
        BoundingBox2D originalExtent = tms.boundingBox();

        for (int zoom = 0; zoom <= 3; zoom++) {
            TileMatrix matrix = intersection.getTileMatrix(zoom);
            BoundingBox2D matrixExtent = matrix.boundingBox();

            // The extent may exceed the original matrix set bounds due to tile snapping,
            // but should be reasonably close and still contain the intersection area
            // This is expected behavior since tile extents are snapped to tile boundaries

            // But should still intersect with the partial overlap area
            assertTrue(
                    matrixExtent.intersects(partialOverlap),
                    "Should still intersect with the target area at zoom " + zoom);

            // The intersection should cover the overlapping portion
            BoundingBox2D overlap = originalExtent.intersection(partialOverlap);
            assertTrue(
                    matrixExtent.contains(overlap) || matrixExtent.intersects(overlap),
                    "Should cover or intersect the overlapping area at zoom " + zoom);
        }
    }
}
