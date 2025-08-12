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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.pyramid.AxisOrigin;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DefaultTileMatrixSetsTest {

    @Test
    void testWorldEPSG4326() {
        TileMatrixSet tms = DefaultTileMatrixSets.WORLD_EPSG4326;

        assertNotNull(tms);
        assertEquals("EPSG:4326", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());
        assertEquals(AxisOrigin.UPPER_LEFT, tms.tilePyramid().axisOrigin());

        // EPSG:4326 should have 22 zoom levels (0-21)
        assertEquals(0, tms.tilePyramid().minZoomLevel());
        assertEquals(21, tms.tilePyramid().maxZoomLevel());
        assertEquals(22, tms.tilePyramid().levels().size());

        // Test extent
        Extent extent = tms.extent();
        assertEquals(-180.0, extent.minX(), 0.001);
        assertEquals(-90.0, extent.minY(), 0.001);
        assertEquals(180.0, extent.maxX(), 0.001);
        assertEquals(90.0, extent.maxY(), 0.001);
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
        assertEquals(AxisOrigin.UPPER_LEFT, tms.tilePyramid().axisOrigin());

        // Should have 31 zoom levels (0-30) based on WEBMERCATOR_RESOLUTIONS
        assertEquals(0, tms.tilePyramid().minZoomLevel());
        assertEquals(30, tms.tilePyramid().maxZoomLevel());
        assertEquals(31, tms.tilePyramid().levels().size());

        // Test WebMercator extent
        Extent extent = tms.extent();
        assertEquals(-20037508.34, extent.minX(), 0.001);
        assertEquals(-20037508.34, extent.minY(), 0.001);
        assertEquals(20037508.34, extent.maxX(), 0.001);
        assertEquals(20037508.34, extent.maxY(), 0.001);
    }

    @Test
    void testWebMercatorQuad() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        assertNotNull(tms);
        assertEquals("EPSG:3857", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());

        // Should have 25 zoom levels (0-24) based on WEBMERCATOR_QUAD_SCALES
        assertEquals(0, tms.tilePyramid().minZoomLevel());
        assertEquals(24, tms.tilePyramid().maxZoomLevel());
        assertEquals(25, tms.tilePyramid().levels().size());

        // Test TMS extent (more precise)
        Extent extent = tms.extent();
        assertEquals(-20037508.3427892, extent.minX(), 0.001);
        assertEquals(-20037508.3427892, extent.minY(), 0.001);
        assertEquals(20037508.3427892, extent.maxX(), 0.001);
        assertEquals(20037508.3427892, extent.maxY(), 0.001);
    }

    @Test
    void testWorldCRS84Quad() {
        TileMatrixSet tms = DefaultTileMatrixSets.WORLD_CRS84_QUAD;

        assertNotNull(tms);
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
        assertTrue(matrix0.contains(TileIndex.of(0, 0, 0)));
        assertFalse(matrix0.contains(TileIndex.of(1, 0, 0))); // Outside bounds

        // Test coordinate to tile conversion
        Coordinate center = Coordinate.of(0, 0); // Center of WebMercator
        Optional<Tile> centerTileOpt = matrix0.coordinateToTile(center);
        assertTrue(centerTileOpt.isPresent());
        Tile centerTile = centerTileOpt.get();
        assertEquals(0, centerTile.x());
        assertEquals(0, centerTile.y());
        assertEquals(0, centerTile.z());

        // Test tile extent
        Extent tileExtent = centerTile.extent();
        assertNotNull(tileExtent);
        assertTrue(tileExtent.contains(center));
    }

    @Test
    void testTileMatrixSetIntersection() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test intersection with Europe extent (roughly)
        Extent europeExtent = Extent.of(-2000000, 4000000, 4000000, 8000000); // WebMercator coordinates
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
        Extent noIntersectionExtent = Extent.of(30000000, 30000000, 40000000, 40000000);
        Optional<TileMatrixSet> emptyIntersection = tms.intersection(noIntersectionExtent);
        assertThat(emptyIntersection).isEmpty();
    }

    @Test
    void testTileMatrixSetIntersectionAtZoomLevel() {
        TileMatrixSet tms = DefaultTileMatrixSets.WEB_MERCATOR_QUAD;

        // Test intersection with Europe extent at a specific zoom level
        Extent europeExtent = Extent.of(-2000000, 4000000, 4000000, 8000000); // WebMercator coordinates
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
        Extent noIntersectionExtent = Extent.of(30000000, 30000000, 40000000, 40000000);
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
        Extent europeExtent = Extent.of(-2000000, 4000000, 4000000, 8000000);
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
}
