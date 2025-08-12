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
package io.tileverse.pmtiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.tiling.matrix.TileMatrixSet;
import org.junit.jupiter.api.Test;

class PMTilesTileMatrixSetTest {

    @Test
    void testFromWebMercator() {
        // Create a test header with zoom levels 5-15 and NYC bounds
        PMTilesHeader header = PMTilesHeader.builder()
                .minZoom((byte) 5)
                .maxZoom((byte) 15)
                .minLon(-74.1) // NYC west
                .minLat(40.6) // NYC south
                .maxLon(-73.9) // NYC east
                .maxLat(40.8) // NYC north
                .build();

        TileMatrixSet tms = PMTilesTileMatrixSet.fromWebMercator(header);

        assertNotNull(tms);
        assertEquals("EPSG:3857", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());

        // Check zoom level range
        assertEquals(5, tms.tilePyramid().minZoomLevel());
        assertEquals(15, tms.tilePyramid().maxZoomLevel());

        // Check that we have the correct number of levels
        assertEquals(11, tms.tilePyramid().levels().size()); // 5-15 inclusive = 11 levels

        // Verify that tile ranges are smaller than full world (due to NYC bounds)
        // At zoom level 5, full world would be 32x32 tiles, but NYC should be much smaller
        io.tileverse.tiling.pyramid.TileRange level5Range = tms.tilePyramid().tileRange(5);
        assertTrue(level5Range.count() < 32 * 32, "NYC bounds should result in fewer tiles than full world");
        assertTrue(level5Range.count() > 0, "Should have some tiles in the bounded area");
    }

    @Test
    void testFromWebMercator512() {
        PMTilesHeader header = PMTilesHeader.builder()
                .minZoom((byte) 3)
                .maxZoom((byte) 10)
                .minLon(-1.0)
                .minLat(51.0)
                .maxLon(1.0)
                .maxLat(52.0) // London area
                .build();

        TileMatrixSet tms = PMTilesTileMatrixSet.fromWebMercator512(header);

        assertNotNull(tms);
        assertEquals("EPSG:3857", tms.crsId());
        assertEquals(512, tms.tileWidth());
        assertEquals(512, tms.tileHeight());

        assertEquals(3, tms.tilePyramid().minZoomLevel());
        assertEquals(10, tms.tilePyramid().maxZoomLevel());
        assertEquals(8, tms.tilePyramid().levels().size()); // 3-10 inclusive = 8 levels
    }

    @Test
    void testFromCRS84() {
        PMTilesHeader header =
                PMTilesHeader.builder().minZoom((byte) 2).maxZoom((byte) 8).build();

        TileMatrixSet tms = PMTilesTileMatrixSet.fromCRS84(header);

        assertNotNull(tms);
        assertEquals("EPSG:4326", tms.crsId());
        assertEquals(256, tms.tileWidth());
        assertEquals(256, tms.tileHeight());

        assertEquals(2, tms.tilePyramid().minZoomLevel());
        assertEquals(8, tms.tilePyramid().maxZoomLevel());
        assertEquals(7, tms.tilePyramid().levels().size()); // 2-8 inclusive = 7 levels
    }

    @Test
    void testResolutionsAreCorrect() {
        PMTilesHeader header =
                PMTilesHeader.builder().minZoom((byte) 0).maxZoom((byte) 2).build();

        TileMatrixSet tms = PMTilesTileMatrixSet.fromWebMercator(header);

        // Test that resolutions decrease (become more detailed) as zoom increases
        double res0 = tms.resolution(0);
        double res1 = tms.resolution(1);
        double res2 = tms.resolution(2);

        assertTrue(res0 > res1, "Resolution should decrease from zoom 0 to 1");
        assertTrue(res1 > res2, "Resolution should decrease from zoom 1 to 2");

        // Test that resolutions roughly double between levels (within precision)
        double ratio1 = res0 / res1;
        double ratio2 = res1 / res2;

        assertTrue(Math.abs(ratio1 - 2.0) < 0.1, "Resolution ratio should be close to 2.0, was: " + ratio1);
        assertTrue(Math.abs(ratio2 - 2.0) < 0.1, "Resolution ratio should be close to 2.0, was: " + ratio2);
    }

    @Test
    void testInvalidZoomRange() {
        PMTilesHeader header = PMTilesHeader.builder()
                .minZoom((byte) 30) // Beyond available zoom levels
                .maxZoom((byte) 35)
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            PMTilesTileMatrixSet.fromWebMercator(header);
        });
    }
}
