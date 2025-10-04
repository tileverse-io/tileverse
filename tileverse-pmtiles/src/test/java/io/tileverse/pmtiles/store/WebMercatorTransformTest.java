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
package io.tileverse.pmtiles.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TileIndex;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WebMercatorTransformTest {

    @TempDir
    private Path tmpFolder;

    @Test
    void testLatLonToWebMercatorRoundTrip() {
        // Test various geographic coordinates
        double[][] testCoords = {
            {0.0, 0.0}, // Equator, Prime Meridian
            {-180.0, -85.0511}, // Southwest corner
            {180.0, 85.0511}, // Northeast corner
            {1.412368, 42.427600}, // Andorra bounds (min)
            {1.787481, 42.657170}, // Andorra bounds (max)
            {-122.4194, 37.7749}, // San Francisco
            {2.3522, 48.8566} // Paris
        };

        for (double[] coord : testCoords) {
            double lon = coord[0];
            double lat = coord[1];

            // Forward transformation
            Coordinate webMercator = WebMercatorTransform.latLonToWebMercator(lon, lat);

            // Inverse transformation
            Coordinate latLonBack = WebMercatorTransform.webMercatorToLatLon(webMercator.x(), webMercator.y());

            // Should round-trip with high precision
            assertThat(latLonBack.x()).isCloseTo(lon, within(1e-10));
            assertThat(latLonBack.y()).isCloseTo(lat, within(1e-10));
        }
    }

    @Test
    void testTileCoordinateConversion() {
        // Test tile (0,0) at zoom 0 - should cover entire world
        TileIndex tile00 = TileIndex.xyz(0, 0, 0);
        BoundingBox2D geographicExtent = WebMercatorTransform.tileToGeographicExtent(tile00);

        // Should cover approximately the entire world
        assertThat(geographicExtent.minX()).isCloseTo(-180.0, within(0.1));
        assertThat(geographicExtent.maxX()).isCloseTo(180.0, within(0.1));
        assertThat(geographicExtent.minY()).isCloseTo(-85.051, within(0.1));
        assertThat(geographicExtent.maxY()).isCloseTo(85.051, within(0.1));
    }

    @Test
    void testLatLonToTileConversion() {
        // Test known coordinate to tile mappings at zoom 10
        int zoom = 10;

        // San Francisco should be around tile (163, 395) at zoom 10
        TileIndex sfTile = WebMercatorTransform.latLonToTile(-122.4194, 37.7749, zoom);
        assertThat(sfTile.x()).isBetween(160L, 170L); // Approximately correct
        assertThat(sfTile.y()).isBetween(390L, 400L);
        assertThat(sfTile.z()).isEqualTo(zoom);

        // Verify the tile contains the original coordinate
        BoundingBox2D tileExtent = WebMercatorTransform.tileToGeographicExtent(sfTile);
        assertThat(tileExtent.contains(Coordinate.of(-122.4194, 37.7749))).isTrue();
    }

    @Test
    void testWebMercatorBounds() {
        BoundingBox2D bounds = WebMercatorTransform.getWebMercatorBounds();
        double expectedBound = Math.PI * WebMercatorTransform.getEarthRadius();

        assertThat(bounds.minX()).isCloseTo(-expectedBound, within(1.0));
        assertThat(bounds.maxX()).isCloseTo(expectedBound, within(1.0));
        assertThat(bounds.minY()).isCloseTo(-expectedBound, within(1.0));
        assertThat(bounds.maxY()).isCloseTo(expectedBound, within(1.0));
    }
}
