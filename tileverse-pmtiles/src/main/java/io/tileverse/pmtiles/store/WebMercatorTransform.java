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

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TileIndex;

/**
 * High-precision Web Mercator (EPSG:3857) coordinate transformations.
 *
 * <p>This class provides accurate transformations between geographic coordinates
 * (EPSG:4326) and Web Mercator projected coordinates (EPSG:3857), as well as
 * tile coordinate calculations following the standard used by web mapping services.
 *
 * <p>The transformations use the precise mathematical formulas for the Spherical
 * Mercator projection, ensuring accuracy across all zoom levels and coordinate ranges.
 *
 * @since 1.0
 */
final class WebMercatorTransform {

    /**
     * Earth radius in meters (WGS84 semi-major axis).
     */
    private static final double EARTH_RADIUS = 6378137.0;

    /**
     * Half circumference of Earth in Web Mercator projection.
     */
    private static final double ORIGIN_SHIFT = Math.PI * EARTH_RADIUS;

    /**
     * Maximum latitude supported by Web Mercator projection.
     * Beyond this latitude, the projection becomes infinite.
     */
    private static final double MAX_LATITUDE = 85.0511287798;

    private WebMercatorTransform() {
        // Utility class - no instantiation
    }

    /**
     * Converts geographic coordinates (EPSG:4326) to Web Mercator coordinates (EPSG:3857).
     *
     * <p>Uses the standard Spherical Mercator projection formulas:
     * <ul>
     * <li>x = a * λ (where λ is longitude in radians)</li>
     * <li>y = a * ln[tan(π/4 + φ/2)] (where φ is latitude in radians)</li>
     * </ul>
     *
     * @param longitude longitude in decimal degrees
     * @param latitude latitude in decimal degrees (clamped to ±85.0511°)
     * @return Web Mercator coordinate in meters
     */
    public static Coordinate latLonToWebMercator(double longitude, double latitude) {
        // Clamp latitude to Web Mercator bounds
        double clampedLat = Math.max(-MAX_LATITUDE, Math.min(MAX_LATITUDE, latitude));

        // Convert to radians
        double lonRad = Math.toRadians(longitude);
        double latRad = Math.toRadians(clampedLat);

        // Apply transformation
        double x = EARTH_RADIUS * lonRad;
        double y = EARTH_RADIUS * Math.log(Math.tan(Math.PI / 4.0 + latRad / 2.0));

        return Coordinate.of(x, y);
    }

    /**
     * Converts Web Mercator coordinates (EPSG:3857) to geographic coordinates (EPSG:4326).
     *
     * <p>Uses the inverse Spherical Mercator projection formulas:
     * <ul>
     * <li>λ = x / a (longitude in radians)</li>
     * <li>φ = π/2 - 2 * atan(e^(-y/a)) (latitude in radians)</li>
     * </ul>
     *
     * @param x Web Mercator X coordinate in meters
     * @param y Web Mercator Y coordinate in meters
     * @return geographic coordinate in decimal degrees
     */
    public static Coordinate webMercatorToLatLon(double x, double y) {
        double longitude = Math.toDegrees(x / EARTH_RADIUS);
        double latitude = Math.toDegrees(Math.PI / 2.0 - 2.0 * Math.atan(Math.exp(-y / EARTH_RADIUS)));

        return Coordinate.of(longitude, latitude);
    }

    /**
     * Converts a geographic extent to Web Mercator extent.
     *
     * @param geographicExtent extent in EPSG:4326 coordinates (lon/lat)
     * @return extent in EPSG:3857 coordinates (meters)
     */
    public static BoundingBox2D latLonToWebMercator(BoundingBox2D geographicExtent) {
        Coordinate min = latLonToWebMercator(geographicExtent.minX(), geographicExtent.minY());
        Coordinate max = latLonToWebMercator(geographicExtent.maxX(), geographicExtent.maxY());

        return BoundingBox2D.extent(min.x(), min.y(), max.x(), max.y());
    }

    /**
     * Converts Web Mercator coordinates to tile coordinates at a specific zoom level.
     *
     * <p>This uses the standard XYZ/Google tile scheme where:
     * <ul>
     * <li>Tile (0,0) is at the top-left (northwest) corner</li>
     * <li>X increases eastward</li>
     * <li>Y increases southward (inverted from geographic coordinates)</li>
     * </ul>
     *
     * @param x Web Mercator X coordinate in meters
     * @param y Web Mercator Y coordinate in meters
     * @param zoom zoom level (0-based)
     * @return tile coordinates as TileIndex
     */
    public static TileIndex webMercatorToTile(double x, double y, int zoom) {
        // Normalize coordinates to [0,1] range
        double normalizedX = (x + ORIGIN_SHIFT) / (2.0 * ORIGIN_SHIFT);
        double normalizedY = (ORIGIN_SHIFT - y) / (2.0 * ORIGIN_SHIFT); // Y is flipped for XYZ scheme

        // Scale by 2^zoom to get tile coordinates
        long tilesAtZoom = 1L << zoom;
        long tileX = (long) Math.floor(normalizedX * tilesAtZoom);
        long tileY = (long) Math.floor(normalizedY * tilesAtZoom);

        // Clamp to valid tile range [0, 2^zoom - 1]
        tileX = Math.max(0, Math.min(tilesAtZoom - 1, tileX));
        tileY = Math.max(0, Math.min(tilesAtZoom - 1, tileY));

        return TileIndex.xyz(tileX, tileY, zoom);
    }

    /**
     * Converts geographic coordinates directly to tile coordinates.
     *
     * @param longitude longitude in decimal degrees
     * @param latitude latitude in decimal degrees
     * @param zoom zoom level (0-based)
     * @return tile coordinates as TileIndex
     */
    public static TileIndex latLonToTile(double longitude, double latitude, int zoom) {
        Coordinate webMercator = latLonToWebMercator(longitude, latitude);
        return webMercatorToTile(webMercator.x(), webMercator.y(), zoom);
    }

    /**
     * Converts tile coordinates to the Web Mercator extent of that tile.
     *
     * @param tileIndex the tile coordinates
     * @return Web Mercator extent of the tile in meters
     */
    public static BoundingBox2D tileToWebMercatorExtent(TileIndex tileIndex) {
        int zoom = tileIndex.z();
        long x = tileIndex.x();
        long y = tileIndex.y();

        long tilesAtZoom = 1L << zoom;
        double tileSize = (2.0 * ORIGIN_SHIFT) / tilesAtZoom;

        // Calculate tile bounds
        double minX = -ORIGIN_SHIFT + x * tileSize;
        double maxX = minX + tileSize;
        double maxY = ORIGIN_SHIFT - y * tileSize; // Y is flipped
        double minY = maxY - tileSize;

        return BoundingBox2D.extent(minX, minY, maxX, maxY);
    }

    /**
     * Converts tile coordinates to the geographic extent of that tile.
     *
     * @param tileIndex the tile coordinates
     * @return geographic extent of the tile in degrees
     */
    public static BoundingBox2D tileToGeographicExtent(TileIndex tileIndex) {
        BoundingBox2D webMercatorExtent = tileToWebMercatorExtent(tileIndex);

        Coordinate min = webMercatorToLatLon(webMercatorExtent.minX(), webMercatorExtent.minY());
        Coordinate max = webMercatorToLatLon(webMercatorExtent.maxX(), webMercatorExtent.maxY());

        return BoundingBox2D.extent(min.x(), min.y(), max.x(), max.y());
    }

    /**
     * Returns the Web Mercator coordinate bounds (full extent).
     *
     * @return the maximum extent of Web Mercator projection
     */
    public static BoundingBox2D getWebMercatorBounds() {
        return BoundingBox2D.extent(-ORIGIN_SHIFT, -ORIGIN_SHIFT, ORIGIN_SHIFT, ORIGIN_SHIFT);
    }

    /**
     * Returns the Earth radius constant used in transformations.
     *
     * @return Earth radius in meters (WGS84 semi-major axis)
     */
    public static double getEarthRadius() {
        return EARTH_RADIUS;
    }

    /**
     * Returns the maximum latitude supported by Web Mercator projection.
     *
     * @return maximum latitude in degrees
     */
    public static double getMaxLatitude() {
        return MAX_LATITUDE;
    }
}
