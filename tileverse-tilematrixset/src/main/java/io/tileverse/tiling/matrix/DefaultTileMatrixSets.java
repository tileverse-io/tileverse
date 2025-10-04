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

import static io.tileverse.tiling.common.CornerOfOrigin.TOP_LEFT;

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;

/**
 * Standard tile matrix sets commonly used in web mapping applications.
 * Provides constants and factory methods for creating tile matrix sets compatible
 * with GeoWebCache, OGC TMS specifications, and PMTiles.
 *
 * <p>This class includes the most commonly used tile matrix sets:
 * <ul>
 * <li>EPSG:4326 (WGS84) - Geographic coordinate system</li>
 * <li>EPSG:3857 (Web Mercator) - Spherical Mercator projection</li>
 * <li>OGC TMS WebMercatorQuad - Official OGC Tile Matrix Set</li>
 * <li>OGC TMS WorldCRS84Quad - Official OGC geographic Tile Matrix Set</li>
 * </ul>
 *
 * @since 1.0
 */
public class DefaultTileMatrixSets {

    private static final Coordinate WEBMERCATOR_BOTTOM_LEFT = new Coordinate(-20037508.3427892, -20037508.3427892);
    private static final Coordinate WEBMERCATOR_TOP_RIGHT = new Coordinate(20037508.3427892, 20037508.3427892);

    // Standard extents from GeoWebCache
    public static final BoundingBox2D WORLD4326 = BoundingBox2D.extent(-180.0, -90.0, 180.0, 90.0);
    public static final BoundingBox2D WebMercatorBounds =
            BoundingBox2D.of(WEBMERCATOR_BOTTOM_LEFT, WEBMERCATOR_TOP_RIGHT);

    // Official OGC TileMatrixSet specification values for WebMercator (GoogleMapsCompatible WKSS)
    // Source: http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible
    private static final double[] WEBMERCATOR_RESOLUTIONS = {
        156543.0339280410,
        78271.51696402048,
        39135.75848201023,
        19567.87924100512,
        9783.939620502561,
        4891.969810251280,
        2445.984905125640,
        1222.992452562820,
        611.4962262814100,
        305.7481131407048,
        152.8740565703525,
        76.43702828517624,
        38.21851414258813,
        19.10925707129406,
        9.554628535647032,
        4.777314267823516,
        2.388657133911758,
        1.194328566955879,
        0.5971642834779395,
        0.2985821417389697,
        0.1492910708694849,
        0.07464553543474244,
        0.03732276771737122,
        0.01866138385868561,
        0.009330691929342805
    };

    // OGC TMS WebMercatorQuad scale denominators
    private static final double[] WEBMERCATOR_QUAD_SCALES = {
        559082264.028717,
        279541132.014358,
        139770566.007179,
        69885283.0035897,
        34942641.5017948,
        17471320.7508974,
        8735660.37544871,
        4367830.18772435,
        2183915.09386217,
        1091957.54693108,
        545978.773465544,
        272989.386732772,
        136494.693366386,
        68247.346683193,
        34123.6733415964,
        17061.8366707982,
        8530.91833539913,
        4265.45916769956,
        2132.72958384978,
        1066.36479192489,
        533.182395962445,
        266.591197981222,
        133.295598990611,
        66.6477994953056,
        33.3238997476528
    };

    // CRS84 (EPSG:4326) quad scale denominators
    private static final double[] CRS84_QUAD_SCALES = {
        279541132.0143589,
        139770566.0071794,
        69885283.00358972,
        34942641.50179486,
        17471320.75089743,
        8735660.375448715,
        4367830.187724357,
        2183915.093862179,
        1091957.546931089,
        545978.7734655447,
        272989.3867327723,
        136494.6933663862,
        68247.34668319309,
        34123.67334159654,
        17061.83667079827,
        8530.918335399136,
        4265.459167699568,
        2132.729583849784,
    };

    // Default constants

    /**
     * EPSG:4326 (WGS84) tile matrix set - GlobalCRS84Geometric/EPSG:4326.
     * Two tiles at zoom 0 (2x1), standard 256px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_EPSG4326 = createEPSG4326(256);

    /**
     * EPSG:4326 (WGS84) tile matrix set with 512px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_EPSG4326x2 = createEPSG4326(512);

    /**
     * EPSG:3857 (WebMercator) tile matrix set - GoogleMapsCompatible/EPSG:3857.
     * Uses common practice resolutions, 256px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_EPSG3857 = createEPSG3857(256);

    /**
     * EPSG:3857 (WebMercator) tile matrix set with 512px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_EPSG3857x2 = createEPSG3857(512);

    /**
     * OGC TMS WebMercatorQuad - official OGC Tile Matrix Set.
     * EPSG:3857 with precise scale denominators, 256px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WEB_MERCATOR_QUAD = createWebMercatorQuad(256);

    /**
     * OGC TMS WebMercatorQuad with 512px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WEB_MERCATOR_QUADx2 = createWebMercatorQuad(512);

    /**
     * OGC TMS WorldCRS84Quad - official OGC Tile Matrix Set.
     * EPSG:4326 with precise scale denominators, 256px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_CRS84_QUAD = createWorldCRS84Quad(256);

    /**
     * OGC TMS WorldCRS84Quad with 512px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_CRS84_QUADx2 = createWorldCRS84Quad(512);

    // Factory methods

    private static TileMatrixSet createEPSG4326(int tileSize) {
        // EPSG:4326 starts with 2 tiles horizontally, 1 vertically at zoom 0
        TilePyramid.Builder pyramidBuilder =
                TilePyramid.builder().cornerOfOrigin(TOP_LEFT); // Geographic typically uses upper-left

        int maxZoom = 21; // Standard max for EPSG:4326
        for (int z = 0; z <= maxZoom; z++) {
            long tilesWide = 2L << z; // 2 * 2^z
            long tilesHigh = 1L << z; // 1 * 2^z
            TileRange range = TileRange.of(0, 0, tilesWide - 1, tilesHigh - 1, z, TOP_LEFT);
            pyramidBuilder.level(range);
        }

        TilePyramid pyramid = pyramidBuilder.build();

        // Calculate resolutions
        double[] resolutions = new double[maxZoom + 1];

        for (int z = 0; z <= maxZoom; z++) {
            // Geographic resolution: 360 degrees / (tileSize * 2 * 2^z)
            resolutions[z] = 360.0 / (tileSize * 2 * (1L << z));
        }

        return TileMatrixSet.builder()
                .tilePyramid(pyramid)
                .crs("EPSG:4326")
                .tileSize(tileSize, tileSize)
                .extent(WORLD4326)
                .resolutions(resolutions)
                .build();
    }

    private static TileMatrixSet createEPSG3857(int tileSize) {
        TilePyramid.Builder pyramidBuilder = TilePyramid.builder().cornerOfOrigin(TOP_LEFT); // Match PMTiles indexing

        int maxZoom = WEBMERCATOR_RESOLUTIONS.length - 1;
        for (int z = 0; z <= maxZoom; z++) {
            long tilesAtZoom = 1L << z; // 2^z tiles per dimension
            TileRange range = TileRange.of(0, 0, tilesAtZoom - 1, tilesAtZoom - 1, z, TOP_LEFT);
            pyramidBuilder.level(range);
        }

        TilePyramid pyramid = pyramidBuilder.build();

        // Use GeoWebCache resolutions, scale for tile size
        double[] resolutions = new double[WEBMERCATOR_RESOLUTIONS.length];

        double scaleFactor = tileSize / 256.0; // Scale resolutions for different tile sizes

        for (int i = 0; i < WEBMERCATOR_RESOLUTIONS.length; i++) {
            resolutions[i] = WEBMERCATOR_RESOLUTIONS[i] * scaleFactor;
        }

        return TileMatrixSet.builder()
                .tilePyramid(pyramid)
                .crs("EPSG:3857")
                .tileSize(tileSize, tileSize)
                .extent(WebMercatorBounds)
                .resolutions(resolutions)
                .build();
    }

    private static TileMatrixSet createWebMercatorQuad(int tileSize) {
        TilePyramid.Builder pyramidBuilder = TilePyramid.builder().cornerOfOrigin(TOP_LEFT);

        int maxZoom = WEBMERCATOR_QUAD_SCALES.length - 1;
        for (int z = 0; z <= maxZoom; z++) {
            long tilesAtZoom = 1L << z;
            TileRange range = TileRange.of(0, 0, tilesAtZoom - 1, tilesAtZoom - 1, z, TOP_LEFT);
            pyramidBuilder.level(range);
        }

        TilePyramid pyramid = pyramidBuilder.build();

        // Convert scale denominators to resolutions
        double[] resolutions = new double[WEBMERCATOR_QUAD_SCALES.length];

        double pixelSizeMeters = 0.00028; // Standard pixel size in meters
        double scaleFactor = tileSize / 256.0;

        for (int i = 0; i < WEBMERCATOR_QUAD_SCALES.length; i++) {
            // Resolution = scale_denominator * pixel_size_in_meters
            resolutions[i] = WEBMERCATOR_QUAD_SCALES[i] * pixelSizeMeters * scaleFactor;
        }

        BoundingBox2D world38572 = WebMercatorBounds;
        return TileMatrixSet.builder()
                .tilePyramid(pyramid)
                .crs("EPSG:3857")
                .tileSize(tileSize, tileSize)
                .extent(world38572)
                .resolutions(resolutions)
                .build();
    }

    private static TileMatrixSet createWorldCRS84Quad(int tileSize) {
        TilePyramid.Builder pyramidBuilder = TilePyramid.builder().cornerOfOrigin(TOP_LEFT);

        int maxZoom = CRS84_QUAD_SCALES.length - 1;
        for (int z = 0; z <= maxZoom; z++) {
            long tilesWide = 2L << z; // 2 * 2^z for CRS84
            long tilesHigh = 1L << z; // 1 * 2^z for CRS84
            TileRange range = TileRange.of(0, 0, tilesWide - 1, tilesHigh - 1, z, TOP_LEFT);
            pyramidBuilder.level(range);
        }

        TilePyramid pyramid = pyramidBuilder.build();

        // Convert scale denominators to resolutions
        double[] resolutions = new double[CRS84_QUAD_SCALES.length];

        double pixelSizeMeters = 0.00028;
        double scaleFactor = tileSize / 256.0;

        for (int i = 0; i < CRS84_QUAD_SCALES.length; i++) {
            resolutions[i] = CRS84_QUAD_SCALES[i] * pixelSizeMeters * scaleFactor;
        }

        return TileMatrixSet.builder()
                .tilePyramid(pyramid)
                .crs("EPSG:4326")
                .tileSize(tileSize, tileSize)
                .extent(WORLD4326)
                .resolutions(resolutions)
                .build();
    }
}
