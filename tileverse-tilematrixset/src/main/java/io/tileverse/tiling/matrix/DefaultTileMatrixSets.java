/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.matrix;

import static io.tileverse.tiling.pyramid.CornerOfOrigin.TOP_LEFT;

import io.tileverse.geom.BoundingBox2D;
import io.tileverse.geom.Coordinate;
import io.tileverse.tiling.pyramid.TilePyramid;
import io.tileverse.tiling.pyramid.TileRange;
import java.net.URI;

/**
 * Standard tile matrix sets commonly used in web mapping applications. Provides constants and factory methods for
 * creating tile matrix sets compatible with GeoWebCache, OGC TMS specifications, and PMTiles.
 *
 * <p>This class includes the most commonly used tile matrix sets:
 *
 * <ul>
 *   <li>EPSG:4326 (WGS84) - Geographic coordinate system
 *   <li>EPSG:3857 (Web Mercator) - Spherical Mercator projection
 *   <li>OGC TMS WebMercatorQuad - Official OGC Tile Matrix Set
 *   <li>OGC TMS WorldCRS84Quad - Official OGC geographic Tile Matrix Set
 * </ul>
 *
 * @since 1.0
 */
public final class DefaultTileMatrixSets {

    private DefaultTileMatrixSets() {
        // utility class
    }

    // OGC well-known scale set URIs from http://www.opengis.net/def/wkss/OGC/1.0/
    private static final URI WKSS_GOOGLE_MAPS_COMPATIBLE =
            URI.create("http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible");
    private static final URI WKSS_GOOGLE_CRS84_QUAD =
            URI.create("http://www.opengis.net/def/wkss/OGC/1.0/GoogleCRS84Quad");

    // CRS URI references
    private static final URI CRS_EPSG_3857 = URI.create("http://www.opengis.net/def/crs/EPSG/0/3857");
    private static final URI CRS_EPSG_4326 = URI.create("http://www.opengis.net/def/crs/EPSG/0/4326");
    private static final URI CRS_OGC_CRS84 = URI.create("http://www.opengis.net/def/crs/OGC/1.3/CRS84");

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

    // The deepest level of the OGC WorldCRS84Quad definition
    private static final int WORLD_CRS84_QUAD_MAX_ZOOM = 17;

    // Default constants

    /**
     * EPSG:4326 (WGS84) tile matrix set - GlobalCRS84Geometric/EPSG:4326. Two tiles at zoom 0 (2x1), standard 256px
     * tiles. Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_EPSG4326 = createEPSG4326(256);

    /**
     * EPSG:4326 (WGS84) tile matrix set with 512px tiles. Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     * A tile covers the same ground as the 256px tile with the same index.
     */
    public static final TileMatrixSet WORLD_EPSG4326x2 = createEPSG4326(512);

    /**
     * EPSG:3857 (WebMercator) tile matrix set - GoogleMapsCompatible/EPSG:3857. Uses common practice resolutions, 256px
     * tiles. Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_EPSG3857 = createEPSG3857(256);

    /**
     * EPSG:3857 (WebMercator) tile matrix set with 512px tiles. Uses UPPER_LEFT axis origin compatible with PMTiles
     * indexing. A tile covers the same ground as the 256px tile with the same index.
     */
    public static final TileMatrixSet WORLD_EPSG3857x2 = createEPSG3857(512);

    /**
     * OGC TMS WebMercatorQuad - official OGC Tile Matrix Set. EPSG:3857 with precise scale denominators, 256px tiles.
     * Uses UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WEB_MERCATOR_QUAD = createWebMercatorQuad(256);

    /**
     * OGC TMS WebMercatorQuad with 512px tiles. Uses UPPER_LEFT axis origin compatible with PMTiles indexing. A tile
     * covers the same ground as the 256px tile with the same index.
     */
    public static final TileMatrixSet WEB_MERCATOR_QUADx2 = createWebMercatorQuad(512);

    /**
     * OGC TMS WorldCRS84Quad - official OGC Tile Matrix Set. CRS84 (WGS84 longitude, latitude), 256px tiles. Uses
     * UPPER_LEFT axis origin compatible with PMTiles indexing.
     */
    public static final TileMatrixSet WORLD_CRS84_QUAD = createWorldCRS84Quad(256);

    /**
     * OGC TMS WorldCRS84Quad with 512px tiles. Uses UPPER_LEFT axis origin compatible with PMTiles indexing. A tile
     * covers the same ground as the 256px tile with the same index.
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

        double[] resolutions = geographicResolutions(tileSize, maxZoom);

        return TileMatrixSet.builder()
                .identifier(tileSize == 256 ? "WORLD_EPSG4326" : "WORLD_EPSG4326x" + (tileSize / 256))
                .tilePyramid(pyramid)
                .crs("EPSG:4326")
                .supportedCRS(CRS_EPSG_4326)
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
        for (int i = 0; i < WEBMERCATOR_RESOLUTIONS.length; i++) {
            resolutions[i] = resolutionForTileSize(WEBMERCATOR_RESOLUTIONS[i], tileSize);
        }

        return TileMatrixSet.builder()
                .identifier(tileSize == 256 ? "WORLD_EPSG3857" : "WORLD_EPSG3857x" + (tileSize / 256))
                .tilePyramid(pyramid)
                .crs("EPSG:3857")
                .supportedCRS(CRS_EPSG_3857)
                .tileSize(tileSize, tileSize)
                .extent(WebMercatorBounds)
                .resolutions(resolutions)
                .wellKnownScaleSet(WKSS_GOOGLE_MAPS_COMPATIBLE)
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

        // Convert scale denominators to resolutions in meters
        double[] resolutions = new double[WEBMERCATOR_QUAD_SCALES.length];
        for (int i = 0; i < WEBMERCATOR_QUAD_SCALES.length; i++) {
            double resolutionOf256pxTiles = WEBMERCATOR_QUAD_SCALES[i] * CrsUnits.STANDARD_PIXEL_SIZE_METERS;
            resolutions[i] = resolutionForTileSize(resolutionOf256pxTiles, tileSize);
        }

        BoundingBox2D world38572 = WebMercatorBounds;
        return TileMatrixSet.builder()
                .identifier(tileSize == 256 ? "WebMercatorQuad" : "WebMercatorQuadx" + (tileSize / 256))
                .tilePyramid(pyramid)
                .crs("EPSG:3857")
                .supportedCRS(CRS_EPSG_3857)
                .tileSize(tileSize, tileSize)
                .extent(world38572)
                .resolutions(resolutions)
                .wellKnownScaleSet(WKSS_GOOGLE_MAPS_COMPATIBLE)
                .build();
    }

    private static TileMatrixSet createWorldCRS84Quad(int tileSize) {
        TilePyramid.Builder pyramidBuilder = TilePyramid.builder().cornerOfOrigin(TOP_LEFT);

        int maxZoom = WORLD_CRS84_QUAD_MAX_ZOOM;
        for (int z = 0; z <= maxZoom; z++) {
            long tilesWide = 2L << z; // 2 * 2^z for CRS84
            long tilesHigh = 1L << z; // 1 * 2^z for CRS84
            TileRange range = TileRange.of(0, 0, tilesWide - 1, tilesHigh - 1, z, TOP_LEFT);
            pyramidBuilder.level(range);
        }

        TilePyramid pyramid = pyramidBuilder.build();

        double[] resolutions = geographicResolutions(tileSize, maxZoom);

        return TileMatrixSet.builder()
                .identifier(tileSize == 256 ? "WorldCRS84Quad" : "WorldCRS84Quadx" + (tileSize / 256))
                .tilePyramid(pyramid)
                .crs("CRS:84")
                .supportedCRS(CRS_OGC_CRS84)
                .tileSize(tileSize, tileSize)
                .extent(WORLD4326)
                .resolutions(resolutions)
                .wellKnownScaleSet(WKSS_GOOGLE_CRS84_QUAD)
                .build();
    }

    /**
     * Resolutions in degrees from level 0 to {@code maxZoom}. Two tiles span the 360 degrees of level 0, and each level
     * doubles the number of tiles across.
     */
    private static double[] geographicResolutions(int tileSize, int maxZoom) {
        double[] resolutions = new double[maxZoom + 1];
        for (int z = 0; z <= maxZoom; z++) {
            long tilesWide = 2L << z;
            resolutions[z] = 360.0 / (tileSize * tilesWide);
        }
        return resolutions;
    }

    /**
     * Resolution of tiles of {@code tileSize} pixels, from the resolution of 256px tiles at the same level. A 512px
     * tile covers the ground of a 256px tile with twice as many pixels across, and each pixel is half as wide.
     */
    private static double resolutionForTileSize(double resolutionOf256pxTiles, int tileSize) {
        return resolutionOf256pxTiles * 256 / tileSize;
    }
}
