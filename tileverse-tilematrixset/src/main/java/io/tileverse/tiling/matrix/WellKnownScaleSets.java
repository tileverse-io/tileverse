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

public class WellKnownScaleSets {

    /**
     * This WKSS has been defined for global cartographic products. Rounded scales
     * have been chosen for intuitive cartographic representation of vector data.
     * The scale denominator is only accurate near the Equator.
     */
    public static ScaleSet GlobalCRS84Scale = ScaleSet.builder()
            .uri("http://www.opengis.net/def/wkss/OGC/1.0/GlobalCRS84Scale")
            .crsURI("http://www.opengis.net/def/crs/OGC/1.3/CRS84")
            .pixelSize(ScaleSet.DEFAULT_PIXEL_SIZE)
            .metersPerUnit(111319.49079327358)
            .scaleDenominators(
                    500_000_000,
                    250_000_000,
                    100_000_000,
                    50_000_000,
                    25_000_000,
                    10_000_000,
                    5_000_000,
                    2_500_000,
                    1_000_000,
                    500_000,
                    250_000,
                    100_000,
                    50_000,
                    25_000,
                    10_000,
                    5000,
                    2500,
                    1000,
                    500,
                    250,
                    100)
            .build();

    /**
     * This WKSS has been defined for global cartographic products. Rounded cell
     * sizes have been chosen for intuitive cartographic representation of raster
     * data. Some values have been chosen to coincide with original cell size of
     * commonly used global products like STRM (1” and 3”), GTOPO (30”) or ETOPO (2’
     * and 5’). The scale denominator and approximated cell size in meters are only
     * accurate near the Equator.
     */
    public static ScaleSet GlobalCRS84Pixel = ScaleSet.builder()
            .uri("http://www.opengis.net/def/wkss/OGC/1.0/GlobalCRS84Pixel")
            .crsURI("http://www.opengis.net/def/crs/OGC/1.3/CRS84")
            .pixelSize(ScaleSet.DEFAULT_PIXEL_SIZE)
            .metersPerUnit(111319.49079327358)
            .cellSizes(
                    2.0,
                    1.0,
                    0.5,
                    0.3333333333333333,
                    0.1666666666666667,
                    0.0833333333333333,
                    0.0333333333333333,
                    0.0166666666666667,
                    0.0083333333333333,
                    0.0041666666666667,
                    0.0013888888888889,
                    8.333333333333E-4,
                    2.777777777778E-4,
                    1.388888888889E-4,
                    8.33333333333E-5,
                    2.77777777778E-5,
                    8.3333333333E-6,
                    2.7777777778E-6)
            .build();

    /**
     * This WKSS has been defined to allow quadtree pyramids in CRS84. The scale denominator is only accurate near the equator.
     * <p>
     * <strong>NOTE 1</strong>: The first scale denominator allows representation of the whole world in a single tile
     * of 256×256 cells, where 128 lines of the tile are left blank. The latter is the reason why in the
     * Annex D “World CRS84 Quad TileMatrixSet definition” this level is not used. The next level
     * allows representation of the whole world in 2×1 tiles of 256×256 cells and so on in powers of 2.
     * <p>
     * <strong>NOTE 2</strong>: Selecting the word “Google” for this WKSS id is maintained for backwards compatibility
     * even if the authors recognize that it was an unfortunate selection and might result in confusion
     * since the “Google-like” tiles do not use CRS84.
     */
    public static ScaleSet GoogleCRS84Quad = ScaleSet.builder()
            .uri("http://www.opengis.net/def/wkss/OGC/1.0/GoogleCRS84Quad")
            .crsURI("http://www.opengis.net/def/crs/OGC/1.3/CRS84")
            .pixelSize(ScaleSet.DEFAULT_PIXEL_SIZE)
            .metersPerUnit(111319.49079327358)
            .scaleDenominators(
                    5.590822640287178E8,
                    2.795411320143589E8,
                    1.397705660071794E8,
                    6.988528300358972E7,
                    3.494264150179486E7,
                    1.747132075089743E7,
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
                    2132.729583849784)
            .build();

    /**
     * This well-known scale set (a.k.a {@literal GoogleMapsCompatible}), has been defined to be compatible with many mass marked
     * implementations such as Google Maps, Microsoft Bing Maps (formerly Microsoft Live Maps) and
     * Open Street Map tiles. The scale denominator and cell size are only accurate near the equator.
     * <p>
     * <strong>NOTE</strong>: Level 0 allows representing most of the world (limited to latitudes between
     * approximately +-85 degrees) in a single tile of 256×256 cells (Mercator projection cannot cover
     * the whole world because mathematically the poles are at infinity). The next level represents
     * most of the world in 2×2 tiles of 256×256 cells and so on in powers of 2.
     */
    public static ScaleSet WebMercatorQuad = ScaleSet.builder()
            .uri("http://www.opengis.net/def/wkss/OGC/1.0/GoogleMapsCompatible")
            .crsURI("http://www.opengis.net/def/crs/EPSG/0/3857")
            .pixelSize(ScaleSet.DEFAULT_PIXEL_SIZE)
            .metersPerUnit(1.0)
            .cellSizes(
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
                    0.009330691929342805)
            .build();

    /**
     * This well-known scale set has been defined as similar to
     * {@link #WebMercatorQuad Google Maps} and Microsoft Bing Maps but using
     * the WGS84 ellipsoid. The scale denominator and cell size are only accurate
     * near the equator.
     * <p>
     * <strong>NOTE 1</strong>: Level 0 allows representing most of the world
     * (limited to latitudes between approximately +-85 degrees) in a single tile of
     * 256×256 cells (Mercator projection cannot cover the whole world because
     * mathematically the poles are at infinity). The next level represents most of
     * the world in 2×2 tiles of 256×256 cells and so on in powers of 2.
     * <p>
     * <strong>NOTE 2</strong>: Mercator projection distorts the cell size closer to
     * the poles. The cell sizes provided here are only valid next to the equator.
     * <p>
     * <strong>NOTE 3</strong>: The scales and cell sizes of {@literal WorldMercatorWGS84} and
     * {@linkplain #WebMercatorQuad GoogleMapsCompatible} are identical, but the two WKSS reference a different
     * CRS. This WorldMercatorWGS84 WKSS was introduced in the first version of this
     * standard and not present in the WMTS 1.0.0 specifications Annex E. However,
     * WKSS are obsolete and not required to define a TileMatrixSet, so the introduction
     * of this new WKSS was not necessary to define the WorldMercatorWGS84Quad TileMatrixSet
     */
    public static ScaleSet WorldMercatorWGS84 = WebMercatorQuad.toBuilder()
            .uri("http://www.opengis.net/def/wkss/OGC/1.0/WorldMercatorWGS84")
            .crsURI("http://www.opengis.net/def/crs/EPSG/0/3395")
            .build();
}
