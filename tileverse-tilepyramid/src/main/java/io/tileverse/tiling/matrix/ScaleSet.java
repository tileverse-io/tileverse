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

import java.util.List;

/**
 * For the case of a two-dimensional space, given the top left point of the tile
 * matrix in CRS coordinates ({@code tileMatrixMinX, tileMatrixMaxY}), the width
 * and height of the tile matrix in tile units
 * ({@linkplain TileMatrix#matrixWidth() matrixWidth},
 * {@linkplain TileMatrix#matrixHeight() matrixHeight}), the rendering cells in
 * a tile values ({@linkplain TileMatrix#tileWidth() tileWidth},
 * {@linkplain TileMatrix#tileHeight() tileHeight}), the coefficient to convert
 * the coordinate reference system (CRS) units into meters
 * ({@code metersPerUnit}), and the scale ({@code 1:scaleDenominator}), the
 * bottom right corner of the bounding box of a tile matrix (tileMatrixMaxX,
 * tileMatrixMinY) can be calculated as follows:
 *
 * <pre>
 * {@code
 *   cellSize = scaleDenominator x 0.28 10^(-3) // metersPerUnit(crs)
 *   tileSpanX = tileWidth x cellSize
 *   tileSpanY = tileHeight x cellSize
 *   tileMatrixMaxX = tileMatrixMinX + tileSpanX x matrixWidth
 *   tileMatrixMinY = tileMatrixMaxY - tileSpanY x matrixHeight
 * }</pre>
 * <p>
 * In a CRS with coordinates expressed in meters, {@code metersPerUnit(crs)}
 * equals 1
 * <p>
 * In a CRS with coordinates expressed in degrees {@code metersPerUnit(crs)}
 * equals {@code  360 / (EquatorialRadius * 2 * PI)} (360 degrees are
 * equivalent to the EquatorialPerimeter). E.g for WGS84
 * {@code metersPerUnit(crs)} is {@code 111319.4908} meters/degree
 *
 * @see https://docs.ogc.org/is/17-083r4/17-083r4.pdf
 */
public record ScaleSet(double metersPerUnit, List<Scale> scales) {

    /**
     * A "standardized rendering pixel size" of
     * {@code 0.28mm x 0.28mm} (millimeters). The definition is the same as used in Web Map Service WMS 1.3.0 OGC 06-042
     * and in the Symbology Encoding (SE) Implementation Specification 1.1.0 OGC 05-077r4 that was later adopted by
     * WMTS 1.0 OGC 07-057r7. Frequently, the true pixel size of the device is unknown and 0.28 mm was the actual pixel
     * size of a common display from 2005.
     * <p>
     * This value is still being used as reference, even if current display devices are built with much smaller pixel sizes.
     */
    public static final double DEFAULT_PIXEL_SIZE = 2.8E-4;

    public static record Scale(double denominator, double resolution) {}

    public ScaleSet(double metersPerUnit, List<Scale> scales) {
        this.metersPerUnit = metersPerUnit;
        this.scales = List.copyOf(scales); // immutable and prevents nulls automatically
    }

    public static ScaleSetBuilder builder() {
        return new ScaleSetBuilder();
    }

    public ScaleSetBuilder toBuilder() {
        return new ScaleSetBuilder(this);
    }

    public static class ScaleSetBuilder {

        public ScaleSetBuilder() {}

        public ScaleSetBuilder(ScaleSet scaleSet) {
            // TODO Auto-generated constructor stub
        }

        /**
         * Scale denominators for this scale set. If not set, will be computed from
         * {@link #cellSizes(double...)}. If both are set, both arrays must have the same length
         *
         * @param denominators
         * @return
         */
        public ScaleSetBuilder scaleDenominators(double... denominators) {
            // TODO Auto-generated method stub
            return this;
        }

        /**
         * Cell sizes (a.k.a. resolutions) for this scale set. If not set, will be computed from
         * {@link #scaleDenominators(double...)}. If both are set, both arrays must have the same length
         *
         * @param cellSizes (A.K.A. resolutions), size of a single cell (pixel) in CRS units
         * @return
         */
        public ScaleSetBuilder cellSizes(double... cellSizes) {
            return null;
        }

        public ScaleSetBuilder crsURI(String string) {
            // TODO Auto-generated method stub
            return this;
        }

        public ScaleSet build() {
            // TODO Auto-generated method stub
            return null;
        }

        public ScaleSetBuilder pixelSize(double d) {
            // TODO Auto-generated method stub
            return this;
        }

        public ScaleSetBuilder metersPerUnit(double d) {
            // TODO Auto-generated method stub
            return null;
        }

        public ScaleSetBuilder uri(String string) {
            // TODO Auto-generated method stub
            return null;
        }
    }
}
