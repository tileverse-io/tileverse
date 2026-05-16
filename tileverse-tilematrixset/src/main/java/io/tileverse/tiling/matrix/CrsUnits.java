/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

/**
 * CRS unit helpers used by the OGC Two Dimensional Tile Matrix Set scale-denominator/resolution algebra.
 *
 * <p>The {@link #metersPerUnit(String)} coefficient converts a CRS axis unit into meters, as defined by OGC 17-083r2
 * clause 8 (Annex B). The conversion lets resolutions in CRS units be related to the standard rendering pixel size
 * (0.28 mm) used by the scale denominator formulas.
 *
 * <p>Only the CRS families used by the bundled well-known tile matrix sets are supported by name. Custom CRS
 * identifiers fall back to {@code 1.0} (a meters assumption), which matches the OGC convention for projected CRS unless
 * the caller substitutes a more accurate coefficient.
 *
 * @since 1.4
 */
public final class CrsUnits {

    /** Standard OGC rendering pixel size in meters (0.28 mm). */
    public static final double STANDARD_PIXEL_SIZE_METERS = 0.28e-3;

    private static final double WGS84_EQUATORIAL_RADIUS_METERS = 6378137.0;

    /** Meters per degree of arc at the equator on the WGS84 ellipsoid ({@code 2 * PI * a / 360}). */
    public static final double METERS_PER_DEGREE = 2.0 * Math.PI * WGS84_EQUATORIAL_RADIUS_METERS / 360.0;

    private CrsUnits() {
        // utility
    }

    /**
     * Returns the conversion coefficient from the units of the given CRS to meters.
     *
     * <p>Recognized CRS identifier patterns:
     *
     * <ul>
     *   <li>EPSG:4326, EPSG:4979, CRS:84, OGC:CRS84 - geographic, returns {@link #METERS_PER_DEGREE}
     *   <li>EPSG:3857, EPSG:900913, EPSG:3395 - projected meters, returns {@code 1.0}
     *   <li>Other identifiers - returns {@code 1.0} (projected-meters assumption)
     * </ul>
     *
     * @param crsId CRS identifier (URI fragment or short form such as {@code "EPSG:3857"})
     * @return meters per CRS unit
     */
    public static double metersPerUnit(String crsId) {
        if (crsId == null) {
            return 1.0;
        }
        String normalized = crsId.toUpperCase();
        if (normalized.contains("4326")
                || normalized.contains("4979")
                || normalized.contains("CRS84")
                || normalized.contains("CRS:84")) {
            return METERS_PER_DEGREE;
        }
        return 1.0;
    }

    /**
     * Converts a resolution in CRS units per pixel to the OGC scale denominator using the standard 0.28 mm rendering
     * pixel size.
     *
     * @param resolution map units per pixel
     * @param crsId CRS identifier
     * @return scale denominator (dimensionless ratio)
     */
    public static double resolutionToScaleDenominator(double resolution, String crsId) {
        return resolution * metersPerUnit(crsId) / STANDARD_PIXEL_SIZE_METERS;
    }

    /**
     * Converts an OGC scale denominator to a resolution in CRS units per pixel.
     *
     * @param scaleDenominator scale denominator
     * @param crsId CRS identifier
     * @return CRS units per pixel
     */
    public static double scaleDenominatorToResolution(double scaleDenominator, String crsId) {
        return scaleDenominator * STANDARD_PIXEL_SIZE_METERS / metersPerUnit(crsId);
    }
}
