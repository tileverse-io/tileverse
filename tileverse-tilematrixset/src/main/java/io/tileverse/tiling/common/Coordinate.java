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
package io.tileverse.tiling.common;

/**
 * Represents a 2D coordinate in map space (CRS coordinates).
 * This is a simple, GIS-library-agnostic representation that can easily
 * be converted to/from coordinates in various GIS libraries.
 *
 * <p>The coordinate values can represent:
 * <ul>
 * <li>Geographic coordinates (longitude, latitude) in degrees</li>
 * <li>Projected coordinates (easting, northing) in meters or other units</li>
 * <li>Any other 2D coordinate system values</li>
 * </ul>
 *
 * @param x the X coordinate (typically longitude or easting)
 * @param y the Y coordinate (typically latitude or northing)
 * @since 1.0
 */
public record Coordinate(double x, double y) {

    /**
     * Creates a coordinate with the specified X and Y values.
     *
     * @param x the X coordinate
     * @param y the Y coordinate
     * @return a new Coordinate instance
     */
    public static Coordinate of(double x, double y) {
        return new Coordinate(x, y);
    }

    /**
     * Returns a coordinate offset by the specified amounts.
     *
     * @param deltaX the X offset
     * @param deltaY the Y offset
     * @return a new coordinate offset by the specified amounts
     */
    public Coordinate offset(double deltaX, double deltaY) {
        return new Coordinate(x + deltaX, y + deltaY);
    }

    /**
     * Returns the distance to another coordinate using Euclidean distance.
     * This is appropriate for projected coordinate systems but not for
     * geographic coordinates (longitude/latitude).
     *
     * @param other the other coordinate
     * @return the Euclidean distance
     */
    public double distanceTo(Coordinate other) {
        double dx = x - other.x;
        double dy = y - other.y;
        return Math.sqrt(dx * dx + dy * dy);
    }

    @Override
    public String toString() {
        return String.format("Coordinate(%.6f, %.6f)", x, y);
    }
}
