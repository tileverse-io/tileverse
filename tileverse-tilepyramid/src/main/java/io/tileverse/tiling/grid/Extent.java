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
package io.tileverse.tiling.grid;

/**
 * Represents a 2D bounding box extent in map space (CRS coordinates).
 * This is a simple, GIS-library-agnostic representation that can easily
 * be converted to/from bounding boxes in various GIS libraries.
 *
 * <p>The extent is defined by minimum and maximum coordinate values,
 * representing a rectangular area in the coordinate reference system.
 *
 * @param minX the minimum X coordinate (left edge)
 * @param minY the minimum Y coordinate (bottom edge)
 * @param maxX the maximum X coordinate (right edge)
 * @param maxY the maximum Y coordinate (top edge)
 * @since 1.0
 */
public record Extent(double minX, double minY, double maxX, double maxY) {

    public Extent {
        if (minX > maxX || minY > maxY) {
            throw new IllegalArgumentException(
                    String.format("Invalid extent: min(%.6f,%.6f) must be <= max(%.6f,%.6f)", minX, minY, maxX, maxY));
        }
    }

    /**
     * Creates an extent with the specified bounds.
     *
     * @param minX the minimum X coordinate
     * @param minY the minimum Y coordinate
     * @param maxX the maximum X coordinate
     * @param maxY the maximum Y coordinate
     * @return a new Extent instance
     * @throws IllegalArgumentException if min > max for any dimension
     */
    public static Extent of(double minX, double minY, double maxX, double maxY) {
        return new Extent(minX, minY, maxX, maxY);
    }

    /**
     * Creates an extent from two corner coordinates.
     * The coordinates don't need to be in min/max order - this method
     * will determine the correct bounds.
     *
     * @param corner1 the first corner coordinate
     * @param corner2 the second corner coordinate
     * @return a new Extent instance
     */
    public static Extent of(Coordinate corner1, Coordinate corner2) {
        return new Extent(
                Math.min(corner1.x(), corner2.x()),
                Math.min(corner1.y(), corner2.y()),
                Math.max(corner1.x(), corner2.x()),
                Math.max(corner1.y(), corner2.y()));
    }

    /**
     * Creates an extent centered on a coordinate with the specified width and height.
     *
     * @param center the center coordinate
     * @param width the width of the extent
     * @param height the height of the extent
     * @return a new Extent instance
     * @throws IllegalArgumentException if width or height is negative
     */
    public static Extent centered(Coordinate center, double width, double height) {
        if (width < 0 || height < 0) {
            throw new IllegalArgumentException("Width and height must be non-negative");
        }
        double halfWidth = width / 2.0;
        double halfHeight = height / 2.0;
        return new Extent(
                center.x() - halfWidth, center.y() - halfHeight, center.x() + halfWidth, center.y() + halfHeight);
    }

    /**
     * Returns the width of this extent (maxX - minX).
     *
     * @return the width
     */
    public double width() {
        return maxX - minX;
    }

    /**
     * Returns the height of this extent (maxY - minY).
     *
     * @return the height
     */
    public double height() {
        return maxY - minY;
    }

    /**
     * Returns the area of this extent (width * height).
     *
     * @return the area
     */
    public double area() {
        return width() * height();
    }

    /**
     * Returns the center coordinate of this extent.
     *
     * @return the center coordinate
     */
    public Coordinate center() {
        return new Coordinate((minX + maxX) / 2.0, (minY + maxY) / 2.0);
    }

    /**
     * Returns the minimum coordinate (lower-left corner).
     *
     * @return the minimum coordinate
     */
    public Coordinate min() {
        return new Coordinate(minX, minY);
    }

    /**
     * Returns the maximum coordinate (upper-right corner).
     *
     * @return the maximum coordinate
     */
    public Coordinate max() {
        return new Coordinate(maxX, maxY);
    }

    /**
     * Tests whether this extent contains the specified coordinate.
     *
     * @param coordinate the coordinate to test
     * @return true if the coordinate is within this extent (inclusive)
     */
    public boolean contains(Coordinate coordinate) {
        return coordinate.x() >= minX && coordinate.x() <= maxX && coordinate.y() >= minY && coordinate.y() <= maxY;
    }

    /**
     * Tests whether this extent intersects with another extent.
     *
     * @param other the other extent
     * @return true if the extents intersect
     */
    public boolean intersects(Extent other) {
        return !(other.maxX < minX || other.minX > maxX || other.maxY < minY || other.minY > maxY);
    }

    /**
     * Returns the intersection of this extent with another extent.
     *
     * @param other the other extent
     * @return the intersection extent, or null if they don't intersect
     */
    public Extent intersection(Extent other) {
        if (!intersects(other)) {
            return null;
        }
        return new Extent(
                Math.max(minX, other.minX),
                Math.max(minY, other.minY),
                Math.min(maxX, other.maxX),
                Math.min(maxY, other.maxY));
    }

    /**
     * Returns the union of this extent with another extent.
     *
     * @param other the other extent
     * @return the union extent containing both extents
     */
    public Extent union(Extent other) {
        return new Extent(
                Math.min(minX, other.minX),
                Math.min(minY, other.minY),
                Math.max(maxX, other.maxX),
                Math.max(maxY, other.maxY));
    }

    /**
     * Returns this extent expanded by the specified amounts in all directions.
     *
     * @param amount the amount to expand (can be negative to shrink)
     * @return the expanded extent
     */
    public Extent expand(double amount) {
        return new Extent(minX - amount, minY - amount, maxX + amount, maxY + amount);
    }

    @Override
    public String toString() {
        return String.format("Extent(%.6f, %.6f, %.6f, %.6f)", minX, minY, maxX, maxY);
    }
}
