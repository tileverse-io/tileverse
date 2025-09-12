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

import io.tileverse.tiling.common.BoundingBox2D;
import io.tileverse.tiling.common.Coordinate;
import io.tileverse.tiling.pyramid.TileIndex;

/**
 * A tile represents a rectangular region of map data with specific dimensions and spatial extent.
 * It combines the discrete grid coordinates ({@link TileIndex}) with the spatial properties
 * needed for rendering and coordinate transformations.
 *
 * <p>Key properties:
 * <ul>
 * <li><b>Tile Index</b>: The discrete grid coordinates (X, Y, Z)</li>
 * <li><b>Extent</b>: The map space bounding box covered by this tile</li>
 * <li><b>Dimensions</b>: The pixel width and height of the tile</li>
 * <li><b>Resolution</b>: Map units per pixel for this tile</li>
 * <li><b>CRS</b>: Coordinate reference system identifier</li>
 * </ul>
 *
 * <p>Tiles are immutable value objects that can be used for:
 * <ul>
 * <li>Spatial queries and intersection testing</li>
 * <li>Coordinate transformations between pixel and map space</li>
 * <li>Tile-based rendering and caching operations</li>
 * <li>Geometric operations on rectangular tile extents</li>
 * </ul>
 *
 * @param tileIndex the discrete grid coordinates for this tile
 * @param extent the map space bounding box covered by this tile
 * @param width the tile width in pixels
 * @param height the tile height in pixels
 * @param resolution map units per pixel
 * @param crsId coordinate reference system identifier
 *
 * @since 1.0
 */
public record Tile(TileIndex tileIndex, BoundingBox2D extent, int width, int height, double resolution, String crsId) {

    /**
     * Compact constructor with validation.
     */
    public Tile {
        if (tileIndex == null) {
            throw new IllegalArgumentException("tileIndex cannot be null");
        }
        if (extent == null) {
            throw new IllegalArgumentException("extent cannot be null");
        }
        if (crsId == null || crsId.trim().isEmpty()) {
            throw new IllegalArgumentException("crsId cannot be null or empty");
        }
        if (width <= 0 || height <= 0) {
            throw new IllegalArgumentException("tile dimensions must be positive");
        }
        if (resolution <= 0) {
            throw new IllegalArgumentException("resolution must be positive");
        }
    }

    /**
     * Returns the X coordinate of this tile.
     *
     * @return the tile X coordinate
     */
    public long x() {
        return tileIndex.x();
    }

    /**
     * Returns the Y coordinate of this tile.
     *
     * @return the tile Y coordinate
     */
    public long y() {
        return tileIndex.y();
    }

    /**
     * Returns the zoom level of this tile.
     *
     * @return the tile zoom level
     */
    public int z() {
        return tileIndex.z();
    }

    /**
     * Returns true if this tile contains the specified map space coordinate.
     *
     * @param coordinate the map space coordinate to test
     * @return true if the coordinate is within this tile's extent
     */
    public boolean contains(Coordinate coordinate) {
        return extent.contains(coordinate);
    }

    /**
     * Returns true if this tile intersects with the given extent.
     *
     * @param otherExtent the extent to test for intersection
     * @return true if this tile intersects with the extent
     */
    public boolean intersects(BoundingBox2D otherExtent) {
        return extent.intersects(otherExtent);
    }

    /**
     * Converts a map space coordinate to pixel coordinates within this tile.
     * The returned coordinate has its origin at the upper-left corner of the tile,
     * with X increasing to the right and Y increasing downward.
     *
     * @param mapCoordinate the map space coordinate
     * @return the pixel coordinate within this tile, or null if outside tile bounds
     */
    public Coordinate mapToPixel(Coordinate mapCoordinate) {
        if (!contains(mapCoordinate)) {
            return null;
        }

        // Convert map coordinate to pixel coordinate relative to tile's upper-left corner
        double pixelX = (mapCoordinate.x() - extent.minX()) / resolution;
        double pixelY = (extent.maxY() - mapCoordinate.y()) / resolution;

        // Clamp to tile bounds
        pixelX = Math.max(0, Math.min(width - 1, pixelX));
        pixelY = Math.max(0, Math.min(height - 1, pixelY));

        return Coordinate.of(pixelX, pixelY);
    }

    /**
     * Converts a pixel coordinate within this tile to map space coordinates.
     * The pixel coordinate should have its origin at the upper-left corner of the tile.
     *
     * @param pixelCoordinate the pixel coordinate within this tile
     * @return the corresponding map space coordinate
     * @throws IllegalArgumentException if pixel coordinate is outside tile bounds
     */
    public Coordinate pixelToMap(Coordinate pixelCoordinate) {
        if (pixelCoordinate.x() < 0
                || pixelCoordinate.x() >= width
                || pixelCoordinate.y() < 0
                || pixelCoordinate.y() >= height) {
            throw new IllegalArgumentException("Pixel coordinate " + pixelCoordinate + " is outside tile bounds [0,0,"
                    + width + "," + height + "]");
        }

        // Convert pixel coordinate to map coordinate
        double mapX = extent.minX() + pixelCoordinate.x() * resolution;
        double mapY = extent.maxY() - pixelCoordinate.y() * resolution;

        return Coordinate.of(mapX, mapY);
    }

    /**
     * Returns the center coordinate of this tile in map space.
     *
     * @return the center coordinate of this tile
     */
    public Coordinate center() {
        return extent.center();
    }

    /**
     * Returns the area covered by this tile in map units squared.
     *
     * @return the tile area in map units squared
     */
    public double area() {
        return extent.area();
    }

    /**
     * Returns a string representation of this tile showing its key properties.
     *
     * @return a string representation of this tile
     */
    @Override
    public String toString() {
        return String.format(
                "Tile[%s, extent=%s, %dx%d, res=%.3f, %s]", tileIndex, extent, width, height, resolution, crsId);
    }

    /**
     * Creates a new builder for Tile.
     *
     * @return a new builder instance
     */
    public static TileBuilder builder() {
        return new TileBuilder();
    }
}
