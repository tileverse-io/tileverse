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
package io.tileverse.tiling.pyramid;

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.matrix.Coordinate;
import io.tileverse.tiling.matrix.Extent;
import java.util.Comparator;
import java.util.Optional;

/**
 * Represents a rectangular range of tiles at a specific zoom level.
 * Combines 2D spatial bounds with zoom level information for complete tile range definition.
 * Provides methods for counting tiles and handling meta-tiles.
 *
 * @since 1.0
 */
public interface TileRange extends Comparable<TileRange> {

    static final Comparator<TileRange> COMPARATOR =
            Comparator.comparing(TileRange::min).thenComparing(TileRange::max);

    /**
     * Returns the zoom level of this tile range.
     *
     * @return the zoom level
     */
    int zoomLevel();

    /**
     * Returns the minimum tile coordinates of this range.
     * Always returns (minx, miny) regardless of axis origin or traversal order.
     * This is a simple accessor for the range's minimum X and Y values.
     *
     * @return the minimum tile coordinates as a TileIndex
     * @see #first() for traversal start position
     */
    default TileIndex min() {
        return TileIndex.of(minx(), miny(), zoomLevel());
    }

    /**
     * Returns the maximum tile coordinates of this range.
     * Always returns (maxx, maxy) regardless of axis origin or traversal order.
     * This is a simple accessor for the range's maximum X and Y values.
     *
     * @return the maximum tile coordinates as a TileIndex
     * @see #last() for traversal end position
     */
    default TileIndex max() {
        return TileIndex.of(maxx(), maxy(), zoomLevel());
    }

    /**
     * Returns the lower-left corner as a TileIndex.
     * The actual coordinates depend on the axis origin:
     * - LOWER_LEFT: (minx, miny)
     * - UPPER_LEFT: (minx, maxy)
     * - LOWER_RIGHT: (maxx, miny)
     * - UPPER_RIGHT: (maxx, maxy)
     *
     * @return the lower-left corner tile index
     */
    default TileIndex lowerLeft() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> TileIndex.of(minx(), miny(), zoomLevel());
            case UPPER_LEFT -> TileIndex.of(minx(), maxy(), zoomLevel());
            case LOWER_RIGHT -> TileIndex.of(maxx(), miny(), zoomLevel());
            case UPPER_RIGHT -> TileIndex.of(maxx(), maxy(), zoomLevel());
        };
    }

    /**
     * Returns the upper-right corner as a TileIndex.
     * The actual coordinates depend on the axis origin:
     * - LOWER_LEFT: (maxx, maxy)
     * - UPPER_LEFT: (maxx, miny)
     * - LOWER_RIGHT: (minx, maxy)
     * - UPPER_RIGHT: (minx, miny)
     *
     * @return the upper-right corner tile index
     */
    default TileIndex upperRight() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> TileIndex.of(maxx(), maxy(), zoomLevel());
            case UPPER_LEFT -> TileIndex.of(maxx(), miny(), zoomLevel());
            case LOWER_RIGHT -> TileIndex.of(minx(), maxy(), zoomLevel());
            case UPPER_RIGHT -> TileIndex.of(minx(), miny(), zoomLevel());
        };
    }

    /**
     * Returns the lower-right corner as a TileIndex.
     * The actual coordinates depend on the axis origin:
     * - LOWER_LEFT: (maxx, miny)
     * - UPPER_LEFT: (maxx, maxy)
     * - LOWER_RIGHT: (minx, miny)
     * - UPPER_RIGHT: (minx, maxy)
     *
     * @return the lower-right corner tile index
     */
    default TileIndex lowerRight() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> TileIndex.of(maxx(), miny(), zoomLevel());
            case UPPER_LEFT -> TileIndex.of(maxx(), maxy(), zoomLevel());
            case LOWER_RIGHT -> TileIndex.of(minx(), miny(), zoomLevel());
            case UPPER_RIGHT -> TileIndex.of(minx(), maxy(), zoomLevel());
        };
    }

    /**
     * Returns the upper-left corner as a TileIndex.
     * The actual coordinates depend on the axis origin:
     * - LOWER_LEFT: (minx, maxy)
     * - UPPER_LEFT: (minx, miny)
     * - LOWER_RIGHT: (maxx, maxy)
     * - UPPER_RIGHT: (maxx, miny)
     *
     * @return the upper-left corner tile index
     */
    default TileIndex upperLeft() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> TileIndex.of(minx(), maxy(), zoomLevel());
            case UPPER_LEFT -> TileIndex.of(minx(), miny(), zoomLevel());
            case LOWER_RIGHT -> TileIndex.of(maxx(), maxy(), zoomLevel());
            case UPPER_RIGHT -> TileIndex.of(maxx(), miny(), zoomLevel());
        };
    }

    /**
     * Returns the minimum X coordinate of the range.
     *
     * @return the minimum X coordinate
     */
    long minx();

    /**
     * Returns the minimum Y coordinate of the range.
     *
     * @return the minimum Y coordinate
     */
    long miny();

    /**
     * Returns the maximum X coordinate of the range.
     *
     * @return the maximum X coordinate
     */
    long maxx();

    /**
     * Returns the maximum Y coordinate of the range.
     *
     * @return the maximum Y coordinate
     */
    long maxy();

    /**
     * Returns the number of tiles spanning the X direction.
     *
     * @return the X span (inclusive count)
     */
    long spanX();

    /**
     * Returns the number of tiles spanning the Y direction.
     *
     * @return the Y span (inclusive count)
     */
    long spanY();

    /**
     * Returns the total number of tiles in the range.
     *
     * @return the total number of tiles in the range
     */
    long count();

    /**
     * Returns the total number of meta-tiles with the specified dimensions.
     *
     * @param width width of each meta-tile in tiles
     * @param height height of each meta-tile in tiles
     * @return the total number of meta-tiles
     */
    long countMetaTiles(int width, int height);

    /**
     * Converts this tile range to a meta-tile range with the specified tile dimensions.
     * The resulting TileRange will cover the same or slightly larger tile space
     * to accommodate partial meta-tiles at the boundaries.
     *
     * @param tilesWide width of each meta-tile in individual tiles
     * @param tilesHigh height of each meta-tile in individual tiles
     * @return a TileRange representing meta-tiles covering this tile range
     * @throws IllegalArgumentException if {@code tilesWide} or {@code tilesHigh} is {@code <= 0}
     */
    default TileRange asMetaTiles(int tilesWide, int tilesHigh) {
        return TileRangeTransforms.toMetaTileRange(this, tilesWide, tilesHigh);
    }

    /**
     * Converts this tile range to individual tile coordinates.
     * For regular TileRange, this expands to all individual tiles within the range.
     * For MetaTileRange, this converts to the equivalent individual tile range.
     *
     * @return a TileRange representing individual tiles
     */
    default TileRange asTiles() {
        return TileRangeTransforms.toTileRange(this);
    }

    /**
     * Returns true if this tile range represents meta-tiles.
     * Regular individual tile ranges return false, while meta-tile views return true.
     *
     * @return true if this is a meta-tile range, false for individual tiles
     */
    default boolean isMetaTiled() {
        return false;
    }

    /**
     * Returns the width of each meta-tile in individual tiles.
     * For regular TileRange, returns 1 (each "meta-tile" is a single tile).
     * For MetaTileRange, returns the actual meta-tile width.
     *
     * @return the meta-tile width in individual tiles
     */
    default int metaWidth() {
        return 1;
    }

    /**
     * Returns the height of each meta-tile in individual tiles.
     * For regular TileRange, returns 1 (each "meta-tile" is a single tile).
     * For MetaTileRange, returns the actual meta-tile height.
     *
     * @return the meta-tile height in individual tiles
     */
    default int metaHeight() {
        return 1;
    }

    /**
     * Returns the first tile in natural traversal order for this axis origin.
     * This is the logical starting point for row-major iteration, which varies by axis origin:
     * - LOWER_LEFT: (minx, miny) - bottom-left corner
     * - UPPER_LEFT: (minx, maxy) - top-left corner
     * - LOWER_RIGHT: (maxx, miny) - bottom-right corner
     * - UPPER_RIGHT: (maxx, maxy) - top-right corner
     *
     * @return the first tile index in traversal order
     * @see #min() for minimum tile coordinates
     */
    default TileIndex first() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> TileIndex.of(minx(), miny(), zoomLevel());
            case UPPER_LEFT -> TileIndex.of(minx(), maxy(), zoomLevel()); // Start from top row
            case LOWER_RIGHT -> TileIndex.of(maxx(), miny(), zoomLevel());
            case UPPER_RIGHT -> TileIndex.of(maxx(), maxy(), zoomLevel()); // Start from top row
        };
    }

    /**
     * Returns the last tile in natural traversal order for this axis origin.
     * This is the logical ending point for row-major iteration, which varies by axis origin:
     * - LOWER_LEFT: (maxx, maxy) - top-right corner
     * - UPPER_LEFT: (maxx, miny) - bottom-right corner
     * - LOWER_RIGHT: (minx, maxy) - top-left corner
     * - UPPER_RIGHT: (minx, miny) - bottom-left corner
     *
     * @return the last tile index in traversal order
     * @see #max() for maximum tile coordinates
     */
    default TileIndex last() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> TileIndex.of(maxx(), maxy(), zoomLevel());
            case UPPER_LEFT -> TileIndex.of(maxx(), miny(), zoomLevel()); // End at bottom row
            case LOWER_RIGHT -> TileIndex.of(minx(), maxy(), zoomLevel());
            case UPPER_RIGHT -> TileIndex.of(minx(), miny(), zoomLevel()); // End at bottom row
        };
    }

    /**
     * Returns the next tile in natural traversal order after the given tile.
     * Uses row-major ordering adapted for the axis origin:
     * - Increments X first (left-to-right or right-to-left based on axis origin)
     * - When X reaches boundary, wraps to next row and resets X
     * - Increments Y in the direction appropriate for axis origin
     *
     * @param current the current tile index
     * @return the next tile index, or empty if current is the last tile or outside range
     */
    default Optional<TileIndex> next(TileIndex current) {
        if (!contains(current)) {
            return Optional.empty();
        }
        return axisOrigin().next(current, this);
    }

    /**
     * Returns the previous tile in natural traversal order before the given tile.
     * Uses row-major ordering adapted for the axis origin in reverse.
     *
     * @param current the current tile index
     * @return the previous tile index, or empty if current is the first tile or outside range
     */
    default Optional<TileIndex> prev(TileIndex current) {
        if (!contains(current)) {
            return Optional.empty();
        }
        return axisOrigin().prev(current, this);
    }

    /**
     * Tests whether this tile range contains the given tile index.
     * The tile must be at the same zoom level and within the coordinate bounds.
     *
     * @param tile the tile index to test
     * @return true if this range contains the tile, false otherwise
     */
    default boolean contains(TileIndex tile) {
        if (tile == null || tile.z() != zoomLevel()) {
            return false;
        }
        long x = tile.x();
        long y = tile.y();
        return x >= minx() && x <= maxx() && y >= miny() && y <= maxy();
    }

    /**
     * Returns the intersection of this tile range with another tile range.
     * The intersection contains only tiles that are present in both ranges.
     * Both ranges must be at the same zoom level.
     *
     * @param other the other tile range to intersect with
     * @return the intersection of the two ranges, or an empty range if no intersection exists
     * @throws IllegalArgumentException if the ranges are at different zoom levels
     */
    default Optional<TileRange> intersection(TileRange other) {
        requireNonNull(other);
        if (other.zoomLevel() != zoomLevel()) {
            throw new IllegalArgumentException(
                    "Cannot intersect ranges at different zoom levels: " + zoomLevel() + " and " + other.zoomLevel());
        }

        long minX = Math.max(minx(), other.minx());
        long minY = Math.max(miny(), other.miny());
        long maxX = Math.min(maxx(), other.maxx());
        long maxY = Math.min(maxy(), other.maxy());

        // Ensure the intersection is valid
        if (minX > maxX || minY > maxY) {
            // No intersection - return empty range using MAX_VALUE coordinates as a marker
            return Optional.empty();
        }

        return Optional.of(TileRange.of(minX, minY, maxX, maxY, zoomLevel(), axisOrigin()));
    }

    /**
     * Returns the axis origin for this tile range.
     * Defines where the (0,0) coordinate is conceptually positioned.
     *
     * @return the axis origin, defaults to LOWER_LEFT for backward compatibility
     */
    default AxisOrigin axisOrigin() {
        return AxisOrigin.LOWER_LEFT;
    }

    /**
     * Returns a view of this tile range with a different axis origin.
     * If coordinate transformation is needed, a new view is created that
     * computes transformed coordinates on-demand.
     *
     * @param targetOrigin the target axis origin
     * @return a TileRange view with the specified axis origin
     */
    default TileRange withAxisOrigin(AxisOrigin targetOrigin) {
        if (this.axisOrigin() == targetOrigin) {
            return this;
        }
        return new AxisOriginView(this, targetOrigin);
    }

    /**
     * Compares this tile range with another for ordering.
     * The natural ordering is:
     * <ol>
     * <li>Primary: zoom level (ascending) - lower zoom levels come first</li>
     * <li>Secondary: minimum X coordinate (ascending)</li>
     * <li>Tertiary: minimum Y coordinate (ascending)</li>
     * <li>Quaternary: maximum X coordinate (ascending)</li>
     * <li>Quinary: maximum Y coordinate (ascending)</li>
     * </ol>
     *
     * This ordering ensures that tile ranges are naturally sorted by zoom level first,
     * then by spatial position from lower-left to upper-right.
     *
     * @param o the tile range to compare with
     * @return a negative integer, zero, or a positive integer as this range is less than,
     *         equal to, or greater than the specified range
     */
    @Override
    default int compareTo(TileRange o) {
        return COMPARATOR.compare(this, o);
    }

    /**
     * Calculates the map space extent covered by this tile range.
     *
     * @param origin the map space coordinate for tile (0,0)
     * @param resolution map units per pixel
     * @param tileWidth tile width in pixels
     * @param tileHeight tile height in pixels
     * @return the map space extent covered by this tile range
     */
    default Extent extent(Coordinate origin, double resolution, int tileWidth, int tileHeight) {
        // Calculate tile size in map units
        double tileMapWidth = tileWidth * resolution;
        double tileMapHeight = tileHeight * resolution;

        double minExtentX, minExtentY, maxExtentX, maxExtentY;

        // Calculate extents based on axis origin
        switch (axisOrigin()) {
            case LOWER_LEFT -> {
                minExtentX = origin.x() + minx() * tileMapWidth;
                minExtentY = origin.y() + miny() * tileMapHeight;
                maxExtentX = origin.x() + (maxx() + 1) * tileMapWidth;
                maxExtentY = origin.y() + (maxy() + 1) * tileMapHeight;
            }
            case UPPER_LEFT -> {
                minExtentX = origin.x() + minx() * tileMapWidth;
                maxExtentY = origin.y() - miny() * tileMapHeight;
                maxExtentX = origin.x() + (maxx() + 1) * tileMapWidth;
                minExtentY = origin.y() - (maxy() + 1) * tileMapHeight;
            }
            case LOWER_RIGHT -> {
                maxExtentX = origin.x() - minx() * tileMapWidth;
                minExtentY = origin.y() + miny() * tileMapHeight;
                minExtentX = origin.x() - (maxx() + 1) * tileMapWidth;
                maxExtentY = origin.y() + (maxy() + 1) * tileMapHeight;
            }
            case UPPER_RIGHT -> {
                maxExtentX = origin.x() - minx() * tileMapWidth;
                maxExtentY = origin.y() - miny() * tileMapHeight;
                minExtentX = origin.x() - (maxx() + 1) * tileMapWidth;
                minExtentY = origin.y() - (maxy() + 1) * tileMapHeight;
            }
            default -> throw new IllegalStateException("Unsupported axis origin: " + axisOrigin());
        }

        return Extent.of(minExtentX, minExtentY, maxExtentX, maxExtentY);
    }

    /**
     * Factory method to create a TileRange from coordinates and zoom level.
     *
     * @param minx minimum X coordinate (inclusive)
     * @param miny minimum Y coordinate (inclusive)
     * @param maxx maximum X coordinate (inclusive)
     * @param maxy maximum Y coordinate (inclusive)
     * @param zoomLevel the zoom level
     * @return a new TileRange instance with LOWER_LEFT axis origin
     */
    static TileRange of(long minx, long miny, long maxx, long maxy, int zoomLevel) {
        return new TileRangeImpl(minx, miny, maxx, maxy, zoomLevel, AxisOrigin.LOWER_LEFT);
    }

    /**
     * Factory method to create a TileRange from coordinates, zoom level and axis origin.
     *
     * @param minx minimum X coordinate (inclusive)
     * @param miny minimum Y coordinate (inclusive)
     * @param maxx maximum X coordinate (inclusive)
     * @param maxy maximum Y coordinate (inclusive)
     * @param zoomLevel the zoom level
     * @param axisOrigin the axis origin
     * @return a new TileRange instance
     */
    static TileRange of(long minx, long miny, long maxx, long maxy, int zoomLevel, AxisOrigin axisOrigin) {
        return new TileRangeImpl(minx, miny, maxx, maxy, zoomLevel, axisOrigin);
    }

    /**
     * Factory method to create a TileRange from corner indices.
     * Both tile indices must have the same zoom level.
     *
     * @param lowerLeft the lower-left corner tile index
     * @param upperRight the upper-right corner tile index
     * @return a new TileRange instance with LOWER_LEFT axis origin
     * @throws IllegalArgumentException if the tile indices have different zoom levels
     */
    static TileRange of(TileIndex lowerLeft, TileIndex upperRight) {
        if (lowerLeft.z() != upperRight.z()) {
            throw new IllegalArgumentException("TileIndex arguments must have same zoom level: lowerLeft.z="
                    + lowerLeft.z() + ", upperRight.z=" + upperRight.z());
        }
        return new TileRangeImpl(
                lowerLeft.x(), lowerLeft.y(), upperRight.x(), upperRight.y(), lowerLeft.z(), AxisOrigin.LOWER_LEFT);
    }

    /**
     * Factory method to create a TileRange from minimum and maximum tile coordinates with axis origin.
     * Both tile indices must have the same zoom level.
     *
     * @param minimum the minimum tile coordinates (minx, miny)
     * @param maximum the maximum tile coordinates (maxx, maxy)
     * @param axisOrigin the axis origin
     * @return a new TileRange instance
     * @throws IllegalArgumentException if the tile indices have different zoom levels
     */
    static TileRange of(TileIndex minimum, TileIndex maximum, AxisOrigin axisOrigin) {
        if (minimum.z() != maximum.z()) {
            throw new IllegalArgumentException("TileIndex arguments must have same zoom level: minimum.z=" + minimum.z()
                    + ", maximum.z=" + maximum.z());
        }
        return new TileRangeImpl(minimum.x(), minimum.y(), maximum.x(), maximum.y(), minimum.z(), axisOrigin);
    }
    /**
     * Static equals method for TileRange instances.
     * Two tile ranges are equal if they represent the same tile space.
     * A MetaTileRange with tilesWide=1 and tilesHigh=1 can be equal to a regular TileRange
     * with the same coordinates and axis origin, as they represent identical tile spaces.
     *
     * @param range1 the first tile range
     * @param range2 the second tile range
     * @return true if the tile ranges are equal
     */
    static boolean equals(TileRange range1, TileRange range2) {
        if (range1 == range2) return true;
        if (range1 == null || range2 == null) return false;

        // Basic coordinate, zoom level, and axis origin equality
        if (range1.zoomLevel() != range2.zoomLevel()
                || range1.minx() != range2.minx()
                || range1.miny() != range2.miny()
                || range1.maxx() != range2.maxx()
                || range1.maxy() != range2.maxy()
                || range1.axisOrigin() != range2.axisOrigin()) {
            return false;
        }

        // Same class types are equal if coordinates and axis origin match and (for MetaTileRange) tile dimensions match
        if (range1.getClass() == range2.getClass()) {
            // For MetaTileRange, also check tile dimensions
            if (range1 instanceof MetaTileRange meta1 && range2 instanceof MetaTileRange meta2) {
                return meta1.tilesWide() == meta2.tilesWide() && meta1.tilesHigh() == meta2.tilesHigh();
            }
            return true;
        }

        // Special case: MetaTileRange with 1x1 tiles equals regular TileRange (same coordinates and axis origin)
        if (range1 instanceof MetaTileRange meta1 && range2 instanceof TileRange) {
            return meta1.tilesWide() == 1 && meta1.tilesHigh() == 1;
        }
        if (range2 instanceof MetaTileRange meta2 && range1 instanceof TileRange) {
            return meta2.tilesWide() == 1 && meta2.tilesHigh() == 1;
        }

        return false;
    }

    /**
     * Static hashCode method for TileRange instances.
     * Computes hash code based on coordinates, zoom level, axis origin, and tile dimensions.
     * MetaTileRange with 1x1 tiles has the same hash code as regular TileRange with same coordinates and axis origin.
     *
     * @param range the tile range
     * @return the hash code
     */
    static int hashCode(TileRange range) {
        if (range == null) return 0;

        int result;
        if (range instanceof MetaTileRange meta) {
            if (meta.tilesWide() == 1 && meta.tilesHigh() == 1) {
                // 1x1 meta-tiles hash the same as regular tiles
                result = TileRangeImpl.class.hashCode();
            } else {
                // Include tile dimensions in hash for non-1x1 meta-tiles
                result = MetaTileRange.class.hashCode();
                result = 31 * result + Integer.hashCode(meta.tilesWide());
                result = 31 * result + Integer.hashCode(meta.tilesHigh());
            }
        } else {
            result = range.getClass().hashCode();
        }

        result = 31 * result + Integer.hashCode(range.zoomLevel());
        result = 31 * result + Long.hashCode(range.minx());
        result = 31 * result + Long.hashCode(range.miny());
        result = 31 * result + Long.hashCode(range.maxx());
        result = 31 * result + Long.hashCode(range.maxy());
        result = 31 * result + range.axisOrigin().hashCode();
        return result;
    }
}
