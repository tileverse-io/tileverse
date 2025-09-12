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

import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Represents a rectangular range of tiles at a specific zoom level.
 * <p>
 * Combines 2D spatial bounds with zoom level information for complete tile range definition.
 * Provides methods for counting tiles and traversing tiles.
 * <p>
 * A {@code TileRange} may be <strong>{@linkplain #isSparse() sparse}</strong> as the result of a {@link #union(TileRange)}
 * with another {@code TileRange}.
 * @since 1.0
 */
public sealed interface TileRange extends Comparable<TileRange> permits TileRangeImpl, SparseTileRange {

    static final Comparator<TileRange> COMPARATOR =
            Comparator.comparing(TileRange::min).thenComparing(TileRange::max);

    /**
     * Returns {@code true} if this {@code TileRange} represents a non-contiguous
     * array of {@link TileIndex tile indices}.
     * <p>
     * A sparse {@link TileRange} is composed of two or more tile ranges and covers exactly the union of
     * tile indices of its parts, with the same {@link #cornerOfOrigin()} and {@link #zoomLevel()}.
     * <p>
     * The bound-related method may return tile indices that lay outside the actual tile indices:
     * <ul>
     * <li> {@link #minx()}, {@link #miny()}, {@link #maxx()}, {@link #maxy()}
     * <li> {@link #min()}, {@link #max()}
     * <li> {@link #topLeft()}, {@link #topRight()}, {@link #bottomLeft()}, {@link #bottomRight()}
     * <li> {@link #spanX()}, {@link #spanY()}
     * </ul>
     * The navigation-related methods will though only traverse the tile indices covered by its parts, with no duplication
     * even if the parts intersect themselves:
     * <ul>
     * <li> {@link #first()}, {@link #last()}
     * <li> {@link #next(TileIndex)}, {@link #prev(TileIndex)}
     * <li> {@link #all()}
     * </ul>
     * {@link #count()} will return the actual tile count.
     *
     * @return
     */
    default boolean isSparse() {
        return false;
    }

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
     * <p><strong>Warning:</strong> Do NOT use this method as a starting point for traversal
     * with {@link #next(TileIndex)} or {@link #prev(TileIndex)}. The traversal methods are
     * designed to work with axis-origin-aware positions. Use {@link #first()} instead for
     * the correct forward traversal starting position.
     *
     * @return the minimum tile coordinates as a TileIndex
     * @see #first() for forward traversal start position
     * @see #next(TileIndex) for forward traversal
     */
    default TileIndex min() {
        return TileIndex.xyz(minx(), miny(), zoomLevel());
    }

    /**
     * Returns the maximum tile coordinates of this range.
     * Always returns (maxx, maxy) regardless of axis origin or traversal order.
     * This is a simple accessor for the range's maximum X and Y values.
     *
     * <p><strong>Warning:</strong> Do NOT use this method as a starting point for traversal
     * with {@link #next(TileIndex)} or {@link #prev(TileIndex)}. The traversal methods are
     * designed to work with axis-origin-aware positions. Use {@link #last()} instead for
     * the correct reverse-order traversal starting position.
     *
     * @return the maximum tile coordinates as a TileIndex
     * @see #last() for reverse-order traversal starting position
     * @see #prev(TileIndex) for reverse traversal
     */
    default TileIndex max() {
        return TileIndex.xyz(maxx(), maxy(), zoomLevel());
    }

    /**
     * Returns the lower-left corner as a TileIndex.
     * Delegates to the grid origin for coordinate calculation.
     *
     * @return the lower-left corner tile index
     */
    default TileIndex bottomLeft() {
        return cornerOfOrigin().lowerLeft(this);
    }

    /**
     * Returns the upper-right corner as a TileIndex.
     * Delegates to the grid origin for coordinate calculation.
     *
     * @return the upper-right corner tile index
     */
    default TileIndex topRight() {
        return cornerOfOrigin().upperRight(this);
    }

    /**
     * Returns the lower-right corner as a TileIndex.
     * Delegates to the grid origin for coordinate calculation.
     *
     * @return the lower-right corner tile index
     */
    default TileIndex bottomRight() {
        return cornerOfOrigin().lowerRight(this);
    }

    /**
     * Returns the upper-left corner as a TileIndex.
     * Delegates to the grid origin for coordinate calculation.
     *
     * @return the upper-left corner tile index
     */
    default TileIndex topLeft() {
        return cornerOfOrigin().upperLeft(this);
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
    default long spanX() {
        return maxx() - minx() + 1;
    }

    /**
     * Returns the number of tiles spanning the Y direction.
     *
     * @return the Y span (inclusive count)
     */
    default long spanY() {
        return maxy() - miny() + 1;
    }

    /**
     * Returns the total number of tiles in the range.
     *
     * @return the total number of tiles in the range
     */
    default long count() {
        return Math.multiplyExact(spanX(), spanY());
    }

    /**
     * Returns the first tile in natural traversal order.
     * Always starts from the minimum coordinates (minx, miny) regardless of grid origin.
     * The grid origin affects spatial interpretation, not traversal order.
     *
     * @return the first tile index in traversal order
     * @see #min() for minimum tile coordinates
     */
    default TileIndex first() {
        return TileIndex.xyz(minx(), miny(), zoomLevel());
    }

    /**
     * Returns the last tile in natural traversal order.
     * Always ends at the maximum coordinates (maxx, maxy) regardless of grid origin.
     * The grid origin affects spatial interpretation, not traversal order.
     *
     * @return the last tile index in traversal order
     * @see #max() for maximum tile coordinates
     */
    default TileIndex last() {
        return TileIndex.xyz(maxx(), maxy(), zoomLevel());
    }
    /**
     * Returns the next tile in natural traversal order after the given tile.
     * Uses standard left-to-right, top-to-bottom row-major ordering in grid coordinate space:
     * - Increments X first (left-to-right in coordinate space)
     * - When X reaches maxx, wraps to next row at minx and increments Y
     * - Grid origin affects spatial interpretation, not traversal order
     *
     * @param current the current tile index
     * @return the next tile index, or empty if current is the last tile or outside range
     */
    default Optional<TileIndex> next(TileIndex current) {
        TileIndex next = null;
        if (contains(current)) {
            // Standard left-to-right traversal: increment X first, then wrap to next row at minx
            long nextX = current.x() + 1;
            if (nextX <= maxx()) {
                next = TileIndex.xyz(nextX, current.y(), current.z());
            } else {
                // Wrap to next row
                long nextY = current.y() + 1;
                if (nextY <= maxy()) {
                    next = TileIndex.xyz(minx(), nextY, current.z());
                }
            }
        }
        return Optional.ofNullable(next);
    }

    /**
     * Returns the previous tile in natural traversal order before the given tile.
     * Uses standard left-to-right, top-to-bottom row-major ordering in reverse:
     * - Decrements X first (right-to-left in coordinate space)
     * - When X reaches minx, wraps to previous row at maxx and decrements Y
     * - Grid origin affects spatial interpretation, not traversal order
     *
     * @param current the current tile index
     * @return the previous tile index, or empty if current is the first tile or outside range
     */
    default Optional<TileIndex> prev(TileIndex current) {
        TileIndex prev = null;
        if (contains(current)) {
            // Standard left-to-right reverse: decrement X first, then wrap to prev row at maxx
            long prevX = current.x() - 1;
            if (prevX >= minx()) {
                prev = TileIndex.xyz(prevX, current.y(), current.z());
            } else {
                // Wrap to prev row
                long prevY = current.y() - 1;
                if (prevY >= miny()) {
                    prev = TileIndex.xyz(maxx(), prevY, current.z());
                }
            }
        }
        return Optional.ofNullable(prev);
    }

    default Stream<TileIndex> all() {
        return Stream.iterate(first(), prev -> this.next(prev).orElseThrow()).limit(count());
    }

    /**
     * Tests whether this tile range contains the given tile index.
     * The tile must be at the same zoom level and within the coordinate bounds.
     *
     * @param tile the tile index to test
     * @return true if this range contains the tile, false otherwise
     */
    default boolean contains(TileIndex tile) {
        if (requireNonNull(tile).z() != zoomLevel()) {
            return false;
        }
        long x = tile.x();
        long y = tile.y();
        return x >= minx() && x <= maxx() && y >= miny() && y <= maxy();
    }

    default boolean contains(TileRange range) {
        return contains(range.min()) && contains(range.max());
    }

    /**
     * Returns the intersection of this tile range with another tile range.
     * The intersection contains only tiles that are present in both ranges.
     * Both ranges must be at the same zoom level. If ranges have different grid origins,
     * the other range is automatically transformed to match this range's grid origin.
     *
     * @param other the other tile range to intersect with
     * @return the intersection of the two ranges, or an empty range if no intersection exists
     * @throws IllegalArgumentException if the ranges are at different zoom levels
     */
    default Optional<TileRange> intersection(TileRange other) {
        return TileRangeUtil.intersection(this, other);
    }

    default boolean intersects(TileRange other) {
        if (requireNonNull(other).zoomLevel() != zoomLevel()) {
            return false;
        }
        TileRange compatible = other.withCornerOfOrigin(this.cornerOfOrigin());
        long minX = Math.max(minx(), compatible.minx());
        long minY = Math.max(miny(), compatible.miny());
        long maxX = Math.min(maxx(), compatible.maxx());
        long maxY = Math.min(maxy(), compatible.maxy());

        // Ensure the intersection is valid
        if (minX > maxX || minY > maxY) {
            // No intersection - return empty range using MAX_VALUE coordinates as a marker
            return false;
        }

        return true;
    }

    /**
     * Returns a <strong>possibly sparse</strong> {@link TileRange} that's the union of this range with the {@code other}
     */
    default TileRange union(TileRange other) {
        return TileRangeUtil.union(this, other);
    }

    /**
     * Returns the axis origin for this tile range.
     * Defines where the (0,0) coordinate is conceptually positioned.
     */
    CornerOfOrigin cornerOfOrigin();

    /**
     * Returns a new tile range with a different grid origin.
     * Transforms coordinates to preserve the same spatial area within the zoom level grid.
     * A range representing the "top-left quarter" will remain the top-left quarter
     * after transformation, just expressed in the new coordinate system.
     *
     * @param targetOrigin the target grid origin
     * @return a new TileRange with transformed coordinates and the specified grid origin
     */
    default TileRange withCornerOfOrigin(CornerOfOrigin targetOrigin) {
        if (this.cornerOfOrigin() == targetOrigin) {
            return this;
        }

        // Transform coordinates based on zoom level grid space (2^zoom tiles per side)
        long tilesPerSide = 1L << zoomLevel();
        long maxCoord = tilesPerSide - 1;

        long newMinX, newMinY, newMaxX, newMaxY;

        newMinX = minx();
        newMaxX = maxx();
        if (cornerOfOrigin().needsYFlip(targetOrigin)) {
            newMinY = maxCoord - maxy();
            newMaxY = maxCoord - miny();
        } else {
            newMinY = miny();
            newMaxY = maxy();
        }

        return new TileRangeImpl(newMinX, newMinY, newMaxX, newMaxY, zoomLevel(), targetOrigin);
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
        return new TileRangeImpl(minx, miny, maxx, maxy, zoomLevel, CornerOfOrigin.BOTTOM_LEFT);
    }

    /**
     * Factory method to create a TileRange from coordinates, zoom level and axis origin.
     *
     * @param minx minimum X coordinate (inclusive)
     * @param miny minimum Y coordinate (inclusive)
     * @param maxx maximum X coordinate (inclusive)
     * @param maxy maximum Y coordinate (inclusive)
     * @param zoomLevel the zoom level
     * @param cornerOfOrigin the axis origin
     * @return a new TileRange instance
     */
    static TileRange of(long minx, long miny, long maxx, long maxy, int zoomLevel, CornerOfOrigin cornerOfOrigin) {
        return new TileRangeImpl(minx, miny, maxx, maxy, zoomLevel, cornerOfOrigin);
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
                lowerLeft.x(),
                lowerLeft.y(),
                upperRight.x(),
                upperRight.y(),
                lowerLeft.z(),
                CornerOfOrigin.BOTTOM_LEFT);
    }

    /**
     * Factory method to create a TileRange from minimum and maximum tile coordinates with axis origin.
     * Both tile indices must have the same zoom level.
     *
     * @param minimum the minimum tile coordinates (minx, miny)
     * @param maximum the maximum tile coordinates (maxx, maxy)
     * @param cornerOfOrigin the axis origin
     * @return a new TileRange instance
     * @throws IllegalArgumentException if the tile indices have different zoom levels
     */
    static TileRange of(TileIndex minimum, TileIndex maximum, CornerOfOrigin cornerOfOrigin) {
        if (minimum.z() != maximum.z()) {
            throw new IllegalArgumentException("TileIndex arguments must have same zoom level: minimum.z=" + minimum.z()
                    + ", maximum.z=" + maximum.z());
        }
        return new TileRangeImpl(minimum.x(), minimum.y(), maximum.x(), maximum.y(), minimum.z(), cornerOfOrigin);
    }
    /**
     * Static equals method for TileRange instances.
     * Two tile ranges are equal if they represent the same tile space.
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
                || range1.cornerOfOrigin() != range2.cornerOfOrigin()) {
            return false;
        }

        return true;
    }

    /**
     * Static hashCode method for TileRange instances.
     * Computes hash code based on coordinates, zoom level, axis origin, and tile dimensions.
     *
     * @param range the tile range
     * @return the hash code
     */
    static int hashCode(TileRange range) {
        if (range == null) return 0;

        int result = range.getClass().hashCode();

        result = 31 * result + Integer.hashCode(range.zoomLevel());
        result = 31 * result + Long.hashCode(range.minx());
        result = 31 * result + Long.hashCode(range.miny());
        result = 31 * result + Long.hashCode(range.maxx());
        result = 31 * result + Long.hashCode(range.maxy());
        result = 31 * result + range.cornerOfOrigin().hashCode();
        return result;
    }
}
