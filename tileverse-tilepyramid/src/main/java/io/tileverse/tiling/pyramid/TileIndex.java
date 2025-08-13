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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Comparator;
import java.util.function.LongUnaryOperator;

/**
 * Represents a 3D tile coordinate (x, y, z) combining spatial position with zoom level.
 * This is an immutable value object that provides coordinate manipulation methods.
 * Orders tiles by zoom level first, then by 2D coordinates.
 *
 * <p>The implementation automatically chooses between memory-optimized variants:
 * uses 32-bit integers when coordinates fit, falls back to 64-bit longs when needed.
 *
 * @since 1.0
 */
public sealed interface TileIndex extends Comparable<TileIndex> permits TileIndexInt, TileIndexLong, MetaTileIndex {

    public static final Comparator<TileIndex> COMPARATOR =
            Comparator.comparingInt(TileIndex::z).thenComparing(TileIndex::x).thenComparing(TileIndex::y);

    /**
     * Factory method to create a TileIndex with the specified coordinates.
     * Automatically chooses the most memory-efficient implementation.
     *
     * @param x the X coordinate
     * @param y the Y coordinate
     * @param z the zoom level
     * @return a new TileIndex instance
     */
    @JsonCreator
    static TileIndex of(@JsonProperty("x") long x, @JsonProperty("y") long y, @JsonProperty("z") int z) {
        if (x >= Integer.MIN_VALUE && x <= Integer.MAX_VALUE && y >= Integer.MIN_VALUE && y <= Integer.MAX_VALUE) {
            return new TileIndexInt((int) x, (int) y, z);
        }
        return new TileIndexLong(x, y, z);
    }

    /**
     * Returns the X coordinate of this tile.
     *
     * @return the X coordinate
     */
    long x();

    /**
     * Returns the Y coordinate of this tile.
     *
     * @return the Y coordinate
     */
    long y();

    /**
     * Returns the zoom level of this tile.
     *
     * @return the zoom level
     */
    int z();

    /**
     * Creates a new TileIndex shifted by the specified amount in the X direction.
     *
     * @param deltaX the amount to shift in the X direction
     * @return a new TileIndex with the shifted X coordinate
     */
    default TileIndex shiftX(long deltaX) {
        return deltaX == 0 ? this : TileIndex.of(x() + deltaX, y(), z());
    }

    /**
     * Creates a new TileIndex shifted by the specified amount in the Y direction.
     *
     * @param deltaY the amount to shift in the Y direction
     * @return a new TileIndex with the shifted Y coordinate
     */
    default TileIndex shiftY(long deltaY) {
        return deltaY == 0 ? this : TileIndex.of(x(), y() + deltaY, z());
    }

    /**
     * Creates a new TileIndex with the specified X coordinate, keeping Y and Z unchanged.
     *
     * @param newX the new X coordinate
     * @return a new TileIndex with the specified X coordinate
     */
    default TileIndex withX(long newX) {
        return newX == x() ? this : TileIndex.of(newX, y(), z());
    }

    /**
     * Creates a new TileIndex with the specified Y coordinate, keeping X and Z unchanged.
     *
     * @param newY the new Y coordinate
     * @return a new TileIndex with the specified Y coordinate
     */
    default TileIndex withY(long newY) {
        return newY == y() ? this : TileIndex.of(x(), newY, z());
    }

    /**
     * Creates a new TileIndex shifted by the specified amounts in both directions.
     *
     * @param deltaX the amount to shift in the X direction
     * @param deltaY the amount to shift in the Y direction
     * @return a new TileIndex with the shifted coordinates
     */
    default TileIndex shiftBy(long deltaX, long deltaY) {
        return TileIndex.of(x() + deltaX, y() + deltaY, z());
    }

    /**
     * Creates a new TileIndex by applying transformation functions to the coordinates.
     *
     * @param xfunction function to transform the X coordinate
     * @param yfunction function to transform the Y coordinate
     * @return a new TileIndex with the transformed coordinates
     */
    default TileIndex shiftBy(LongUnaryOperator xfunction, LongUnaryOperator yfunction) {
        return TileIndex.of(xfunction.applyAsLong(x()), yfunction.applyAsLong(y()), z());
    }

    /**
     * Creates a new TileIndex with a different zoom level but same X,Y coordinates.
     *
     * @param newZ the new zoom level
     * @return a new TileIndex with the specified zoom level
     */
    default TileIndex atZoom(int newZ) {
        return newZ == z() ? this : TileIndex.of(x(), y(), newZ);
    }

    /**
     * Returns the TileRange that represents the area covered by this tile.
     * For regular tiles, this returns a single-tile range.
     * For meta-tiles, this returns the range of all constituent individual tiles.
     *
     * @return a TileRange covering the area represented by this tile
     */
    default TileRange asTileRange() {
        return TileRange.of(x(), y(), x(), y(), z());
    }

    /**
     * Compares this tile index with another, ordering by zoom level first, then by 2D coordinates.
     *
     * @param o the other tile index to compare with
     * @return a negative integer, zero, or a positive integer as this tile index is less than,
     *         equal to, or greater than the specified tile index
     */
    @Override
    default int compareTo(TileIndex o) {
        return COMPARATOR.compare(this, o);
    }

    /**
     * Static helper method for equals implementation.
     * Ensures consistent equality behavior across different TileIndex implementations.
     *
     * @param tile1 the first tile index
     * @param tile2 the index to compare with
     * @return true if the objects are equal, false otherwise
     */
    static boolean tilesEqual(TileIndex tile1, TileIndex tile2) {
        if (tile1 == tile2) return true;
        return tile1.z() == tile2.z() && tile1.x() == tile2.x() && tile1.y() == tile2.y();
    }

    /**
     * Static helper method for hashCode implementation.
     * Ensures consistent hash code behavior across different TileIndex implementations.
     *
     * @param tile the tile index to compute hash code for
     * @return the hash code
     */
    static int tilesHashCode(TileIndex tile) {
        return Long.hashCode(tile.x()) ^ Long.hashCode(tile.y()) ^ Integer.hashCode(tile.z());
    }
}
