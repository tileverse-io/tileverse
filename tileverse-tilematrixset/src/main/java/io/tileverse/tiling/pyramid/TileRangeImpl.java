/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.pyramid;

/**
 * Standard implementation of TileRange representing a rectangular range of individual tiles.
 *
 * @param minx minimum X coordinate (inclusive)
 * @param miny minimum Y coordinate (inclusive)
 * @param maxx maximum X coordinate (inclusive)
 * @param maxy maximum Y coordinate (inclusive)
 * @param zoomLevel the zoom level
 * @param cornerOfOrigin the axis origin
 * @since 1.0
 */
record TileRangeImpl(long minx, long miny, long maxx, long maxy, int zoomLevel, CornerOfOrigin cornerOfOrigin)
        implements TileRange {

    TileRangeImpl {
        if (minx > maxx || miny > maxy) {
            throw new IllegalArgumentException(
                    "Invalid range: min(%d,%d) must be <= max(%d,%d)".formatted(minx, miny, maxx, maxy));
        }
        if (cornerOfOrigin == null) {
            throw new IllegalArgumentException("cornerOfOrigin cannot be null");
        }
    }

    @Override
    public int zoomLevel() {
        return zoomLevel;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TileRange other && TileRange.equals(this, other);
    }

    @Override
    public int hashCode() {
        return TileRange.hashCode(this);
    }

    @Override
    public String toString() {
        return "%s[%d,%d - %d,%d]".formatted(cornerOfOrigin, minx, miny, maxx, maxy);
    }
}
