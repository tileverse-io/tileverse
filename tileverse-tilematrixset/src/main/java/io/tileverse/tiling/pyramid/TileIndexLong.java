/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.pyramid;

/**
 * TileIndex implementation using 64-bit longs for coordinates.
 *
 * <p>Used when either X or Y coordinates exceed the int range. Provides full long precision for very large coordinate
 * values.
 *
 * @param x the X coordinate (64-bit)
 * @param y the Y coordinate (64-bit)
 * @param z the zoom level
 * @since 1.0
 */
record TileIndexLong(long xCoord, long yCoord, int zCoord) implements TileIndex {

    // Implement interface methods using record components
    public long x() {
        return xCoord;
    }

    public long y() {
        return yCoord;
    }

    public int z() {
        return zCoord;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TileIndex other && TileIndex.tilesEqual(this, other);
    }

    @Override
    public int hashCode() {
        return TileIndex.tilesHashCode(this);
    }

    @Override
    public String toString() {
        return "TileIndex[z=%d, x=%d, y=%d]".formatted(z(), x(), y());
    }
}
