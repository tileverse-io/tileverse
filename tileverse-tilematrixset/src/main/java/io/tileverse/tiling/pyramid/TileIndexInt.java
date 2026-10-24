/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.pyramid;

/**
 * Memory-optimized TileIndex implementation using 32-bit integers for coordinates.
 *
 * <p>Used when both X and Y coordinates fit within the int range (-2^31 to 2^31-1). This saves 8 bytes per instance
 * compared to the long-based implementation.
 *
 * @param x the X coordinate (32-bit)
 * @param y the Y coordinate (32-bit)
 * @param z the zoom level
 * @since 1.0
 */
record TileIndexInt(int xCoord, int yCoord, int zCoord) implements TileIndex {

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
