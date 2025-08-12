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
package io.tileverse.pmtiles;

/**
 * Represents a tile coordinate in the standard z/x/y format.
 * <p>
 * Z represents the zoom level, and X and Y are the column and row coordinates
 * at that zoom level.
 * <p>
 * Note: This class uses the common "XYZ" / "TMS" tile coordinate system where:
 * - X increases from west to east (0 = leftmost/westmost tile)
 * - Y increases from north to south (0 = topmost/northmost tile)
 * - This is sometimes called "Google" or "XYZ" convention (not "TMS" which flips Y)
 * <p>
 * This is important to distinguish from the TMS system where Y is flipped
 * (Y=0 is at the bottom, not the top).
 */
public record ZXY(byte z, int x, int y) {

    /**
     * Creates a new ZXY tile coordinate with int zoom level for convenience.
     * This avoids the need to cast zoom level to byte.
     *
     * @param z the zoom level (0-127, within byte range)
     * @param x the X coordinate
     * @param y the Y coordinate
     * @return a new ZXY instance
     * @throws IllegalArgumentException if the coordinates are outside the valid range or z overflows byte
     */
    public static ZXY of(int z, int x, int y) {
        if (z < Byte.MIN_VALUE || z > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Zoom level " + z + " is outside byte range " + Byte.MIN_VALUE + " to " + Byte.MAX_VALUE);
        }
        return new ZXY((byte) z, x, y);
    }

    /**
     * Validates that the coordinates are valid for the given zoom level.
     * At zoom level z, the valid range for x and y is 0 to 2^z - 1.
     *
     * @throws IllegalArgumentException if the coordinates are outside the valid range
     */
    public ZXY {
        if (z < 0) {
            throw new IllegalArgumentException("Zoom level must be non-negative");
        }

        int maxCoord = (1 << z) - 1;
        if (x < 0 || x > maxCoord) {
            throw new IllegalArgumentException(
                    "X coordinate " + x + " is outside valid range 0 to " + maxCoord + " for zoom level " + z);
        }
        if (y < 0 || y > maxCoord) {
            throw new IllegalArgumentException(
                    "Y coordinate " + y + " is outside valid range 0 to " + maxCoord + " for zoom level " + z);
        }
    }

    /**
     * Converts this tile coordinate to a unique tile ID.
     * <p>
     * The tile ID uniquely identifies a tile across all zoom levels using a Hilbert curve
     * ordering to preserve spatial locality.
     *
     * @return the tile ID
     * @throws IllegalArgumentException if the zoom level is too large
     */
    public long toTileId() {
        if (z > 31) {
            throw new IllegalArgumentException("Tile zoom exceeds 64-bit limit");
        }

        // Accumulate the total number of tiles in all zoom levels before this one
        long acc = 0;
        for (byte t_z = 0; t_z < z; t_z++) {
            long tilesPerZoom = 1L << t_z;
            acc += tilesPerZoom * tilesPerZoom;
        }

        // Convert x,y position to a position on the Hilbert curve
        // Using the direct implementation for better clarity and compatibility
        long d = hilbertXYToIndex(z, x, y);

        // Calculate the final tile ID by adding the accumulated count of tiles
        // from all previous zoom levels to the Hilbert curve index at this zoom level
        long tileId = acc + d;
        return tileId;
    }

    /**
     * Direct implementation of the Hilbert curve calculation, matching the reference implementation.
     * This implementation is mathematically identical to the previous one but more straightforward.
     * It preserves spatial locality which helps with caching and data organization.
     *
     * @param z the zoom level
     * @param x the X coordinate
     * @param y the Y coordinate
     * @return the Hilbert curve index at the given zoom level
     */
    private long hilbertXYToIndex(int z, int x, int y) {
        // Calculate the size of the grid at this zoom level
        int n = 1 << z;

        // Use the reference implementation for simplicity and correctness
        return HilbertCurve.xy2d(n, x, y);
    }

    // The rotate method has been integrated directly into the hilbertXYToIndex method

    /**
     * Converts a tile ID back to a ZXY coordinate.
     *
     * @param tileId the tile ID
     * @return the ZXY coordinate
     * @throws IllegalArgumentException if the tile ID is invalid
     */
    public static ZXY fromTileId(long tileId) {
        if (tileId < 0) {
            throw new IllegalArgumentException("Tile ID must be non-negative");
        }

        // Find which zoom level this tile belongs to
        long acc = 0;
        for (byte z = 0; z < 32; z++) {
            long tilesPerZoom = 1L << z;
            long numTiles = tilesPerZoom * tilesPerZoom;
            if (acc + numTiles > tileId) {
                return tileOnLevel(z, tileId - acc);
            }
            acc += numTiles;
        }

        throw new IllegalArgumentException("Tile ID too large");
    }

    /**
     * Computes the ZXY coordinate for a position on the Hilbert curve at a given zoom level.
     *
     * @param z the zoom level
     * @param pos the position on the Hilbert curve
     * @return the ZXY coordinate
     */
    private static ZXY tileOnLevel(byte z, long pos) {
        // Using direct implementation of Hilbert index to x,y conversion
        int[] coords = hilbertIndexToXY(z, pos);
        return new ZXY(z, coords[0], coords[1]);
    }

    /**
     * Direct implementation of the inverse Hilbert curve calculation.
     * Converts a Hilbert curve index to x,y coordinates.
     *
     * @param z the zoom level
     * @param h the Hilbert curve index
     * @return an array containing [x, y] coordinates
     */
    private static int[] hilbertIndexToXY(int z, long h) {
        // Calculate the size of the grid at this zoom level
        int n = 1 << z;

        // Use the reference implementation for simplicity and correctness
        return HilbertCurve.d2xy(n, h);
    }
}
