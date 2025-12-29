/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.pmtiles;

import io.tileverse.tiling.pyramid.TileIndex;
import java.util.Arrays;

/**
 * High-performance Hilbert curve implementation for PMTiles tile ID encoding/decoding.
 *
 * <p>
 * PMTiles uses a Hilbert curve to map 2D tile coordinates (x, y, z) into a single scalar
 * "Tile ID". This preserves spatial locality: tiles that are close geographically are
 * likely to have close Tile IDs, optimizing storage layout and range requests.
 *
 * <p>
 * <strong>Performance Notes:</strong>
 * <ul>
 * <li>This class is zero-allocation (except for the final {@link TileIndex} result).</li>
 * <li>All coordinate math uses inlined primitive operations to facilitate JIT loop unrolling.</li>
 * <li>Zoom level lookup uses binary search rather than linear scan.</li>
 * </ul>
 * * <p>
 * This implementation assumes {@link TileIndex} will become a Value Object in future JDKs (Project Valhalla).
 */
final class HilbertCurve {

    /**
     * Pre-computed offsets for the start of each zoom level.
     * <p>
     * {@code TZ_VALUES[z]} is the cumulative count of all tiles in levels {@code 0} through {@code z-1}.
     * Effectively, it is the Tile ID of the first tile at zoom level {@code z} (coordinate 0,0).
     * <p>
     * Max supported Zoom: 26.
     */
    private static final long[] TZ_VALUES = {
        0L,
        1L,
        5L,
        21L,
        85L,
        341L,
        1365L,
        5461L,
        21845L,
        87381L,
        349525L,
        1398101L,
        5592405L,
        22369621L,
        89478485L,
        357913941L,
        1431655765L,
        5726623061L,
        22906492245L,
        91625968981L,
        366503875925L,
        1466015503701L,
        5864062014805L,
        23456248059221L,
        93824992236885L,
        375299968947541L,
        1501199875790165L
    };

    /**
     * Decodes a scalar PMTiles Tile ID into (z, x, y) coordinates.
     *
     * <p>
     * This method determines the zoom level via binary search, validates bounds,
     * and performs the inverse Hilbert mapping to reconstruct the 2D position.
     *
     * @param tileId the global PMTiles identifier (must be positive).
     * @return the decoded {@link TileIndex}.
     * @throws IllegalArgumentException if {@code tileId} is negative or exceeds the max zoom limit.
     */
    public TileIndex tileIdToTileIndex(long tileId) {
        if (tileId < 0) {
            throw new IllegalArgumentException("Tile ID cannot be negative: " + tileId);
        }

        // Optimization 1: Binary Search for Zoom Level instead of linear scan.
        // This reduces complexity from O(Z) to O(log Z), crucial for high-throughput lookups.
        int z = Arrays.binarySearch(TZ_VALUES, tileId);
        if (z < 0) {
            // Arrays.binarySearch returns (-(insertion point) - 1).
            // We need the index of the start of the range (the value just <= tileId).
            z = -z - 2;
        }

        checkBounds(tileId, z);

        // Optimization 2: Primitive-only logic (Zero Allocation).
        // Using local variables ensures variables stay on the stack/registers.
        long rx;
        long ry;
        long t;
        long tmp;
        long x = 0;
        long y = 0;

        // Calculate the relative position of the ID within its specific zoom level.
        t = tileId - TZ_VALUES[z];

        // n is the dimension of the grid at this zoom level (2^z).
        // We loop 's' through powers of 2 (1, 2, 4...) until we reach n.
        long n = 1L << z;

        for (long s = 1; s < n; s <<= 1) {
            rx = 1 & (t >> 1); // Extract bit for X component
            ry = 1 & (t ^ rx); // Extract bit for Y component

            // Rotate/Flip quadrant if necessary
            if (ry == 0) {
                if (rx == 1) {
                    x = (s - 1) - x;
                    y = (s - 1) - y;
                }
                // Swap x and y
                tmp = x;
                x = y;
                y = tmp;
            }

            // Accumulate coordinates
            x += s * rx;
            y += s * ry;
            t >>= 2; // Move to next pair of bits
        }

        return TileIndex.xyz(x, y, z);
    }

    /**
     * Validates that the given {@code tileId} actually falls within the range of the detected zoom level {@code z}.
     * * @param tileId the input tile ID.
     * @param z the zoom level detected by binary search.
     */
    private void checkBounds(long tileId, int z) {
        if (z >= TZ_VALUES.length) {
            throw new IllegalArgumentException("Tile ID exceeds max supported zoom limit (26)");
        }

        // Calculate the relative ID (index within the specific zoom level).
        long levelStart = TZ_VALUES[z];
        long relativeId = tileId - levelStart;

        // Max tiles in level z is 4^z.
        // 4^z == (2^2)^z == 2^(2z) == 1 << (2*z).
        // For z=26, 1L << 52 is valid.
        long maxTilesInLevel = 1L << (z * 2);

        if (relativeId >= maxTilesInLevel) {
            throw new IllegalArgumentException("Tile ID " + tileId + " is too large for zoom level " + z);
        }
    }

    /**
     * Encodes (z, x, y) coordinates into a scalar PMTiles Tile ID.
     *
     * <p>
     * This performs the forward Hilbert curve mapping.
     *
     * @param tileIndex the tile coordinates.
     * @return the scalar PMTiles ID.
     * @throws IllegalArgumentException if the zoom level > 26 or x/y are out of bounds.
     */
    public long tileIndexToTileId(TileIndex tileIndex) {
        final int z = tileIndex.z();
        long x = tileIndex.x();
        long y = tileIndex.y();

        if (z > 26) {
            throw new IllegalArgumentException("Zoom level " + z + " exceeds limit (26)");
        }

        final long n = 1L << z; // Equivalent to Math.pow(2, z)
        if (x >= n || y >= n) {
            throw new IllegalArgumentException("x/y out of bounds for zoom level " + z);
        }

        long d = 0;
        long rx;
        long ry;
        long tmp;

        // Iterate from the most significant bit (n/2) down to 1.
        for (long s = n >> 1; s > 0; s >>= 1) {
            rx = (x & s) > 0 ? 1 : 0;
            ry = (y & s) > 0 ? 1 : 0;

            // Update the Hilbert distance 'd' based on the quadrant.
            // Formula: d += s^2 * ((3 * rx) ^ ry)
            d += s * s * ((3 * rx) ^ ry);

            // Rotate/Flip quadrant if necessary to match Hilbert orientation
            if (ry == 0) {
                if (rx == 1) {
                    x = (s - 1) - x;
                    y = (s - 1) - y;
                }
                // Swap x and y
                tmp = x;
                x = y;
                y = tmp;
            }
        }

        return TZ_VALUES[z] + d;
    }
}
