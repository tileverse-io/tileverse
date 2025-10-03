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

import io.tileverse.tiling.pyramid.TileIndex;

/**
 * Hilbert curve algorithms for PMTiles tile ID encoding/decoding.
 *
 * <p>PMTiles uses a Hilbert curve to convert 2D tile coordinates (x,y,z) into
 * a single tile ID that preserves spatial locality. This implementation provides
 * the conversion methods needed for PMTiles compatibility.
 *
 * <p>The implementation includes both the standard reference algorithm and an
 * optimized version based on the BareMaps/flatgeobuf approach.
 */
class HilbertCurve {

    /**
     * Pre-computed values for tile count accumulation across zoom levels.
     * TZ_VALUES[z] represents the total number of tiles in zoom levels 0 through z-1.
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

    private HilbertCurve() {
        // Utility class
    }

    /**
     * Converts tile coordinates to a PMTiles tile ID using Hilbert curve encoding.
     *
     * @param tileIndex the tile coordinates
     * @return the PMTiles tile ID
     * @throws IllegalArgumentException if the zoom level exceeds limits
     */
    public static long tileIndexToTileId(TileIndex tileIndex) {
        return zxyToTileId(tileIndex.z(), tileIndex.x(), tileIndex.y());
    }

    /**
     * Converts a PMTiles tile ID back to tile coordinates using Hilbert curve decoding.
     *
     * @param tileId the PMTiles tile ID
     * @return the tile coordinates
     * @throws IllegalArgumentException if the tile ID is invalid
     */
    public static TileIndex tileIdToTileIndex(long tileId) {
        long[] zxy = tileIdToZxy(tileId);
        return TileIndex.xyz(zxy[1], zxy[2], (int) zxy[0]);
    }

    /**
     * Converts z,x,y coordinates to a PMTiles tile ID using Hilbert curve encoding.
     * This matches the PMTiles specification exactly.
     *
     * @param z the zoom level
     * @param x the X coordinate
     * @param y the Y coordinate
     * @return the PMTiles tile ID
     * @throws IllegalArgumentException if coordinates are invalid
     */
    public static long zxyToTileId(int z, long x, long y) {
        if (z > 26) {
            throw new IllegalArgumentException("Tile zoom level exceeds max safe number limit (26)");
        }
        if (x > Math.pow(2, z) - 1 || y > Math.pow(2, z) - 1) {
            throw new IllegalArgumentException("tile x/y outside zoom level bounds");
        }

        long acc = TZ_VALUES[z];
        long n = 1L << z;
        long rx, ry, d = 0;
        long[] xy = {x, y};

        for (long s = n / 2; s > 0; s /= 2) {
            rx = (xy[0] & s) > 0 ? 1 : 0;
            ry = (xy[1] & s) > 0 ? 1 : 0;
            d += s * s * ((3 * rx) ^ ry);
            rotate(s, xy, rx, ry);
        }

        return acc + d;
    }

    /**
     * Converts a PMTiles tile ID back to z,x,y coordinates.
     *
     * @param tileId the PMTiles tile ID
     * @return array containing [z, x, y]
     * @throws IllegalArgumentException if tile ID is invalid
     */
    public static long[] tileIdToZxy(long tileId) {
        if (tileId < 0) {
            throw new IllegalArgumentException("Tile ID cannot be negative: " + tileId);
        }

        long acc = 0;
        for (int z = 0; z < 27; z++) {
            long numTiles = (1L << z) * (1L << z);
            if (acc + numTiles > tileId) {
                return idOnLevel(z, tileId - acc);
            }
            acc += numTiles;
        }
        throw new IllegalArgumentException("Tile zoom level exceeds max safe number limit (26)");
    }

    /**
     * Convert a position to z, x, y coordinates at a specific zoom level.
     *
     * @param z the zoom level
     * @param pos the position on the Hilbert curve
     * @return array containing [z, x, y]
     */
    private static long[] idOnLevel(int z, long pos) {
        long n = 1L << z;
        long rx, ry, t = pos;
        long[] xy = {0, 0};

        for (long s = 1; s < n; s *= 2) {
            rx = 1 & (t / 2);
            ry = 1 & (t ^ rx);
            rotate(s, xy, rx, ry);
            xy[0] += s * rx;
            xy[1] += s * ry;
            t /= 4;
        }

        return new long[] {z, xy[0], xy[1]};
    }

    /**
     * Rotate coordinates using Hilbert curve rotation.
     * This method properly modifies the coordinates in place.
     *
     * @param n the size of the quadrant
     * @param xy the coordinates to rotate (modified in place)
     * @param rx the x transform
     * @param ry the y transform
     */
    private static void rotate(long n, long[] xy, long rx, long ry) {
        if (ry == 0) {
            if (rx == 1) {
                xy[0] = n - 1 - xy[0];
                xy[1] = n - 1 - xy[1];
            }
            // Swap x and y
            long t = xy[0];
            xy[0] = xy[1];
            xy[1] = t;
        }
    }
}
