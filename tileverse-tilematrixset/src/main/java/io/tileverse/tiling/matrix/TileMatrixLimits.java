/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import static java.util.Objects.requireNonNull;

/**
 * Limits on the {@code tileRow} and {@code tileCol} indices of a specific {@link TileMatrix} within a
 * {@link TileMatrixSet}, as defined by the OGC Two Dimensional Tile Matrix Set standard (clause 7.3).
 *
 * <p>A {@code TileMatrixLimits} narrows a tile matrix to a rectangular subset, expressing the range of tiles that are
 * actually available, populated, or relevant for a specific dataset. The four range values are bounded by the matrix
 * dimensions of the referenced tile matrix.
 *
 * @param tileMatrix the identifier of the referenced tile matrix (must match a {@link TileMatrix#identifier()})
 * @param minTileRow the minimum tile row index (inclusive), {@code 0 <= minTileRow <= maxTileRow}
 * @param maxTileRow the maximum tile row index (inclusive), {@code maxTileRow <= matrixHeight - 1}
 * @param minTileCol the minimum tile column index (inclusive), {@code 0 <= minTileCol <= maxTileCol}
 * @param maxTileCol the maximum tile column index (inclusive), {@code maxTileCol <= matrixWidth - 1}
 * @since 1.4
 * @see <a href="https://docs.ogc.org/is/17-083r2/17-083r2.html#27">OGC 17-083r2 clause 7.3</a>
 */
public record TileMatrixLimits(String tileMatrix, long minTileRow, long maxTileRow, long minTileCol, long maxTileCol) {

    /** Compact constructor enforcing the inter-value constraints of clause 7.3. */
    public TileMatrixLimits {
        requireNonNull(tileMatrix, "tileMatrix identifier cannot be null");
        if (minTileRow < 0) {
            throw new IllegalArgumentException("minTileRow must be >= 0, got " + minTileRow);
        }
        if (minTileCol < 0) {
            throw new IllegalArgumentException("minTileCol must be >= 0, got " + minTileCol);
        }
        if (minTileRow > maxTileRow) {
            throw new IllegalArgumentException(
                    "minTileRow (" + minTileRow + ") must be <= maxTileRow (" + maxTileRow + ")");
        }
        if (minTileCol > maxTileCol) {
            throw new IllegalArgumentException(
                    "minTileCol (" + minTileCol + ") must be <= maxTileCol (" + maxTileCol + ")");
        }
    }

    /** Returns the number of tile rows covered by these limits ({@code maxTileRow - minTileRow + 1}). */
    public long rowSpan() {
        return maxTileRow - minTileRow + 1;
    }

    /** Returns the number of tile columns covered by these limits ({@code maxTileCol - minTileCol + 1}). */
    public long colSpan() {
        return maxTileCol - minTileCol + 1;
    }

    /** Returns the total number of tiles covered by these limits. */
    public long tileCount() {
        return rowSpan() * colSpan();
    }
}
