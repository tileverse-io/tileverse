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

/**
 * Coalescence factor applied to a contiguous row range of a {@link TileMatrix} to compensate for spatial distortion at
 * high latitudes (for instance in Equirectangular projections), as defined by OGC 17-083r2 clause 7.2.
 *
 * <p>A {@code VariableMatrixWidth} declares that the tiles in rows {@code [minTileRow, maxTileRow]} are coalesced by a
 * factor of {@code coalesce}: each logical column position in those rows is shared by {@code coalesce} contiguous
 * columns of the unconstrained grid. Rows that do not appear in any {@code VariableMatrixWidth} entry are implicitly
 * assigned a coalesce factor of {@code 1}.
 *
 * @param coalesce coalescence factor; must be greater than 1
 * @param minTileRow minimum row index in the affected range (inclusive)
 * @param maxTileRow maximum row index in the affected range (inclusive)
 * @since 1.4
 * @see <a href="https://docs.ogc.org/is/17-083r2/17-083r2.html#26">OGC 17-083r2 clause 7.2</a>
 */
public record VariableMatrixWidth(int coalesce, long minTileRow, long maxTileRow) {

    public VariableMatrixWidth {
        if (coalesce <= 1) {
            throw new IllegalArgumentException("coalesce must be > 1, got " + coalesce);
        }
        if (minTileRow < 0) {
            throw new IllegalArgumentException("minTileRow must be >= 0, got " + minTileRow);
        }
        if (minTileRow > maxTileRow) {
            throw new IllegalArgumentException(
                    "minTileRow (" + minTileRow + ") must be <= maxTileRow (" + maxTileRow + ")");
        }
    }
}
