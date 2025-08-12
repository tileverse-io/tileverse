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

/**
 * TileIndex implementation using 64-bit longs for coordinates.
 * <p>
 * Used when either X or Y coordinates exceed the int range.
 * Provides full long precision for very large coordinate values.
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
}
