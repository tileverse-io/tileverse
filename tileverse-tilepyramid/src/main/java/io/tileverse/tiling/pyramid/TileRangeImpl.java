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

import io.tileverse.tiling.common.CornerOfOrigin;

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
                    String.format("Invalid range: min(%d,%d) must be <= max(%d,%d)", minx, miny, maxx, maxy));
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
