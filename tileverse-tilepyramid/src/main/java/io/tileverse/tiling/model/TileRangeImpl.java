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
package io.tileverse.tiling.model;

/**
 * Standard implementation of TileRange representing a rectangular range of individual tiles.
 *
 * @param minx minimum X coordinate (inclusive)
 * @param miny minimum Y coordinate (inclusive)
 * @param maxx maximum X coordinate (inclusive)
 * @param maxy maximum Y coordinate (inclusive)
 * @param zoomLevel the zoom level
 * @param axisOrigin the axis origin
 * @since 1.0
 */
record TileRangeImpl(long minx, long miny, long maxx, long maxy, int zoomLevel, AxisOrigin axisOrigin)
        implements TileRange {

    TileRangeImpl {
        if (minx > maxx || miny > maxy) {
            throw new IllegalArgumentException(
                    String.format("Invalid range: min(%d,%d) must be <= max(%d,%d)", minx, miny, maxx, maxy));
        }
        if (axisOrigin == null) {
            throw new IllegalArgumentException("axisOrigin cannot be null");
        }
    }

    @Override
    public long spanX() {
        return maxx - minx + 1;
    }

    @Override
    public long spanY() {
        return maxy - miny + 1;
    }

    @Override
    public long count() {
        return Math.multiplyExact(spanX(), spanY());
    }

    @Override
    public long countMetaTiles(int width, int height) {
        if (width <= 0) throw new IllegalArgumentException("width must be > 0");
        if (height <= 0) throw new IllegalArgumentException("height must be > 0");

        long metaX = countMetatiles(spanX(), width);
        long metaY = countMetatiles(spanY(), height);
        return Math.multiplyExact(metaX, metaY);
    }

    @Override
    public int zoomLevel() {
        return zoomLevel;
    }

    private long countMetatiles(long ntiles, int metaSize) {
        long rem = ntiles % metaSize;
        long metas = ntiles / metaSize;
        return (rem > 0 ? 1 : 0) + metas;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TileRange other && TileRange.equals(this, other);
    }

    @Override
    public int hashCode() {
        return TileRange.hashCode(this);
    }
}
