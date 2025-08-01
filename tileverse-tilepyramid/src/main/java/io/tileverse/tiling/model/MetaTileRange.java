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
 * A view of a TileRange that interprets it as meta-tiles.
 * Each meta-tile represents a rectangular group of individual tiles.
 * This is a view wrapper that computes meta-tile coordinates on-demand from the source TileRange.
 *
 * @since 1.0
 */
class MetaTileRange implements TileRange {

    private final TileRange source;
    private final int tilesWide;
    private final int tilesHigh;

    private MetaTileRange(TileRange source, int tilesWide, int tilesHigh) {
        if (tilesWide <= 0) {
            throw new IllegalArgumentException("tilesWide must be > 0");
        }
        if (tilesHigh <= 0) {
            throw new IllegalArgumentException("tilesHigh must be > 0");
        }
        this.source = source;
        this.tilesWide = tilesWide;
        this.tilesHigh = tilesHigh;
    }

    /**
     * Factory method to create a MetaTileRange view from a source TileRange.
     *
     * @param source the source TileRange to view as meta-tiles
     * @param tilesWide width of each meta-tile in individual tiles
     * @param tilesHigh height of each meta-tile in individual tiles
     * @return a MetaTileRange view of the source
     */
    public static MetaTileRange of(TileRange source, int tilesWide, int tilesHigh) {
        return new MetaTileRange(source, tilesWide, tilesHigh);
    }

    /**
     * Factory method to create a MetaTileRange from coordinates.
     * Creates a source TileRange first, then wraps it.
     *
     * @param minx minimum X coordinate (inclusive) in meta-tile space
     * @param miny minimum Y coordinate (inclusive) in meta-tile space
     * @param maxx maximum X coordinate (inclusive) in meta-tile space
     * @param maxy maximum Y coordinate (inclusive) in meta-tile space
     * @param zoomLevel the zoom level
     * @param tilesWide width of each meta-tile in individual tiles
     * @param tilesHigh height of each meta-tile in individual tiles
     * @return a new MetaTileRange with LOWER_LEFT axis origin
     */
    public static MetaTileRange of(
            long minx, long miny, long maxx, long maxy, int zoomLevel, int tilesWide, int tilesHigh) {
        return of(minx, miny, maxx, maxy, zoomLevel, tilesWide, tilesHigh, AxisOrigin.LOWER_LEFT);
    }

    /**
     * Factory method to create a MetaTileRange from coordinates with axis origin.
     * Creates a source TileRange first, then wraps it.
     *
     * @param minx minimum X coordinate (inclusive) in meta-tile space
     * @param miny minimum Y coordinate (inclusive) in meta-tile space
     * @param maxx maximum X coordinate (inclusive) in meta-tile space
     * @param maxy maximum Y coordinate (inclusive) in meta-tile space
     * @param zoomLevel the zoom level
     * @param tilesWide width of each meta-tile in individual tiles
     * @param tilesHigh height of each meta-tile in individual tiles
     * @param axisOrigin the axis origin
     * @return a new MetaTileRange
     */
    public static MetaTileRange of(
            long minx,
            long miny,
            long maxx,
            long maxy,
            int zoomLevel,
            int tilesWide,
            int tilesHigh,
            AxisOrigin axisOrigin) {
        // Convert meta-tile coordinates to individual tile coordinates
        long minTileX = minx * tilesWide;
        long minTileY = miny * tilesHigh;
        long maxTileX = (maxx + 1) * tilesWide - 1;
        long maxTileY = (maxy + 1) * tilesHigh - 1;

        TileRange sourceTileRange = TileRange.of(minTileX, minTileY, maxTileX, maxTileY, zoomLevel, axisOrigin);
        return new MetaTileRange(sourceTileRange, tilesWide, tilesHigh);
    }

    /**
     * Returns the source TileRange being viewed as meta-tiles.
     */
    public TileRange getSource() {
        return source;
    }

    /**
     * Returns the width of each meta-tile in individual tiles.
     */
    public int tilesWide() {
        return tilesWide;
    }

    /**
     * Returns the height of each meta-tile in individual tiles.
     */
    public int tilesHigh() {
        return tilesHigh;
    }

    @Override
    public int zoomLevel() {
        return source.zoomLevel();
    }

    @Override
    public long minx() {
        return source.minx() / tilesWide;
    }

    @Override
    public long miny() {
        return source.miny() / tilesHigh;
    }

    @Override
    public long maxx() {
        return source.maxx() / tilesWide;
    }

    @Override
    public long maxy() {
        return source.maxy() / tilesHigh;
    }

    @Override
    public long spanX() {
        return maxx() - minx() + 1;
    }

    @Override
    public long spanY() {
        return maxy() - miny() + 1;
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

    private long countMetatiles(long ntiles, int metaSize) {
        long rem = ntiles % metaSize;
        long metas = ntiles / metaSize;
        return (rem > 0 ? 1 : 0) + metas;
    }

    @Override
    public TileIndex lowerLeft() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> MetaTileIndex.of(minx(), miny(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_LEFT -> MetaTileIndex.of(minx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
            case LOWER_RIGHT -> MetaTileIndex.of(maxx(), miny(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_RIGHT -> MetaTileIndex.of(maxx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
        };
    }

    @Override
    public TileIndex upperRight() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> MetaTileIndex.of(maxx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_LEFT -> MetaTileIndex.of(maxx(), miny(), zoomLevel(), tilesWide, tilesHigh);
            case LOWER_RIGHT -> MetaTileIndex.of(minx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_RIGHT -> MetaTileIndex.of(minx(), miny(), zoomLevel(), tilesWide, tilesHigh);
        };
    }

    @Override
    public TileIndex lowerRight() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> MetaTileIndex.of(maxx(), miny(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_LEFT -> MetaTileIndex.of(maxx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
            case LOWER_RIGHT -> MetaTileIndex.of(minx(), miny(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_RIGHT -> MetaTileIndex.of(minx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
        };
    }

    @Override
    public TileIndex upperLeft() {
        return switch (axisOrigin()) {
            case LOWER_LEFT -> MetaTileIndex.of(minx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_LEFT -> MetaTileIndex.of(minx(), miny(), zoomLevel(), tilesWide, tilesHigh);
            case LOWER_RIGHT -> MetaTileIndex.of(maxx(), maxy(), zoomLevel(), tilesWide, tilesHigh);
            case UPPER_RIGHT -> MetaTileIndex.of(maxx(), miny(), zoomLevel(), tilesWide, tilesHigh);
        };
    }

    /**
     * Returns the equivalent TileRange in individual tile coordinate space.
     * This simply returns the source TileRange.
     *
     * @return the source TileRange representing individual tiles
     */
    public TileRange asTileRange() {
        return source;
    }

    @Override
    public boolean isMetaTiled() {
        return true;
    }

    @Override
    public int metaWidth() {
        return tilesWide;
    }

    @Override
    public int metaHeight() {
        return tilesHigh;
    }

    @Override
    public AxisOrigin axisOrigin() {
        return source.axisOrigin();
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
        return "MetaTileRange{" + tilesWide + "x" + tilesHigh + ", source=" + source + "}";
    }
}
