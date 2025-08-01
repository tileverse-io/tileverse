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
 * Represents a meta-tile coordinate - a tile that represents a rectangular group of individual tiles.
 * A meta-tile is essentially a higher-level tile that contains multiple regular tiles within it.
 *
 * @param x the X coordinate of the meta-tile
 * @param y the Y coordinate of the meta-tile
 * @param z the zoom level
 * @param tilesWide width of the meta-tile in individual tiles
 * @param tilesHigh height of the meta-tile in individual tiles
 * @since 1.0
 */
record MetaTileIndex(long x, long y, int z, int tilesWide, int tilesHigh) implements TileIndex {

    public MetaTileIndex {
        if (tilesWide <= 0) {
            throw new IllegalArgumentException("tilesWide must be > 0");
        }
        if (tilesHigh <= 0) {
            throw new IllegalArgumentException("tilesHigh must be > 0");
        }
    }

    /**
     * Factory method to create a MetaTileIndex.
     *
     * @param x the X coordinate of the meta-tile
     * @param y the Y coordinate of the meta-tile
     * @param z the zoom level
     * @param tilesWide width of the meta-tile in individual tiles
     * @param tilesHigh height of the meta-tile in individual tiles
     * @return a new MetaTileIndex
     */
    public static MetaTileIndex of(long x, long y, int z, int tilesWide, int tilesHigh) {
        return new MetaTileIndex(x, y, z, tilesWide, tilesHigh);
    }

    /**
     * Returns the TileRange that represents all individual tiles contained within this meta-tile.
     * This converts the meta-tile coordinate space back to individual tile coordinate space.
     * Overrides the default TileIndex.asTileRange() to return the multi-tile range.
     *
     * @return a TileRange covering all individual tiles in this meta-tile
     */
    @Override
    public TileRange asTileRange() {
        long minTileX = x * tilesWide;
        long minTileY = y * tilesHigh;
        long maxTileX = minTileX + tilesWide - 1;
        long maxTileY = minTileY + tilesHigh - 1;

        return TileRange.of(minTileX, minTileY, maxTileX, maxTileY, z);
    }

    /**
     * Alias for asTileRange() for backward compatibility.
     */
    public TileRange asTiles() {
        return asTileRange();
    }
}
