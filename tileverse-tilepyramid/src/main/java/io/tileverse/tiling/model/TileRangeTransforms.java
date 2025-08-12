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
 * Utility class for transforming between different TileRange representations.
 * Provides symmetric conversion operations between individual tile ranges and meta-tile ranges.
 *
 * @since 1.0
 */
final class TileRangeTransforms {

    private TileRangeTransforms() {
        // Utility class
    }

    /**
     * Converts a TileRange to a MetaTileRange view with the specified tile dimensions.
     * The resulting MetaTileRange will be a view that interprets the source as meta-tiles.
     *
     * @param tileRange the tile range to view as meta-tiles
     * @param tilesWide width of each meta-tile in individual tiles
     * @param tilesHigh height of each meta-tile in individual tiles
     * @return a MetaTileRange view of the tile range
     * @throws IllegalArgumentException if tilesWide or tilesHigh is <= 0
     */
    static MetaTileRange toMetaTileRange(TileRange tileRange, int tilesWide, int tilesHigh) {
        if (tilesWide <= 0) throw new IllegalArgumentException("tilesWide must be > 0");
        if (tilesHigh <= 0) throw new IllegalArgumentException("tilesHigh must be > 0");

        // If already a MetaTileRange, handle re-tiling
        if (tileRange instanceof MetaTileRange metaRange) {
            return convertMetaTileRange(metaRange, tilesWide, tilesHigh);
        }

        // Create a view of the tile range as meta-tiles
        return MetaTileRange.of(tileRange, tilesWide, tilesHigh);
    }

    /**
     * Converts a TileRange to individual tile coordinates.
     * For regular TileRange, returns the same range.
     * For MetaTileRange, returns the underlying source TileRange.
     *
     * @param tileRange the tile range to convert
     * @return a TileRange representing individual tiles
     */
    static TileRange toTileRange(TileRange tileRange) {
        if (tileRange instanceof MetaTileRange metaRange) {
            return metaRange.getSource(); // Return the wrapped source
        }
        return tileRange; // Already individual tiles
    }

    /**
     * Converts a MetaTileRange to another MetaTileRange with different tile dimensions.
     * Uses the source TileRange to create a new view with different meta-tile dimensions.
     */
    private static MetaTileRange convertMetaTileRange(MetaTileRange metaRange, int newTilesWide, int newTilesHigh) {
        // Create a new view of the same source with different tile dimensions
        return MetaTileRange.of(metaRange.getSource(), newTilesWide, newTilesHigh);
    }
}
