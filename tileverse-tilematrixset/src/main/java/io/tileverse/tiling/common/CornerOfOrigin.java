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
package io.tileverse.tiling.common;

import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.tiling.pyramid.TileRange;

/**
 * Defines the coordinate system origin for tile pyramids and ranges.
 * Specifies where the (0,0) coordinate is conceptually positioned,
 * which affects how traversal and query algorithms interpret coordinates.
 *
 * @since 1.0
 */
public enum CornerOfOrigin {

    /**
     * Origin at upper-left corner.
     * Y=0 represents the top/north, Y increases downward/southward.
     * Used by XYZ/Google/Slippy Map tiles and PMTiles.
     *
     * Visual representation:
     * <pre>
     * (0,0) (1,0) (2,0)  ← Y=0 at top
     * (0,1) (1,1) (2,1)
     * (0,2) (1,2) (2,2)
     * </pre>
     */
    TOP_LEFT {

        @Override
        public TileIndex lowerLeft(TileRange range) {
            return TileIndex.xyz(range.minx(), range.maxy(), range.zoomLevel());
        }

        @Override
        public TileIndex upperRight(TileRange range) {
            return TileIndex.xyz(range.maxx(), range.miny(), range.zoomLevel());
        }

        @Override
        public TileIndex lowerRight(TileRange range) {
            return TileIndex.xyz(range.maxx(), range.maxy(), range.zoomLevel());
        }

        @Override
        public TileIndex upperLeft(TileRange range) {
            return TileIndex.xyz(range.minx(), range.miny(), range.zoomLevel());
        }
    },

    /**
     * Origin at lower-left corner.
     * Y=0 represents the bottom/south, Y increases upward/northward.
     * Used by TMS (Tile Map Service) and traditional GIS coordinate systems.
     *
     * Visual representation:
     * <pre>
     * (0,2) (1,2) (2,2)
     * (0,1) (1,1) (2,1)
     * (0,0) (1,0) (2,0)  ← Y=0 at bottom
     * </pre>
     */
    BOTTOM_LEFT {

        @Override
        public TileIndex lowerLeft(TileRange range) {
            return TileIndex.xyz(range.minx(), range.miny(), range.zoomLevel());
        }

        @Override
        public TileIndex upperRight(TileRange range) {
            return TileIndex.xyz(range.maxx(), range.maxy(), range.zoomLevel());
        }

        @Override
        public TileIndex lowerRight(TileRange range) {
            return TileIndex.xyz(range.maxx(), range.miny(), range.zoomLevel());
        }

        @Override
        public TileIndex upperLeft(TileRange range) {
            return TileIndex.xyz(range.minx(), range.maxy(), range.zoomLevel());
        }
    };

    /**
     * Returns the lower-left corner tile index for the given tile range.
     *
     * @param range the tile range
     * @return the lower-left corner tile index
     */
    public abstract TileIndex lowerLeft(TileRange range);

    /**
     * Returns the upper-right corner tile index for the given tile range.
     *
     * @param range the tile range
     * @return the upper-right corner tile index
     */
    public abstract TileIndex upperRight(TileRange range);

    /**
     * Returns the lower-right corner tile index for the given tile range.
     *
     * @param range the tile range
     * @return the lower-right corner tile index
     */
    public abstract TileIndex lowerRight(TileRange range);

    /**
     * Returns the upper-left corner tile index for the given tile range.
     *
     * @param range the tile range
     * @return the upper-left corner tile index
     */
    public abstract TileIndex upperLeft(TileRange range);

    /**
     * Returns the point of origin coordinate for tile (0,0) within the given extent.
     * The point of origin is the coordinate that corresponds to tile (0,0)
     * according to this corner of origin.
     *
     * @param extent the spatial extent
     * @return the coordinate for tile (0,0) based on this corner of origin
     */
    public io.tileverse.tiling.common.Coordinate pointOfOrigin(io.tileverse.tiling.common.BoundingBox2D extent) {
        return switch (this) {
            case TOP_LEFT -> extent.upperLeft();
            case BOTTOM_LEFT -> extent.lowerLeft();
        };
    }

    /**
     * Returns true if this axis origin requires Y-coordinate flipping
     * when converting to the target axis origin.
     *
     * @param target the target axis origin
     * @return true if Y-coordinate transformation is needed
     */
    public boolean needsYFlip(CornerOfOrigin target) {
        if (this == target) return false;

        boolean thisIsUpper = this == TOP_LEFT;
        boolean targetIsUpper = target == TOP_LEFT;

        return thisIsUpper != targetIsUpper;
    }
}
