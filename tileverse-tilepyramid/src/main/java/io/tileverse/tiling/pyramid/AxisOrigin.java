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

import java.util.Optional;

/**
 * Defines the coordinate system origin for tile pyramids and ranges.
 * Specifies where the (0,0) coordinate is conceptually positioned,
 * which affects how traversal and query algorithms interpret coordinates.
 *
 * @since 1.0
 */
public enum AxisOrigin {
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
    LOWER_LEFT {
        @Override
        public Optional<TileIndex> next(TileIndex current, TileRange range) {
            // Left-to-right: increment X first, then wrap to next row at minx
            long nextX = current.x() + 1;
            if (nextX <= range.maxx()) {
                return Optional.of(TileIndex.of(nextX, current.y(), current.z()));
            }
            // Wrap to next row
            long nextY = current.y() + 1;
            if (nextY <= range.maxy()) {
                return Optional.of(TileIndex.of(range.minx(), nextY, current.z()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<TileIndex> prev(TileIndex current, TileRange range) {
            // Left-to-right reverse: decrement X first, then wrap to prev row at maxx
            long prevX = current.x() - 1;
            if (prevX >= range.minx()) {
                return Optional.of(TileIndex.of(prevX, current.y(), current.z()));
            }
            // Wrap to prev row
            long prevY = current.y() - 1;
            if (prevY >= range.miny()) {
                return Optional.of(TileIndex.of(range.maxx(), prevY, current.z()));
            }
            return Optional.empty();
        }
    },

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
    UPPER_LEFT {
        @Override
        public Optional<TileIndex> next(TileIndex current, TileRange range) {
            // Left-to-right: increment X first, then wrap to prev row at minx (Y decreases)
            long nextX = current.x() + 1;
            if (nextX <= range.maxx()) {
                return Optional.of(TileIndex.of(nextX, current.y(), current.z()));
            }
            // Wrap to prev row (Y decreases in UPPER_LEFT)
            long prevY = current.y() - 1;
            if (prevY >= range.miny()) {
                return Optional.of(TileIndex.of(range.minx(), prevY, current.z()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<TileIndex> prev(TileIndex current, TileRange range) {
            // Left-to-right reverse: decrement X first, then wrap to next row at maxx (Y increases)
            long prevX = current.x() - 1;
            if (prevX >= range.minx()) {
                return Optional.of(TileIndex.of(prevX, current.y(), current.z()));
            }
            // Wrap to next row (Y increases in UPPER_LEFT reverse)
            long nextY = current.y() + 1;
            if (nextY <= range.maxy()) {
                return Optional.of(TileIndex.of(range.maxx(), nextY, current.z()));
            }
            return Optional.empty();
        }
    },

    /**
     * Origin at lower-right corner.
     * Y=0 represents the bottom/south, Y increases upward/northward.
     * X=0 represents the right/east, X increases leftward/westward.
     * Rarely used in practice.
     *
     * Visual representation:
     * <pre>
     * (2,2) (1,2) (0,2)
     * (2,1) (1,1) (0,1)
     * (2,0) (1,0) (0,0)  ← Y=0 at bottom, X=0 at right
     * </pre>
     */
    LOWER_RIGHT {
        @Override
        public Optional<TileIndex> next(TileIndex current, TileRange range) {
            // Right-to-left: decrement X first, then wrap to next row at maxx
            long prevX = current.x() - 1;
            if (prevX >= range.minx()) {
                return Optional.of(TileIndex.of(prevX, current.y(), current.z()));
            }
            // Wrap to next row
            long nextY = current.y() + 1;
            if (nextY <= range.maxy()) {
                return Optional.of(TileIndex.of(range.maxx(), nextY, current.z()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<TileIndex> prev(TileIndex current, TileRange range) {
            // Right-to-left reverse: increment X first, then wrap to prev row at minx
            long nextX = current.x() + 1;
            if (nextX <= range.maxx()) {
                return Optional.of(TileIndex.of(nextX, current.y(), current.z()));
            }
            // Wrap to prev row
            long prevY = current.y() - 1;
            if (prevY >= range.miny()) {
                return Optional.of(TileIndex.of(range.minx(), prevY, current.z()));
            }
            return Optional.empty();
        }
    },

    /**
     * Origin at upper-right corner.
     * Y=0 represents the top/north, Y increases downward/southward.
     * X=0 represents the right/east, X increases leftward/westward.
     * Rarely used in practice.
     *
     * Visual representation:
     * <pre>
     * (2,0) (1,0) (0,0)  ← Y=0 at top, X=0 at right
     * (2,1) (1,1) (0,1)
     * (2,2) (1,2) (0,2)
     * </pre>
     */
    UPPER_RIGHT {
        @Override
        public Optional<TileIndex> next(TileIndex current, TileRange range) {
            // Right-to-left: decrement X first, then wrap to prev row at maxx (Y decreases)
            long prevX = current.x() - 1;
            if (prevX >= range.minx()) {
                return Optional.of(TileIndex.of(prevX, current.y(), current.z()));
            }
            // Wrap to prev row (Y decreases in UPPER_RIGHT)
            long prevY = current.y() - 1;
            if (prevY >= range.miny()) {
                return Optional.of(TileIndex.of(range.maxx(), prevY, current.z()));
            }
            return Optional.empty();
        }

        @Override
        public Optional<TileIndex> prev(TileIndex current, TileRange range) {
            // Right-to-left reverse: increment X first, then wrap to next row at minx (Y increases)
            long nextX = current.x() + 1;
            if (nextX <= range.maxx()) {
                return Optional.of(TileIndex.of(nextX, current.y(), current.z()));
            }
            // Wrap to next row (Y increases in UPPER_RIGHT reverse)
            long nextY = current.y() + 1;
            if (nextY <= range.maxy()) {
                return Optional.of(TileIndex.of(range.minx(), nextY, current.z()));
            }
            return Optional.empty();
        }
    };

    /**
     * Returns the next tile in natural traversal order for this axis origin.
     * Uses row-major ordering adapted for the specific coordinate system.
     *
     * @param current the current tile index
     * @param range the tile range containing the current tile
     * @return the next tile index, or empty if current is the last tile
     */
    public abstract Optional<TileIndex> next(TileIndex current, TileRange range);

    /**
     * Returns the previous tile in natural traversal order for this axis origin.
     * Uses row-major ordering adapted for the specific coordinate system in reverse.
     *
     * @param current the current tile index
     * @param range the tile range containing the current tile
     * @return the previous tile index, or empty if current is the first tile
     */
    public abstract Optional<TileIndex> prev(TileIndex current, TileRange range);

    /**
     * Returns true if this axis origin requires Y-coordinate flipping
     * when converting to the target axis origin.
     *
     * @param target the target axis origin
     * @return true if Y-coordinate transformation is needed
     */
    public boolean needsYFlip(AxisOrigin target) {
        if (this == target) return false;

        boolean thisIsUpper = (this == UPPER_LEFT || this == UPPER_RIGHT);
        boolean targetIsUpper = (target == UPPER_LEFT || target == UPPER_RIGHT);

        return thisIsUpper != targetIsUpper;
    }

    /**
     * Returns true if this axis origin requires X-coordinate flipping
     * when converting to the target axis origin.
     *
     * @param target the target axis origin
     * @return true if X-coordinate transformation is needed
     */
    public boolean needsXFlip(AxisOrigin target) {
        if (this == target) return false;

        boolean thisIsRight = (this == LOWER_RIGHT || this == UPPER_RIGHT);
        boolean targetIsRight = (target == LOWER_RIGHT || target == UPPER_RIGHT);

        return thisIsRight != targetIsRight;
    }
}
