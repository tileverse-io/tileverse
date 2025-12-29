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
package io.tileverse.pmtiles;

record PMTilesEntryImpl(long tileId, long offset, int length, int runLength) implements PMTilesEntry {

    @Override
    public boolean equals(Object o) {
        return o instanceof PMTilesEntry e && PMTilesEntry.equals(this, e);
    }

    @Override
    public int hashCode() {
        return PMTilesEntry.hashCode(this);
    }

    public static PMTilesEntry of(long tileId, long offset, int length, int runLength) {
        return switch (runLength) {
            case 0 -> Leaf.of(tileId, offset, length);
            case 1 -> Tile.of(tileId, offset, length);
            default -> new PMTilesEntryImpl(tileId, offset, length, runLength);
        };
    }

    private static final record Leaf(long tileId, long offset, int length) implements PMTilesEntry {

        static PMTilesEntry of(long tileId, long offset, int length) {
            if (tileId < Integer.MAX_VALUE && offset < Integer.MAX_VALUE) {
                return new Leaf.LeafInt((int) tileId, (int) offset, length);
            }
            return new Leaf(tileId, offset, length);
        }

        @Override
        public int runLength() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof PMTilesEntry e && PMTilesEntry.equals(this, e);
        }

        @Override
        public int hashCode() {
            return PMTilesEntry.hashCode(this);
        }

        private static final record LeafInt(int intTileId, int intOffset, int length) implements PMTilesEntry {

            @Override
            public int runLength() {
                return 0;
            }

            @Override
            public long tileId() {
                return intTileId;
            }

            @Override
            public long offset() {
                return intOffset;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof PMTilesEntry e && PMTilesEntry.equals(this, e);
            }

            @Override
            public int hashCode() {
                return PMTilesEntry.hashCode(this);
            }
        }
    }

    private static final record Tile(long tileId, long offset, int length) implements PMTilesEntry {

        static PMTilesEntry of(long tileId, long offset, int length) {
            if (tileId < Integer.MAX_VALUE) {
                if (offset < Integer.MAX_VALUE) {
                    return new Tile.TileIntInt((int) tileId, (int) offset, length);
                }
                return new Tile.TileIntLong((int) tileId, offset, length);
            }
            return new Tile(tileId, offset, length);
        }

        @Override
        public int runLength() {
            return 1;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof PMTilesEntry e && PMTilesEntry.equals(this, e);
        }

        @Override
        public int hashCode() {
            return PMTilesEntry.hashCode(this);
        }

        private static final record TileIntInt(int intTileId, int intOffset, int length) implements PMTilesEntry {

            @Override
            public int runLength() {
                return 1;
            }

            @Override
            public long tileId() {
                return intTileId;
            }

            @Override
            public long offset() {
                return intOffset;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof PMTilesEntry e && PMTilesEntry.equals(this, e);
            }

            @Override
            public int hashCode() {
                return PMTilesEntry.hashCode(this);
            }
        }

        private static final record TileIntLong(int intTileId, long offset, int length) implements PMTilesEntry {

            @Override
            public int runLength() {
                return 1;
            }

            @Override
            public long tileId() {
                return intTileId;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof PMTilesEntry e && PMTilesEntry.equals(this, e);
            }

            @Override
            public int hashCode() {
                return PMTilesEntry.hashCode(this);
            }
        }
    }
}
