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

/**
 * Represents a directory entry in a PMTiles file.
 * <p>
 * Each entry maps a tile ID to its location and size in the file, and optionally
 * indicates a run of consecutive tiles that share the same content.
 */
public record PMTilesEntry(
        /**
         * The ID of the tile or the start of a run of tiles.
         * Tile IDs uniquely identify tiles using a Z-order curve value derived
         * from the tile's Z/X/Y coordinates. This allows for efficient binary search
         * and ensures entries are ordered.
         */
        long tileId,

        /**
         * The offset of the tile data or leaf directory in the file.
         * <p>
         * For regular entries, this is the offset within the tile data section,
         * relative to the tileDataOffset in the header.
         * <p>
         * For leaf directory entries, this is the offset within the leaf directories section,
         * relative to the leafDirsOffset in the header.
         */
        long offset,

        /**
         * The length of the tile data or leaf directory in bytes.
         * <p>
         * A length of 0 indicates an empty entry (no tile data).
         */
        int length,

        /**
         * The number of consecutive tiles with the same content.
         * <p>
         * A runLength of 0 indicates this is a leaf directory entry.
         * A runLength of 1 indicates a single tile.
         * A runLength greater than 1 indicates a run of tiles that share the same content,
         * starting at the tileId and continuing for runLength consecutive tile IDs.
         * <p>
         * Runs are used to efficiently represent repeated tiles, such as ocean or
         * empty areas in maps.
         */
        int runLength)
        implements Comparable<PMTilesEntry> {

    /**
     * Creates a new PMTilesEntry with a runLength of 1.
     *
     * @param tileId the tile ID
     * @param offset the offset of the tile data in the file
     * @param length the length of the tile data
     * @return a new PMTilesEntry
     */
    public static PMTilesEntry of(long tileId, long offset, int length) {
        return new PMTilesEntry(tileId, offset, length, 1);
    }

    /**
     * Creates a new PMTilesEntry with the specified runLength.
     *
     * @param tileId the tile ID
     * @param offset the offset of the tile data in the file
     * @param length the length of the tile data
     * @param runLength the number of consecutive tiles with the same content
     * @return a new PMTilesEntry
     */
    public static PMTilesEntry of(long tileId, long offset, int length, int runLength) {
        return new PMTilesEntry(tileId, offset, length, runLength);
    }

    /**
     * Creates a new leaf directory entry.
     * Leaf directory entries have a runLength of 0 to indicate they point to a subdirectory.
     *
     * @param tileId the tile ID
     * @param offset the offset of the subdirectory in the file
     * @param length the length of the subdirectory
     * @return a new PMTilesEntry for a leaf directory
     */
    public static PMTilesEntry leaf(long tileId, long offset, int length) {
        return new PMTilesEntry(tileId, offset, length, 0);
    }

    /**
     * Checks if this entry is a leaf directory entry.
     *
     * @return true if this is a leaf directory entry, false otherwise
     */
    public boolean isLeaf() {
        return runLength == 0;
    }

    /**
     * Checks if this entry is empty.
     * Empty entries are used to represent missing tiles.
     *
     * @return true if this entry is empty, false otherwise
     */
    public boolean isEmpty() {
        return length == 0;
    }

    /**
     * Creates a new empty entry.
     * Empty entries are used to represent missing tiles.
     *
     * @return a new empty PMTilesEntry
     */
    public static PMTilesEntry empty() {
        return new PMTilesEntry(0, 0, 0, 0);
    }

    /**
     * Compares this entry with another entry based on tile ID.
     *
     * @param other the entry to compare with
     * @return a negative integer, zero, or a positive integer as this entry's tile ID
     *         is less than, equal to, or greater than the other entry's tile ID
     */
    @Override
    public int compareTo(PMTilesEntry other) {
        return Long.compare(this.tileId, other.tileId);
    }

    /**
     * Creates a new builder for constructing PMTilesEntry instances.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing PMTilesEntry instances incrementally.
     * This is particularly useful for deserialization where fields are read in separate passes.
     */
    public static class Builder {
        private long tileId = 0;
        private long offset = 0;
        private int length = 0;
        private int runLength = 0;

        /**
         * Sets the tile ID.
         *
         * @param tileId the tile ID
         * @return this builder for method chaining
         */
        public Builder tileId(long tileId) {
            this.tileId = tileId;
            return this;
        }

        /**
         * Sets the offset.
         *
         * @param offset the offset of the tile data or leaf directory
         * @return this builder for method chaining
         */
        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the length.
         *
         * @param length the length of the tile data or leaf directory
         * @return this builder for method chaining
         */
        public Builder length(int length) {
            this.length = length;
            return this;
        }

        /**
         * Sets the run length.
         *
         * @param runLength the number of consecutive tiles with the same content
         * @return this builder for method chaining
         */
        public Builder runLength(int runLength) {
            this.runLength = runLength;
            return this;
        }

        /**
         * Gets the current offset value.
         *
         * @return the current offset
         */
        public long getOffset() {
            return offset;
        }

        /**
         * Gets the current length value.
         *
         * @return the current length
         */
        public int getLength() {
            return length;
        }

        /**
         * Builds the final PMTilesEntry instance.
         *
         * @return a new PMTilesEntry with the configured values
         */
        public PMTilesEntry build() {
            return new PMTilesEntry(tileId, offset, length, runLength);
        }
    }
}
