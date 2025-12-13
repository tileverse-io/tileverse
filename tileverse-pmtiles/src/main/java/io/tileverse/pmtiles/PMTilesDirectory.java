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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents a directory of PMTiles entries.
 * <p>
 * A directory is a collection of {@link PMTilesEntry} objects, either stored in memory
 * as a list or backed by a ByteBuffer. This class provides methods to access entries
 * and iterate over them.
 * <p>
 * This class is package-private as it is an internal detail of the PMTiles implementation.
 */
class PMTilesDirectory implements Iterable<PMTilesEntry> {

    private final int size;
    private final ByteBuffer unpacked;

    /**
     * Constructs a PMTilesDirectory from a buffer of unpacked entries.
     *
     * @param size the number of entries in the directory
     * @param unpackedEntries the buffer containing the unpacked entries
     */
    PMTilesDirectory(int size, ByteBuffer unpackedEntries) {
        this.size = size;
        this.unpacked = unpackedEntries;
    }

    /**
     * Creates a builder for creating a new PMTilesDirectory.
     *
     * @param numEntries the number of entries the directory will contain
     * @return a new Builder instance
     */
    static Builder builder(int numEntries) {
        return new Builder(numEntries);
    }

    /**
     * Returns the number of entries in this directory.
     *
     * @return the number of entries
     */
    public int size() {
        return size;
    }

    /**
     * Retrieves the entry at the specified index.
     *
     * @param index the index of the entry to retrieve
     * @return the PMTilesEntry at the specified index
     */
    public PMTilesEntry get(int index) {
        ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
        long tileId = PMTilesEntry.getId(entry);
        long offset = PMTilesEntry.getOffset(entry);
        int length = PMTilesEntry.getLength(entry);
        int runLength = PMTilesEntry.getRunLength(entry);
        return new PMTilesEntry(tileId, offset, length, runLength);
    }

    @Override
    public Iterator<PMTilesEntry> iterator() {
        return IntStream.range(0, size).mapToObj(this::get).iterator();
    }

    @Override
    public String toString() {
        String ids = IntStream.range(0, Math.min(size, 10))
                .mapToObj(i -> entry(unpacked, i))
                .mapToLong(PMTilesEntry::getId)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(","));
        return "%s[size: %,d, unpacked size: %,d, tileIds: %s]"
                .formatted(getClass().getSimpleName(), size, unpacked.capacity(), ids);
    }

    /**
     * Helper method to get a slice of the ByteBuffer corresponding to a specific entry.
     *
     * @param unpacked the buffer containing all entries
     * @param index the index of the entry
     * @return a ByteBuffer slice for the entry
     */
    static ByteBuffer entry(ByteBuffer unpacked, int index) {
        int len = PMTilesEntry.SERIALIZED_SIZE;
        int offset = len * index;
        return unpacked.slice(offset, len);
    }

    /**
     * Builder for creating PMTilesDirectory instances.
     */
    static class Builder {

        private final int numEntries;
        private final ByteBuffer unpacked;

        /**
         * Constructs a new Builder.
         *
         * @param numEntries the number of entries to be added
         */
        public Builder(int numEntries) {
            this.numEntries = numEntries;
            this.unpacked = ByteBuffer.allocate(numEntries * PMTilesEntry.SERIALIZED_SIZE);
        }

        /**
         * Sets the tile ID for the entry at the specified index.
         *
         * @param index the index of the entry
         * @param tileId the tile ID
         * @return this builder
         */
        public Builder tileId(int index, long tileId) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setId(entry, tileId);
            return this;
        }

        /**
         * Sets the run length for the entry at the specified index.
         *
         * @param index the index of the entry
         * @param runLength the run length
         * @return this builder
         */
        public Builder runLength(int index, int runLength) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setRunLength(entry, runLength);
            return this;
        }

        /**
         * Sets the offset for the entry at the specified index.
         *
         * @param index the index of the entry
         * @param offset the offset
         * @return this builder
         */
        public Builder offset(int index, long offset) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setOffset(entry, offset);
            return this;
        }

        /**
         * Sets the length for the entry at the specified index.
         *
         * @param index the index of the entry
         * @param length the length
         * @return this builder
         */
        public Builder length(int index, int length) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setLength(entry, length);
            return this;
        }

        /**
         * Gets the offset of the entry at the specified index.
         *
         * @param index the index of the entry
         * @return the offset
         */
        public long getOffset(int index) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            return PMTilesEntry.getOffset(entry);
        }

        /**
         * Gets the length of the entry at the specified index.
         *
         * @param index the index of the entry
         * @return the length
         */
        public int getLength(int index) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            return PMTilesEntry.getLength(entry);
        }

        /**
         * Builds the PMTilesDirectory.
         *
         * @return the constructed PMTilesDirectory
         */
        public PMTilesDirectory build() {
            return new PMTilesDirectory(numEntries, unpacked);
        }
    }
}
