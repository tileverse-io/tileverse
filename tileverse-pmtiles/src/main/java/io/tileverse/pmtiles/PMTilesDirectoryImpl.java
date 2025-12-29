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
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class PMTilesDirectoryImpl implements PMTilesDirectory {

    /**
     * Size of {@code tileId + offset + length + runLength}
     */
    static final int SERIALIZED_ENTRY_SIZE = Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    private static final int ID_OFFSET = 0;
    private static final int OFFSET_OFFSET = 8;
    private static final int LENGTH_OFFSET = 16;
    private static final int RUNLENGTH_OFFSET = 20;

    /**
     * The number of entries contained in the directory (MUST be greater than 0)
     */
    private final int size;

    /**
     * Unpacked representation of entries (see {@link ByteBufferEntry})
     */
    final ByteBuffer unpacked;

    /**
     * Constructs a PMTilesDirectory from a buffer of unpacked entries.
     *
     * @param size            the number of entries in the directory
     * @param unpackedEntries the buffer containing the unpacked entries
     */
    private PMTilesDirectoryImpl(int size, ByteBuffer unpackedEntries) {
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
     * @return the number of entries, greater than zero
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Finds the index of the entry with the given tileId using binary search.
     * This method allocates NO memory.
     *
     * @param tileId the tile ID to search for
     * @return the index of the entry, or -1 if not found
     */
    @Override
    public int findEntryIndex(long tileId) {
        int low = 0;
        int high = size - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            // Read directly from buffer without creating an object
            long midVal = PMTilesDirectoryImpl.getTileId(mid, unpacked);

            if (midVal < tileId) low = mid + 1;
            else if (midVal > tileId) high = mid - 1;
            else return mid; // Key found
        }
        return -1; // key not found
    }

    /**
     * Retrieves the entry at the specified index.
     *
     * @param index the index of the entry to retrieve
     * @return the PMTilesEntry at the specified index
     */
    @Override
    public PMTilesEntry getEntry(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Entry index %d out of bounds [%d,%d]".formatted(index, 0, size));
        }
        return dettached(index);
        // return ByteBufferEntry.valueOf(index, unpacked);
    }

    private PMTilesEntry dettached(int index) {
        return PMTilesEntry.of(
                PMTilesDirectoryImpl.getTileId(index, unpacked),
                PMTilesDirectoryImpl.getOffset(index, unpacked),
                PMTilesDirectoryImpl.getLength(index, unpacked),
                PMTilesDirectoryImpl.getRunLength(index, unpacked));
    }

    @Override
    public long getTileId(int index) {
        return PMTilesDirectoryImpl.getTileId(index, unpacked);
    }

    @Override
    public long getOffset(int index) {
        return PMTilesDirectoryImpl.getOffset(index, unpacked);
    }

    @Override
    public int getLength(int index) {
        return PMTilesDirectoryImpl.getLength(index, unpacked);
    }

    @Override
    public int getRunLength(int index) {
        return PMTilesDirectoryImpl.getRunLength(index, unpacked);
    }

    @Override
    public Iterator<PMTilesEntry> iterator() {
        return entries().iterator();
    }

    @Override
    public Spliterator<PMTilesEntry> spliterator() {
        // Uses the Spliterator from the IntStream range, which is already SIZED and SUBSIZED
        return entries().spliterator();
    }

    @Override
    public PMTilesEntry firstEntry() {
        return getEntry(0);
    }

    @Override
    public PMTilesEntry lastEntry() {
        return getEntry(size - 1);
    }

    /**
     * @return all entries
     */
    @Override
    public Stream<PMTilesEntry> entries() {
        return IntStream.range(0, size).mapToObj(this::getEntry);
    }

    /**
     * @return only directory entries
     */
    @Override
    public Stream<PMTilesEntry> directoryEntries() {
        return IntStream.range(0, size)
                .filter(index -> PMTilesDirectoryImpl.isLeaf(index, unpacked))
                .mapToObj(this::getEntry);
    }

    @Override
    public int numDirectoryEntries() {
        return (int) IntStream.range(0, size)
                .filter(index -> PMTilesDirectoryImpl.isLeaf(index, unpacked))
                .count();
    }

    /**
     * @return only tile entries
     */
    @Override
    public Stream<PMTilesEntry> tileEntries() {
        return IntStream.range(0, size)
                .filter(index -> PMTilesDirectoryImpl.isTile(index, unpacked))
                .mapToObj(this::getEntry);
    }

    @Override
    public int numTileEntries() {
        return (int) IntStream.range(0, size)
                .filter(index -> PMTilesDirectoryImpl.isTile(index, unpacked))
                .count();
    }

    @Override
    public String toString() {
        String className = getClass().getSimpleName();
        int storageSize = unpacked.capacity();
        return "%s[size: %,d, unpacked size: %,d]".formatted(className, size, storageSize);
    }

    private static int entryOffset(int index) {
        int entryLength = SERIALIZED_ENTRY_SIZE;
        return entryLength * index;
    }

    /**
     * Sets the tile ID in the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param directory  the byte buffer
     * @param tileId     the tile ID to set
     */
    private static void setId(int entryIndex, ByteBuffer directory, long tileId) {
        directory.putLong(entryOffset(entryIndex) + ID_OFFSET, tileId);
    }

    /**
     * Gets the tile ID from the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param entrySlice the byte buffer
     * @return the tile ID
     */
    private static long getTileId(int entryIndex, ByteBuffer directory) {
        return directory.getLong(entryOffset(entryIndex) + ID_OFFSET);
    }

    /**
     * Sets the offset in the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param directory  the byte buffer
     * @param offset     the offset to set
     */
    private static void setOffset(int entryIndex, ByteBuffer directory, long offset) {
        directory.putLong(entryOffset(entryIndex) + OFFSET_OFFSET, offset);
    }

    /**
     * Gets the offset from the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param entrySlice the byte buffer
     * @return the offset
     */
    private static long getOffset(int entryIndex, ByteBuffer directory) {
        return directory.getLong(entryOffset(entryIndex) + OFFSET_OFFSET);
    }

    /**
     * Sets the length in the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param directory  the byte buffer
     * @param length     the length to set
     */
    private static void setLength(int entryIndex, ByteBuffer directory, int length) {
        directory.putInt(entryOffset(entryIndex) + LENGTH_OFFSET, length);
    }

    /**
     * Gets the length from the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param entrySlice the byte buffer
     * @return the length
     */
    private static int getLength(int entryIndex, ByteBuffer directory) {
        return directory.getInt(entryOffset(entryIndex) + LENGTH_OFFSET);
    }

    /**
     * Sets the run length in the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param entrySlice the byte buffer
     * @param runLength  the run length to set
     */
    private static void setRunLength(int enrtyIndex, ByteBuffer entrySlice, int runLength) {
        entrySlice.putInt(entryOffset(enrtyIndex) + RUNLENGTH_OFFSET, runLength);
    }

    /**
     * Gets the run length from the byte buffer.
     *
     * @param entryIndex index of the entry in the directory buffer
     * @param entrySlice the byte buffer
     * @return the run length
     */
    private static int getRunLength(int entryIndex, ByteBuffer directory) {
        return directory.getInt(entryOffset(entryIndex) + RUNLENGTH_OFFSET);
    }

    private static boolean isLeaf(int entryIndex, ByteBuffer directory) {
        return 0 == PMTilesDirectoryImpl.getRunLength(entryIndex, directory);
    }

    private static boolean isTile(int entryIndex, ByteBuffer entrySlice) {
        return !PMTilesDirectoryImpl.isLeaf(entryIndex, entrySlice);
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
            if (numEntries < 1) {
                throw new IllegalArgumentException(
                        "PMTilesDirectory num entries must be greater than zero: " + numEntries);
            }
            this.numEntries = numEntries;
            final int capacity = numEntries * PMTilesDirectoryImpl.SERIALIZED_ENTRY_SIZE;
            this.unpacked = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
        }

        /**
         * Builds the PMTilesDirectory.
         *
         * @return the constructed PMTilesDirectory
         */
        public PMTilesDirectoryImpl build() {
            return new PMTilesDirectoryImpl(numEntries, unpacked);
        }

        /**
         * Sets the tile ID for the entry at the specified index.
         *
         * @param index  the index of the entry
         * @param tileId the tile ID
         * @return this builder
         */
        public Builder tileId(int index, long tileId) {
            PMTilesDirectoryImpl.setId(index, unpacked, tileId);
            return this;
        }

        /**
         * Sets the run length for the entry at the specified index.
         *
         * @param index     the index of the entry
         * @param runLength the run length
         * @return this builder
         */
        public Builder runLength(int index, int runLength) {
            PMTilesDirectoryImpl.setRunLength(index, unpacked, runLength);
            return this;
        }

        /**
         * Sets the offset for the entry at the specified index.
         *
         * @param index  the index of the entry
         * @param offset the offset
         * @return this builder
         */
        public Builder offset(int index, long offset) {
            PMTilesDirectoryImpl.setOffset(index, unpacked, offset);
            return this;
        }

        /**
         * Sets the length for the entry at the specified index.
         *
         * @param index  the index of the entry
         * @param length the length
         * @return this builder
         */
        public Builder length(int index, int length) {
            PMTilesDirectoryImpl.setLength(index, unpacked, length);
            return this;
        }

        /**
         * Gets the offset of the entry at the specified index.
         *
         * @param index the index of the entry
         * @return the offset
         */
        public long getOffset(int index) {
            return PMTilesDirectoryImpl.getOffset(index, unpacked);
        }

        /**
         * Gets the length of the entry at the specified index.
         *
         * @param index the index of the entry
         * @return the length
         */
        public int getLength(int index) {
            return PMTilesDirectoryImpl.getLength(index, unpacked);
        }
    }

    @SuppressWarnings("unused")
    private static final record ByteBufferEntry(int index, ByteBuffer directory) implements PMTilesEntry {

        /**
         * Factory method from an unpacked entry
         */
        public static PMTilesEntry valueOf(int index, ByteBuffer directory) {
            return new ByteBufferEntry(index, directory);
        }

        @Override
        public long tileId() {
            return PMTilesDirectoryImpl.getTileId(index, directory);
        }

        @Override
        public long offset() {
            return PMTilesDirectoryImpl.getOffset(index, directory);
        }

        @Override
        public int length() {
            return PMTilesDirectoryImpl.getLength(index, directory);
        }

        @Override
        public int runLength() {
            return PMTilesDirectoryImpl.getRunLength(index, directory);
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
