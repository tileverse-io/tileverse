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

class PMTilesDirectory implements Iterable<PMTilesEntry> {

    private final int size;
    private final ByteBuffer unpacked;

    PMTilesDirectory(int size, ByteBuffer unpackedEntries) {
        this.size = size;
        this.unpacked = unpackedEntries;
    }

    static Builder builder(int numEntries) {
        return new Builder(numEntries);
    }

    public int size() {
        return size;
    }

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

    static ByteBuffer entry(ByteBuffer unpacked, int index) {
        int len = PMTilesEntry.SERIALIZED_SIZE;
        int offset = len * index;
        return unpacked.slice(offset, len);
    }

    static class Builder {

        private final int numEntries;
        private final ByteBuffer unpacked;

        public Builder(int numEntries) {
            this.numEntries = numEntries;
            this.unpacked = ByteBuffer.allocate(numEntries * PMTilesEntry.SERIALIZED_SIZE);
        }

        public Builder tileId(int index, long tileId) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setId(entry, tileId);
            return this;
        }

        public Builder runLength(int index, int runLength) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setRunLength(entry, runLength);
            return this;
        }

        public Builder offset(int index, long offset) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setOffset(entry, offset);
            return this;
        }

        public Builder length(int index, int length) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            PMTilesEntry.setLength(entry, length);
            return this;
        }

        public long getOffset(int index) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            return PMTilesEntry.getOffset(entry);
        }

        public int getLength(int index) {
            ByteBuffer entry = PMTilesDirectory.entry(unpacked, index);
            return PMTilesEntry.getLength(entry);
        }

        public PMTilesDirectory build() {
            return new PMTilesDirectory(numEntries, unpacked);
        }
    }
}
