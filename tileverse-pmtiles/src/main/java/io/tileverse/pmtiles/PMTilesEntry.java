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

import java.util.Objects;
import java.util.function.LongConsumer;

public interface PMTilesEntry extends Comparable<PMTilesEntry> {

    /**
     * Specifies the ID of the tile or the first tile in the leaf directory. The
     * TileID corresponds to a cumulative position on the series of Hilbert curves
     * starting at zoom level {@code 0}.
     * <p>
     * When an entry points to a {@link PMTilesEntry#isLeaf Leaf Directory} (rather
     * than a map tile), the TileID field serves as a routing key.
     * <p>
     * Specifically, the TileID represents the lowest (first) Hilbert TileID
     * contained within that leaf directory, hence acting as the inclusive start of
     * the range covered by that leaf, allowing the client to perform a binary
     * search or linear scan to find the correct directory to fetch.
     */
    long tileId();

    /**
     * The offset of the tile data or leaf directory in the file.
     * <p>
     * For regular entries, this is the offset within the tile data section,
     * relative to the tileDataOffset in the header.
     * <p>
     * For leaf directory entries, this is the offset within the leaf directories
     * section, relative to the leafDirsOffset in the header.
     */
    long offset();

    /**
     * Specifies the number of bytes of this tile or leaf directory. This size
     * always indicates the compressed size, if the tile or leaf directory is
     * compressed. The length <strong>MUST</strong> be greater than {@code 0}
     */
    int length();

    /**
     * Specifies the number of tiles for which this entry is valid.
     * <p>
     * A runLength of {@code 0} indicates this is a leaf directory entry. A
     * runLength of {@code 1} indicates a single tile. A runLength greater than
     * {@code 1} indicates a run of tiles that share the same content, starting at
     * the {@code tileId} and continuing for {@code runLength} consecutive tile IDs.
     * <p>
     * Runs are used to efficiently represent repeated tiles, such as ocean or empty
     * areas in maps.
     */
    int runLength();

    default boolean contains(long tileId) {
        return tileId == tileId() || tileId > tileId() && tileId < tileId() + runLength();
    }

    default void forEachId(LongConsumer consumer) {
        Objects.requireNonNull(consumer);
        consumer.accept(tileId());
        if (runLength() > 0) {
            long max = tileId() + runLength();
            for (long id = tileId() + 1; id < max; id++) {
                consumer.accept(id);
            }
        }
    }

    /**
     * Creates a new PMTilesEntry with the specified runLength.
     *
     * @param tileId    the tile ID
     * @param offset    the offset of the tile data in the file
     * @param length    the length of the tile data
     * @param runLength the number of consecutive tiles with the same content
     * @return a new PMTilesEntry
     */
    public static PMTilesEntry of(long tileId, long offset, int length, int runLength) {
        return PMTilesEntryImpl.of(tileId, offset, length, runLength);
    }

    /**
     * Compares this entry with another entry based on tile ID.
     *
     * @param other the entry to compare with
     * @return a negative integer, zero, or a positive integer as this entry's tile
     *         ID is less than, equal to, or greater than the other entry's tile ID
     */
    @Override
    default int compareTo(PMTilesEntry other) {
        return Long.compare(this.tileId(), other.tileId());
    }

    /**
     * @return true if this is a leaf directory entry, false if {@link #isTile()}
     */
    default boolean isLeaf() {
        return runLength() == 0;
    }

    /**
     * @return true if this is a tile entry (or multiple tiles entry, depending on
     *         runLenght being 1 or > 1), false if {@link #isLeaf()}
     */
    default boolean isTile() {
        return runLength() > 0;
    }

    static boolean equals(PMTilesEntry e1, PMTilesEntry e2) {
        return e1.tileId() == e2.tileId()
                && e1.offset() == e2.offset()
                && e1.length() == e2.length()
                && e1.runLength() == e2.runLength();
    }

    static int hashCode(PMTilesEntry e) {
        int result = Long.hashCode(e.tileId());
        result = 31 * result + Long.hashCode(e.offset());
        result = 31 * result + Integer.hashCode(e.length());
        result = 31 * result + Integer.hashCode(e.runLength());
        return result;
    }
}
