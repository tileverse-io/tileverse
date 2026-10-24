/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;

/**
 * A directory is simply a list of {@link PMTilesEntry entries}. Each entry describes either where a specific tile can
 * be found in the tile data section or where a leaf directory can be found in the leaf directories section.
 *
 * <p>The number of entries in the root directory and in the leaf directories is left to the implementation and can vary
 * depending on what the writer has optimized for (cost, bandwidth, latency, etc.). However, the size of the header plus
 * the compressed size of the root directory MUST NOT exceed {@code 16384} bytes to allow latency-optimized clients to
 * retrieve the root directory in its entirety. Therefore, the maximum compressed size of the root directory is
 * {@code 16257} bytes ({@code 16384 - 127} bytes). A sophisticated writer might need several attempts to optimize this.
 *
 * <p>The order of leaf directories SHOULD be ascending by starting TileID. It is discouraged to create an archive with
 * more than one level of leaf directories. If you are implementing a writer and discover this need, please open an
 * issue.
 *
 * <p>This class provides methods to access entries and iterate over them.
 */
public interface PMTilesDirectory extends Iterable<PMTilesEntry> {

    /**
     * Returns the number of entries in this directory.
     *
     * @return the number of entries, greater than zero
     */
    int size();

    /**
     * Finds the index of the entry with the given tileId using binary search. This method allocates NO memory.
     *
     * @param tileId the tile ID to search for
     * @return the index of the entry, or -1 if not found
     */
    int findEntryIndex(long tileId);

    /**
     * Retrieves the entry at the specified index.
     *
     * @param index the index of the entry to retrieve
     * @return the PMTilesEntry at the specified index
     */
    PMTilesEntry getEntry(int index);

    long getTileId(int index);

    long getOffset(int index);

    int getLength(int index);

    int getRunLength(int index);

    @Override
    Iterator<PMTilesEntry> iterator();

    @Override
    Spliterator<PMTilesEntry> spliterator();

    PMTilesEntry firstEntry();

    PMTilesEntry lastEntry();

    /** @return all entries */
    Stream<PMTilesEntry> entries();

    /** @return only directory entries */
    Stream<PMTilesEntry> directoryEntries();

    int numDirectoryEntries();

    /** @return only tile entries */
    Stream<PMTilesEntry> tileEntries();

    int numTileEntries();
}
