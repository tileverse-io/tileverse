/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The block index of a Versatiles container: the blocks of the zoom levels and their location in the file.
 *
 * <p>The block index sits at the end of the file and is always compressed with Brotli. Decompressed, it is a sequence
 * of {@link BlockIndexEntry#ENTRY_BYTES 33-byte} entries with no header.
 */
final class BlockIndex {

    private final List<BlockIndexEntry> entries;

    private final Map<BlockKey, BlockIndexEntry> blocks;

    private BlockIndex(List<BlockIndexEntry> entries) {
        this.entries = List.copyOf(entries);
        this.blocks = new HashMap<>();
        for (BlockIndexEntry entry : entries) {
            blocks.put(new BlockKey(entry.level(), entry.blockCol(), entry.blockRow()), entry);
        }
    }

    /** Returns the block index of an archive without tiles. */
    static BlockIndex empty() {
        return new BlockIndex(List.of());
    }

    /**
     * Parses the decompressed block index.
     *
     * @throws IOException if the length is not a whole number of entries, or if an entry is invalid
     */
    static BlockIndex parse(InputStream decompressed) throws IOException {
        byte[] bytes = decompressed.readAllBytes();
        if (bytes.length % BlockIndexEntry.ENTRY_BYTES != 0) {
            throw new IOException("Block index length %d is not a multiple of %d"
                    .formatted(bytes.length, BlockIndexEntry.ENTRY_BYTES));
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        List<BlockIndexEntry> entries = new ArrayList<>(bytes.length / BlockIndexEntry.ENTRY_BYTES);
        try {
            while (buffer.hasRemaining()) {
                entries.add(readEntry(buffer));
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid block index: " + e.getMessage(), e);
        }
        return new BlockIndex(entries);
    }

    private static BlockIndexEntry readEntry(ByteBuffer buffer) {
        int level = Byte.toUnsignedInt(buffer.get());
        long blockCol = Integer.toUnsignedLong(buffer.getInt());
        long blockRow = Integer.toUnsignedLong(buffer.getInt());
        int colMin = Byte.toUnsignedInt(buffer.get());
        int rowMin = Byte.toUnsignedInt(buffer.get());
        int colMax = Byte.toUnsignedInt(buffer.get());
        int rowMax = Byte.toUnsignedInt(buffer.get());
        long blockOffset = buffer.getLong();
        long tileBlobsLength = buffer.getLong();
        int tileIndexLength = buffer.getInt();
        return new BlockIndexEntry(
                level,
                blockCol,
                blockRow,
                colMin,
                rowMin,
                colMax,
                rowMax,
                blockOffset,
                tileBlobsLength,
                tileIndexLength);
    }

    /**
     * Returns the block holding the tile at {@code (z, x, y)}. The result is empty when the archive has no block there,
     * or when the tile falls outside the tile range of its block.
     */
    Optional<BlockIndexEntry> findBlock(int z, long x, long y) {
        BlockKey key = new BlockKey(z, BlockIndexEntry.blockOf(x), BlockIndexEntry.blockOf(y));
        BlockIndexEntry block = blocks.get(key);
        if (block == null || !block.covers(z, x, y)) {
            return Optional.empty();
        }
        return Optional.of(block);
    }

    /** Returns the entries in file order. */
    List<BlockIndexEntry> entries() {
        return entries;
    }

    private record BlockKey(int level, long blockCol, long blockRow) {}
}
