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
import java.util.Optional;

/**
 * The tile index of a block: the locations of the tile bodies in the tile range of the block, row by row.
 *
 * <p>A slot is 12 bytes: the offset of the tile body from the start of the block (u64), then its length (u32). A slot
 * of length zero means the block has no tile at that position.
 */
final class TileIndexBlob {

    /** Length of a slot in bytes. */
    static final int SLOT_BYTES = 12;

    private final ByteBuffer slots;

    private TileIndexBlob(byte[] slots) {
        this.slots = ByteBuffer.wrap(slots).order(ByteOrder.BIG_ENDIAN).asReadOnlyBuffer();
    }

    /**
     * Parses the decompressed tile index of a block.
     *
     * @throws IOException if the tile index does not hold {@code slotCount} slots, or if a slot is out of range
     */
    static TileIndexBlob parse(InputStream decompressed, int slotCount) throws IOException {
        byte[] bytes = decompressed.readAllBytes();
        if (bytes.length != slotCount * SLOT_BYTES) {
            throw new IOException("Tile index size %d does not match expected %d slots * %d bytes"
                    .formatted(bytes.length, slotCount, SLOT_BYTES));
        }
        TileIndexBlob tileIndex = new TileIndexBlob(bytes);
        tileIndex.requireSlotsInRange(slotCount);
        return tileIndex;
    }

    /** Returns the location of the tile body in slot {@code slotIndex}, or empty if the block has no tile there. */
    Optional<Slot> slot(int slotIndex) {
        int position = slotIndex * SLOT_BYTES;
        long offset = slots.getLong(position);
        int length = slots.getInt(position + Long.BYTES);
        if (length == 0) {
            return Optional.empty();
        }
        return Optional.of(new Slot(offset, length));
    }

    /** Returns the size of the slots in memory, in bytes. */
    int sizeInBytes() {
        return slots.capacity();
    }

    /** A tile body larger than 2 GiB or past the end of a file can only come from a damaged index. */
    private void requireSlotsInRange(int slotCount) throws IOException {
        for (int i = 0; i < slotCount; i++) {
            int position = i * SLOT_BYTES;
            long offset = slots.getLong(position);
            int length = slots.getInt(position + Long.BYTES);
            if (offset < 0 || length < 0) {
                throw new IOException("Tile index slot %d is out of range: offset %s, length %s"
                        .formatted(i, Long.toUnsignedString(offset), Integer.toUnsignedString(length)));
            }
        }
    }

    /**
     * The location of a tile body in its block.
     *
     * @param offsetInBlock the offset of the tile body from the start of the block
     * @param length the length of the tile body
     */
    record Slot(long offsetInBlock, int length) {}
}
