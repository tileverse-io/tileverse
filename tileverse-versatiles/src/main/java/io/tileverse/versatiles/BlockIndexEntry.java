/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import io.tileverse.io.ByteRange;
import io.tileverse.tiling.pyramid.TileIndex;

/**
 * An entry of the block index: a block of up to 256 x 256 tiles at a single zoom level.
 *
 * <p>The entry records the position of the block in the tile grid of its zoom level ({@code blockCol},
 * {@code blockRow}), the range of tiles stored in the block ({@code colMin} to {@code colMax} and {@code rowMin} to
 * {@code rowMax}, counted from the corner of the block), and the location of the block in the file. A block stores its
 * tile bodies first, then its tile index.
 *
 * <p>Wire layout, in big-endian order:
 *
 * <pre>
 *  0  +1   u8  level
 *  1  +4   u32 block_col   (= column / 256)
 *  5  +4   u32 block_row   (= row / 256)
 *  9  +1   u8  col_min
 * 10  +1   u8  row_min
 * 11  +1   u8  col_max
 * 12  +1   u8  row_max
 * 13  +8   u64 block_offset
 * 21  +8   u64 tile_blobs_length
 * 29  +4   u32 tile_index_length
 * </pre>
 */
record BlockIndexEntry(
        int level,
        long blockCol,
        long blockRow,
        int colMin,
        int rowMin,
        int colMax,
        int rowMax,
        long blockOffset,
        long tileBlobsLength,
        int tileIndexLength) {

    /** Length of an entry in the block index, in bytes. */
    static final int ENTRY_BYTES = 33;

    /** Number of tile columns, and of tile rows, of a block. */
    static final int BLOCK_SIZE = 256;

    /**
     * Validates the tile range and the location of the block.
     *
     * @throws IllegalArgumentException if the tile range is reversed, or if the block location is out of range
     */
    BlockIndexEntry {
        if (colMin > colMax || rowMin > rowMax) {
            throw new IllegalArgumentException(
                    "Block %d/%d/%d has a reversed tile range".formatted(level, blockCol, blockRow));
        }
        if (blockOffset < 0 || tileBlobsLength < 0 || tileIndexLength < 0) {
            throw new IllegalArgumentException(
                    "Block %d/%d/%d has an invalid location in the file".formatted(level, blockCol, blockRow));
        }
    }

    /** Returns the block column of a tile column, or the block row of a tile row. */
    static long blockOf(long tileCoordinate) {
        return Math.floorDiv(tileCoordinate, BLOCK_SIZE);
    }

    /** Whether the tile at {@code (z, x, y)} falls in the tile range of this block. */
    boolean covers(int z, long x, long y) {
        if (z != level || blockOf(x) != blockCol || blockOf(y) != blockRow) {
            return false;
        }
        int col = positionInBlock(x);
        int row = positionInBlock(y);
        return col >= colMin && col <= colMax && row >= rowMin && row <= rowMax;
    }

    /**
     * Returns the slot of the tile at {@code (x, y)} in the tile index of this block. The tile index lists the tile
     * range row by row. Callers check {@link #covers} first.
     */
    int slotIndex(long x, long y) {
        int width = colMax - colMin + 1;
        return (positionInBlock(y) - rowMin) * width + (positionInBlock(x) - colMin);
    }

    /** Returns the number of slots in the tile index of this block. */
    int slotCount() {
        return (colMax - colMin + 1) * (rowMax - rowMin + 1);
    }

    /** Returns the position of the tile in slot {@code slotIndex}, the reverse of {@link #slotIndex}. */
    TileIndex tileAt(int slotIndex) {
        int width = colMax - colMin + 1;
        long x = blockCol * BLOCK_SIZE + colMin + slotIndex % width;
        long y = blockRow * BLOCK_SIZE + rowMin + slotIndex / width;
        return TileIndex.zxy(level, x, y);
    }

    /** Returns the location of the tile index in the file. It follows the tile bodies of the block. */
    ByteRange tileIndexRange() {
        return ByteRange.of(blockOffset + tileBlobsLength, tileIndexLength);
    }

    /** Returns the location in the file of a tile body, from its slot in the tile index. */
    ByteRange tileRange(TileIndexBlob.Slot slot) {
        return ByteRange.of(blockOffset + slot.offsetInBlock(), slot.length());
    }

    private static int positionInBlock(long tileCoordinate) {
        return Math.floorMod(tileCoordinate, BLOCK_SIZE);
    }
}
