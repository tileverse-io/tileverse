/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.util.NoSuchElementException;

/**
 * Reads a run of packed varints, such as the geometry commands or the tags of a feature. It decodes each varint as
 * {@link WireReader#readVarint32()} does, with the rule of protobuf for a uint32: the low 32 bits of the first five
 * bytes.
 *
 * <p>The range must end on a byte with its high bit clear, where its last varint ends. Decoding a tile validates its
 * packed runs, and a cursor over a cut run is a programming error. A varint starting inside the range then ends inside
 * it, and the cursor reads its bytes without a range check.
 *
 * <p>{@link #reset} moves the cursor to another range: decoding reads the geometry commands of the features of a layer
 * with one cursor.
 */
final class VarintCursor {

    /** The size before the varints are counted. */
    private static final int NOT_COUNTED = -1;

    private byte[] bytes;
    private int end;

    // The offset of the next varint, and the number of varints before it
    private int bytePosition;
    private int position;

    private int size;

    /**
     * Reads the varints from {@code start} inclusive to {@code end} exclusive.
     *
     * @throws IllegalArgumentException unless the range is empty or ends on a byte with its high bit clear
     */
    VarintCursor(byte[] bytes, int start, int end) {
        reset(bytes, start, end);
    }

    /**
     * Moves the cursor to the first varint of another range, from {@code start} inclusive to {@code end} exclusive.
     *
     * @throws IllegalArgumentException unless the range is empty or ends on a byte with its high bit clear
     */
    void reset(byte[] bytes, int start, int end) {
        if (start < end && bytes[end - 1] < 0) {
            throw new IllegalArgumentException(
                    "The bytes from " + start + " to " + end + " end in the middle of a varint");
        }
        this.bytes = bytes;
        this.end = end;
        this.bytePosition = start;
        this.position = 0;
        this.size = NOT_COUNTED;
    }

    /**
     * The number of varints in the range. The first call counts the varints past the position, and the cursor keeps the
     * result. A cursor read to its end knows its size without reading a byte.
     */
    int size() {
        if (size == NOT_COUNTED) {
            size = position + countVarints(bytePosition, end);
        }
        return size;
    }

    // Each varint ends on the first byte with its high bit clear
    private int countVarints(int from, int to) {
        int count = 0;
        for (int i = from; i < to; i++) {
            if (bytes[i] >= 0) {
                count++;
            }
        }
        return count;
    }

    /** The number of varints read. */
    int position() {
        return position;
    }

    boolean hasNext() {
        return bytePosition < end;
    }

    int remaining() {
        return size() - position;
    }

    /**
     * Reads the next varint.
     *
     * @throws NoSuchElementException if no varint is left
     */
    int next() {
        requireNext();
        position++;
        return readVarint();
    }

    private void requireNext() {
        if (!hasNext()) {
            throw new NoSuchElementException("The " + position + " varints of the range were read");
        }
    }

    /*
     * A single loop reads each length. A separate method for the longer varints would stay out of line, and its calls
     * would keep the cursor on the heap. The loop moves a local offset, kept in a register, and stores the position
     * once.
     */
    private int readVarint() {
        int offset = bytePosition;
        int value = 0;
        for (int shift = 0; shift < 35; shift += 7) {
            byte b = bytes[offset++];
            value |= (b & 0x7F) << shift;
            if (b >= 0) {
                bytePosition = offset;
                return value;
            }
        }
        // The bytes past the fifth are skipped up to the end of the varint
        bytePosition = endOfVarint(offset);
        return value;
    }

    // A varint ends on the first byte with its high bit clear
    private int endOfVarint(int offset) {
        int next = offset;
        byte b;
        do {
            b = bytes[next];
            next++;
        } while (b < 0);
        return next;
    }
}
