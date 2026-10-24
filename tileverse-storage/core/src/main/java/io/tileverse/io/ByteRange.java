/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io;

import java.io.Serializable;

/**
 * Byte range defined by offset and length
 *
 * @param offset the starting position of the range
 * @param length the number of bytes in the range
 */
public record ByteRange(
        /** The starting position of the range */
        long offset,
        /** The number of bytes in the range */
        int length)
        implements Serializable, Comparable<ByteRange> {
    /**
     * Compact constructor that validates the byte range parameters.
     *
     * @param offset the starting position of the range (must be non-negative)
     * @param length the number of bytes in the range (must be non-negative)
     */
    public ByteRange {
        if (offset < 0) {
            throw new IllegalArgumentException("offset can't be < 0: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length can't be < 0: " + length);
        }
    }

    @Override
    public int compareTo(ByteRange o) {
        return Long.compare(offset, o.offset());
    }

    /**
     * Returns the exclusive end offset of this range ({@code offset + length}).
     *
     * @return the first offset after this range
     */
    public long end() {
        return offset + length;
    }

    /**
     * Returns whether the given absolute position falls inside this range. A zero-length range contains no position.
     *
     * @param position the absolute position to test
     * @return true if {@code offset <= position < end()}
     */
    public boolean contains(long position) {
        return position >= offset && position < end();
    }

    /**
     * Returns whether this range and {@code other} share at least one byte. Zero-length ranges overlap nothing.
     *
     * @param other the range to test against
     * @return true if the two half-open intervals intersect
     */
    public boolean overlaps(ByteRange other) {
        return length > 0 && other.length() > 0 && offset < other.end() && other.offset() < end();
    }

    /**
     * Creates a new {@link ByteRange} with a different offset but the same length.
     *
     * @param newOffset The new offset.
     * @return A new {@link ByteRange} instance.
     */
    public ByteRange withOffset(long newOffset) {
        return new ByteRange(newOffset, length());
    }

    /**
     * Factory method to create a new {@link ByteRange}.
     *
     * @param offset The starting offset.
     * @param length The length of the range.
     * @return A new {@link ByteRange} instance.
     */
    public static ByteRange of(long offset, int length) {
        return new ByteRange(offset, length);
    }
}
