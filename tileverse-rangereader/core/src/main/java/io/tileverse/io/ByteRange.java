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
            throw new IllegalArgumentException("length can't be < 0: " + offset);
        }
    }

    @Override
    public int compareTo(ByteRange o) {
        return Long.compare(offset, o.offset());
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
