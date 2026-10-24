/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.batch;

import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;

/**
 * Reads one planned fetch into a buffer with the exact {@link io.tileverse.storage.RangeReader#readRange(long, int,
 * ByteBuffer)} semantics: 0 at or past EOF, a short count when the range straddles EOF, the target position advanced by
 * the count. Normally a {@code readRange} method reference.
 */
@FunctionalInterface
public interface FetchReader {

    /**
     * Reads {@code range} into {@code target} at its current position.
     *
     * @param range the byte range to fetch
     * @param target the buffer the bytes land in
     * @return the number of bytes read
     */
    int read(ByteRange range, ByteBuffer target);
}
