/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.batch;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;

/**
 * Reads one planned fetch into a buffer with the exact {@link io.tileverse.storage.RangeReader#readRange(long, int,
 * ByteBuffer)} semantics: 0 at or past EOF, a short count when the range straddles EOF, the target position advanced by
 * the count. Normally a {@code readRange} method reference; {@code AbstractRangeReader} hands {@link BatchRunner} a
 * reader of its own, writing joined fetches straight into their targets.
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

    /**
     * Reads a joined fetch, of requests following one another without overlapping, into the targets of its requests.
     * Returns how many bytes of the fetch were read from its start, gap bytes included, with the EOF semantics of
     * {@link #read}; the caller then {@link FetchTargets#settle settles} the counts and positions. The default reads
     * the fetch into pooled heap scratch with {@link #read} and copies the bytes into the targets.
     *
     * @param range the byte range of the fetch
     * @param targets where the requests' bytes land
     * @return the number of bytes of the fetch read
     */
    default int readJoined(ByteRange range, FetchTargets targets) {
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(range.length())) {
            ByteBuffer scratch = pooled.buffer();
            int read = read(range, scratch);
            targets.write(0, scratch, 0, read);
            return read;
        }
    }
}
