/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.BatchReadResult;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.NonNull;

/**
 * The outcome of one batch read by {@link BatchRangeLoader}.
 *
 * @param values the cached value of each range, read-only and holding exactly the bytes read
 * @param fetched the ranges fetched by this read, as opposed to found cached or taken from another read
 * @param cost the fetches and bytes transferred by this read
 */
record BatchLoadOutcome(Map<ByteRange, ByteBuffer> values, Set<ByteRange> fetched, BatchReadResult cost) {

    @NonNull
    ByteBuffer cachedValue(ByteRange range) {
        return values().get(range);
    }

    public boolean fetched(ByteRange range) {
        return fetched().contains(range);
    }
}
