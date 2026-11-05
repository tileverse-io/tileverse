/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.lang.foreign.MemorySegment;

import io.tileverse.io.ffm.DefaultSegmentPool.Backing;

/** A borrowed segment from {@link DefaultSegmentPool}; returns its backing on first {@link #close()}. */
final class PooledSegment implements SegmentPool.Pooled {

    private final DefaultSegmentPool pool;
    private final MemorySegment view;
    private Backing backing;

    PooledSegment(DefaultSegmentPool pool, Backing backing, MemorySegment view) {
        this.pool = pool;
        this.backing = backing;
        this.view = view;
    }

    @Override
    public MemorySegment segment() {
        return view;
    }

    @Override
    public void close() {
        if (backing == null) {
            return;
        }
        pool.giveBack(backing);
        pool.borrowReturned();
        backing = null;
    }
}
