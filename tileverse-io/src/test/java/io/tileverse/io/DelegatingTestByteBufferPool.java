/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io;

/**
 * Test {@link ByteBufferPool} provider that delegates to a built-in pool, so default-pool behavior is unchanged while
 * the test can observe that discovery routed {@link ByteBufferPool#getDefault()} through a registered provider.
 * Registered via {@code META-INF/services/io.tileverse.io.ByteBufferPool}.
 */
public class DelegatingTestByteBufferPool implements ByteBufferPool {

    private final ByteBufferPool delegate = ByteBufferPool.builder().build();

    @Override
    public PooledByteBuffer borrowDirect(int size) {
        return delegate.borrowDirect(size);
    }

    @Override
    public PooledByteBuffer borrowHeap(int size) {
        return delegate.borrowHeap(size);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public PoolStatistics getDirectPoolStatistics() {
        return delegate.getDirectPoolStatistics();
    }

    @Override
    public PoolStatistics getHeapPoolStatistics() {
        return delegate.getHeapPoolStatistics();
    }

    @Override
    public long getLeakCount() {
        return delegate.getLeakCount();
    }
}
