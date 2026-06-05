/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
