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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test {@link ByteBufferAllocator} that counts calls and delegates allocation and release to the built-in backend, so
 * pool behavior is identical to the default while the test can observe that allocations and frees were routed through
 * the provider. Registered as a {@code ServiceLoader} provider via
 * {@code META-INF/services/io.tileverse.io.ByteBufferAllocator} to exercise discovery on the default pool.
 */
public class CountingByteBufferAllocator implements ByteBufferAllocator {

    private final AtomicInteger directAllocations = new AtomicInteger();
    private final AtomicInteger heapAllocations = new AtomicInteger();
    private final AtomicInteger frees = new AtomicInteger();

    @Override
    public ByteBuffer allocateDirect(int capacity) {
        directAllocations.incrementAndGet();
        return BuiltinByteBufferAllocator.INSTANCE.allocateDirect(capacity);
    }

    @Override
    public ByteBuffer allocateHeap(int capacity) {
        heapAllocations.incrementAndGet();
        return BuiltinByteBufferAllocator.INSTANCE.allocateHeap(capacity);
    }

    @Override
    public void free(ByteBuffer buffer) {
        frees.incrementAndGet();
        BuiltinByteBufferAllocator.INSTANCE.free(buffer);
    }

    int directAllocations() {
        return directAllocations.get();
    }

    int heapAllocations() {
        return heapAllocations.get();
    }

    int frees() {
        return frees.get();
    }
}
