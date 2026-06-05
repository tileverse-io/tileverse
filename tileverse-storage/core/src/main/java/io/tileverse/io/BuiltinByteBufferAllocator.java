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

/**
 * Default {@link ByteBufferAllocator} used when no provider is registered. Direct buffers come from
 * {@link ByteBuffer#allocateDirect(int)} and are released eagerly through {@link DirectByteBufferCleaner}; heap buffers
 * come from {@link ByteBuffer#allocate(int)} and are left to the garbage collector. This reproduces the allocation
 * behavior {@link ByteBufferPool} had before the allocation backend became pluggable.
 */
final class BuiltinByteBufferAllocator implements ByteBufferAllocator {

    static final BuiltinByteBufferAllocator INSTANCE = new BuiltinByteBufferAllocator();

    private BuiltinByteBufferAllocator() {
        // singleton
    }

    @Override
    public ByteBuffer allocateDirect(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
    }

    @Override
    public ByteBuffer allocateHeap(int capacity) {
        return ByteBuffer.allocate(capacity);
    }

    @Override
    public void free(ByteBuffer buffer) {
        if (buffer != null && buffer.isDirect()) {
            DirectByteBufferCleaner.releaseDirectBuffer(buffer);
        }
    }
}
