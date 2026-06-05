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
 * Pluggable allocation backend for {@link ByteBufferPool}. A provider supplies the raw {@link ByteBuffer} instances the
 * pool reuses and reclaims them when the pool evicts or clears them.
 *
 * <p><strong>Discovery:</strong> {@link ByteBufferPool#getDefault()} discovers a provider through
 * {@link java.util.ServiceLoader}. When no provider is registered, the pool falls back to a built-in backend that
 * allocates direct buffers with {@link ByteBuffer#allocateDirect(int)} and heap buffers with
 * {@link ByteBuffer#allocate(int)}. Standalone tileverse on a Java 17 JVM therefore behaves exactly as it did before
 * this interface existed. Pools constructed directly with {@link ByteBufferPool#ByteBufferPool(int, int, int)} use the
 * built-in backend unless an allocator is passed explicitly.
 *
 * <p><strong>Allocate and free are a pair:</strong> a provider owns both the allocation and the release of every buffer
 * it hands out. The pool calls {@link #free(ByteBuffer)} for every buffer it discards, so a provider must not rely on
 * the pool to release memory by any other means. This pairing matters because the built-in backend releases direct
 * buffers eagerly through {@code sun.misc.Unsafe.invokeCleaner}, which is valid only on a root direct buffer; a buffer
 * obtained from a different mechanism (for example an FFM {@code MemorySegment} viewed as a {@code ByteBuffer}) would
 * be rejected by that path. A provider that returns such buffers must therefore supply a matching
 * {@link #free(ByteBuffer)} rather than inherit the built-in one.
 *
 * <p><strong>Thread safety:</strong> the pool calls these methods concurrently from multiple threads, so
 * implementations must be thread-safe.
 *
 * <p>The interface uses only Java 17 types ({@link ByteBuffer}, {@code int}) so that tileverse-storage compiles to Java
 * 17 bytecode while a provider compiled for a newer runtime can still supply buffers backed by a modern allocation
 * mechanism, handed across the boundary as a plain {@link ByteBuffer}.
 */
public interface ByteBufferAllocator {

    /**
     * Allocates a new off-heap (direct) buffer with at least the given capacity.
     *
     * @param capacity the minimum capacity in bytes
     * @return a direct {@link ByteBuffer} whose capacity is at least {@code capacity}
     */
    ByteBuffer allocateDirect(int capacity);

    /**
     * Allocates a new on-heap buffer with at least the given capacity.
     *
     * @param capacity the minimum capacity in bytes
     * @return a heap {@link ByteBuffer} whose capacity is at least {@code capacity}
     */
    ByteBuffer allocateHeap(int capacity);

    /**
     * Releases a buffer previously produced by this allocator. The pool calls this when it evicts or clears a buffer.
     * Implementations must tolerate both direct and heap buffers and must be safe to call once per buffer.
     *
     * @param buffer the buffer to release; never {@code null}
     */
    void free(ByteBuffer buffer);
}
