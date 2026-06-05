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

import java.nio.ByteBuffer;
import java.time.Duration;

/**
 * A thread-safe pool of {@link ByteBuffer} instances that reduces allocation pressure in range readers needing
 * temporary buffers. Direct buffers are typically used for I/O, heap buffers for in-memory processing.
 *
 * <p><strong>Pluggable implementation:</strong> {@code ByteBufferPool} is an interface so the whole pooling behavior,
 * not just where buffers come from, can be supplied by a provider. {@link #getDefault()} discovers an implementation
 * through {@link java.util.ServiceLoader}; when none is registered it builds the {@linkplain #builder() built-in pool}.
 * A provider that owns a process-wide memory budget (for example one backed by a foreign-memory arena) can register a
 * pool so that callers borrowing here draw from, and are accounted against, that single budget.
 *
 * <p><strong>Borrowing never blocks:</strong> a borrow always returns a usable buffer. If nothing suitable is retained,
 * the built-in pool creates one, even beyond its retention budget; only what the free list keeps is bounded.
 *
 * <p><strong>Buffer lifecycle:</strong> borrowed buffers are wrapped in a {@link PooledByteBuffer} that returns the
 * buffer to the pool when closed. Always use try-with-resources.
 *
 * <pre>{@code
 * try (PooledByteBuffer pooled = ByteBufferPool.directBuffer(8192)) {
 *     ByteBuffer buffer = pooled.buffer();
 *     channel.read(buffer);
 *     buffer.flip();
 *     // process data...
 * } // buffer automatically returned to the pool
 * }</pre>
 *
 * <p><strong>Buffer capacity:</strong> {@link PooledByteBuffer#buffer()} returns a buffer whose capacity matches the
 * requested size exactly. The built-in pool may allocate larger buffers (rounded to a block-size boundary) and return a
 * slice matching the request.
 *
 * @see PooledByteBuffer
 * @see #directBuffer(int)
 * @see #heapBuffer(int)
 */
public interface ByteBufferPool {

    /** Default maximum number of direct buffers retained in the free list. */
    int DEFAULT_MAX_DIRECT_BUFFERS = 32;

    /** Default maximum number of heap buffers retained in the free list. */
    int DEFAULT_MAX_HEAP_BUFFERS = 64;

    /** Default block size for allocation alignment and pooling (8KB). */
    int DEFAULT_BLOCK_SIZE = 8192;

    /** Default upper bound on direct bytes retained in the free list (64 MiB). */
    long DEFAULT_MAX_DIRECT_BYTES = 64L * 1024 * 1024;

    /** Default upper bound on heap bytes retained in the free list (32 MiB). */
    long DEFAULT_MAX_HEAP_BYTES = 32L * 1024 * 1024;

    /** Default idle period after which a quiet pool releases its whole free list. */
    Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Borrows a direct (off-heap) buffer with at least the requested capacity. The returned {@link PooledByteBuffer}
     * must be closed (use try-with-resources). Never blocks: a buffer is always returned.
     *
     * @param size minimum required capacity in bytes
     * @return a handle wrapping a direct buffer whose {@link PooledByteBuffer#buffer() buffer} has the requested
     *     capacity
     * @throws IllegalArgumentException if size is negative
     */
    PooledByteBuffer borrowDirect(int size);

    /**
     * Borrows an on-heap buffer with at least the requested capacity. The returned {@link PooledByteBuffer} must be
     * closed (use try-with-resources). Never blocks: a buffer is always returned.
     *
     * @param size minimum required capacity in bytes
     * @return a handle wrapping a heap buffer whose {@link PooledByteBuffer#buffer() buffer} has the requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    PooledByteBuffer borrowHeap(int size);

    /** Releases all retained buffers. Optional: a provider that retains nothing may leave this a no-op. */
    default void clear() {
        // no retained state by default
    }

    /** Returns combined direct and heap statistics. Providers without statistics report zeros. */
    default PoolStatistics getStatistics() {
        return getDirectPoolStatistics().add(getHeapPoolStatistics());
    }

    /** Returns direct-buffer statistics. Providers without statistics report zeros. */
    default PoolStatistics getDirectPoolStatistics() {
        return PoolStatistics.EMPTY;
    }

    /** Returns heap-buffer statistics. Providers without statistics report zeros. */
    default PoolStatistics getHeapPoolStatistics() {
        return PoolStatistics.EMPTY;
    }

    /**
     * Returns the number of borrows detected as leaked (garbage-collected without {@link PooledByteBuffer#close()}). A
     * non-zero value indicates a borrow site that does not close its buffer. Providers without leak detection report
     * zero.
     *
     * @return the cumulative leak count
     */
    default long getLeakCount() {
        return 0L;
    }

    /**
     * Returns the shared default pool: the registered {@link java.util.ServiceLoader} provider if any, otherwise the
     * built-in pool. Borrowing through {@link #directBuffer(int)} and {@link #heapBuffer(int)} uses this pool.
     *
     * @return the default pool
     */
    static ByteBufferPool getDefault() {
        return DefaultByteBufferPool.DEFAULT;
    }

    /**
     * Borrows a direct buffer from the {@linkplain #getDefault() default pool}. Equivalent to
     * {@code getDefault().borrowDirect(size)}.
     *
     * @param size minimum required capacity in bytes
     * @return a handle that must be closed (use try-with-resources)
     */
    static PooledByteBuffer directBuffer(int size) {
        return getDefault().borrowDirect(size);
    }

    /**
     * Borrows a heap buffer from the {@linkplain #getDefault() default pool}. Equivalent to
     * {@code getDefault().borrowHeap(size)}.
     *
     * @param size minimum required capacity in bytes
     * @return a handle that must be closed (use try-with-resources)
     */
    static PooledByteBuffer heapBuffer(int size) {
        return getDefault().borrowHeap(size);
    }

    /**
     * Creates a {@link Builder} for the built-in pool, seeded with the default settings.
     *
     * @return a new builder
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A wrapper around a pooled {@link ByteBuffer} that implements {@link AutoCloseable} so try-with-resources returns
     * the buffer to the pool on close.
     */
    interface PooledByteBuffer extends AutoCloseable {

        /** Returns the borrowed buffer, with capacity matching the requested size. */
        ByteBuffer buffer();

        @Override
        void close();
    }

    /** Immutable statistics snapshot for a pool. */
    record PoolStatistics(
            int maxSize, int poolSize, long bytesSize, long created, long reused, long returned, long discarded) {

        /** All-zero snapshot, reported by providers that do not track statistics. */
        static final PoolStatistics EMPTY = new PoolStatistics(0, 0, 0, 0, 0, 0, 0);

        public double hitRate() {
            long totalBorrows = created + reused;
            return totalBorrows > 0 ? (reused * 100.0) / totalBorrows : 0.0;
        }

        public double returnRate() {
            long totalReturns = returned + discarded;
            return totalReturns > 0 ? (returned * 100.0) / totalReturns : 0.0;
        }

        PoolStatistics add(PoolStatistics other) {
            return new PoolStatistics(
                    maxSize + other.maxSize,
                    poolSize + other.poolSize,
                    bytesSize + other.bytesSize,
                    created + other.created,
                    reused + other.reused,
                    returned + other.returned,
                    discarded + other.discarded);
        }
    }

    /**
     * Fluent configuration for the built-in pool. Every setting defaults to the documented default, so only the knobs
     * that matter for a given use case need to be set.
     */
    final class Builder {

        private int maxDirectBuffers = DEFAULT_MAX_DIRECT_BUFFERS;
        private int maxHeapBuffers = DEFAULT_MAX_HEAP_BUFFERS;
        private int blockSize = DEFAULT_BLOCK_SIZE;
        private long maxDirectBytes = DEFAULT_MAX_DIRECT_BYTES;
        private long maxHeapBytes = DEFAULT_MAX_HEAP_BYTES;
        private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private boolean leakDetection = true;

        private Builder() {
            // use ByteBufferPool.builder()
        }

        /** Sets the maximum number of direct buffers retained in the free list. */
        public Builder maxDirectBuffers(int maxDirectBuffers) {
            this.maxDirectBuffers = maxDirectBuffers;
            return this;
        }

        /** Sets the maximum number of heap buffers retained in the free list. */
        public Builder maxHeapBuffers(int maxHeapBuffers) {
            this.maxHeapBuffers = maxHeapBuffers;
            return this;
        }

        /** Sets the block size for allocation alignment and the minimum pooling threshold. */
        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /** Sets the upper bound on direct bytes retained in the free list. */
        public Builder maxDirectBytes(long maxDirectBytes) {
            this.maxDirectBytes = maxDirectBytes;
            return this;
        }

        /** Sets the upper bound on heap bytes retained in the free list. */
        public Builder maxHeapBytes(long maxHeapBytes) {
            this.maxHeapBytes = maxHeapBytes;
            return this;
        }

        /**
         * Sets how long a pool may stay idle before releasing its whole free list. A zero or negative duration disables
         * idle eviction.
         */
        public Builder idleTimeout(Duration idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        /**
         * Enables or disables watching borrowed buffers for being garbage-collected without being closed. Detection is
         * on by default; turn it off to avoid the small per-borrow overhead on a hot path that is known to be correct.
         */
        public Builder leakDetection(boolean leakDetection) {
            this.leakDetection = leakDetection;
            return this;
        }

        /** Builds the configured built-in pool. */
        public ByteBufferPool build() {
            return new DefaultByteBufferPool(
                    maxDirectBuffers,
                    maxHeapBuffers,
                    blockSize,
                    maxDirectBytes,
                    maxHeapBytes,
                    idleTimeout,
                    leakDetection);
        }
    }
}
