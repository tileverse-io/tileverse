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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread-safe pool of {@link ByteBuffer} instances to reduce garbage collection
 * pressure in range readers that require temporary buffers.
 * <p>
 * This pool manages direct and heap ByteBuffers separately, allowing efficient
 * reuse of buffers for different use cases. Direct buffers are typically used
 * for I/O operations, while heap buffers are used for in-memory processing.
 * <p>
 * <strong>Thread Safety:</strong> This class is fully thread-safe and can be
 * used concurrently by multiple threads without external synchronization.
 * <p>
 * <strong>Memory Management:</strong> The pool automatically limits the number
 * of pooled buffers to prevent unbounded memory growth. Buffers that exceed
 * the pool capacity are simply discarded when returned.
 * <p>
 * <strong>Buffer Lifecycle:</strong> Borrowed buffers are wrapped in a
 * {@link PooledByteBuffer} that automatically returns the buffer to the pool
 * when closed. Always use try-with-resources to ensure proper cleanup.
 *
 * <p><strong>Usage Example (Recommended):</strong></p>
 * <pre>{@code
 * // Using static convenience methods with default pool
 * try (var pooled = ByteBufferPool.directBuffer(8192)) {
 *     ByteBuffer buffer = pooled.buffer();
 *     // Use buffer for I/O operations
 *     channel.read(buffer);
 *     buffer.flip();
 *     // Process data...
 * } // Buffer automatically returned to pool
 * }</pre>
 *
 * <p><strong>Custom Pool Configuration:</strong></p>
 * <pre>{@code
 * // Create custom pool with specific limits
 * ByteBufferPool customPool = new ByteBufferPool(
 *     50,    // maxDirectBuffers
 *     100,   // maxHeapBuffers
 *     1024   // blockSize
 * );
 *
 * try (var pooled = customPool.borrowDirect(65536)) {
 *     ByteBuffer buffer = pooled.buffer();
 *     // Use large buffer...
 * } // Automatically returned to pool
 * }</pre>
 *
 * <p><strong>Buffer Capacity:</strong></p>
 * The {@link PooledByteBuffer#buffer()} method returns a buffer whose capacity
 * matches the requested size exactly. Internally, the pool may allocate larger
 * buffers (rounded to block size boundaries for alignment), but the returned buffer
 * is a slice that matches your requested capacity.
 *
 * @see PooledByteBuffer
 * @see #directBuffer(int)
 * @see #heapBuffer(int)
 */
public class ByteBufferPool {

    private static final Logger logger = LoggerFactory.getLogger(ByteBufferPool.class);

    /** Default singleton instance for convenient access. */
    private static final ByteBufferPool DEFAULT_INSTANCE = new ByteBufferPool();

    /** Pool of direct ByteBuffers. */
    private final ConcurrentLinkedQueue<ByteBuffer> directBuffers = new ConcurrentLinkedQueue<>();

    /** Pool of heap ByteBuffers. */
    private final ConcurrentLinkedQueue<ByteBuffer> heapBuffers = new ConcurrentLinkedQueue<>();

    /** Current count of pooled direct buffers. */
    private final AtomicInteger directBufferCount = new AtomicInteger(0);

    /** Current count of pooled heap buffers. */
    private final AtomicInteger heapBufferCount = new AtomicInteger(0);

    /** Maximum number of direct buffers to pool. */
    private final int maxDirectBuffers;

    /** Maximum number of heap buffers to pool. */
    private final int maxHeapBuffers;

    /** Block size for allocation alignment and minimum pooling threshold. */
    private final int blockSize;

    /** Statistics: total buffers created. */
    private final AtomicLong buffersCreated = new AtomicLong(0);

    /** Statistics: total buffers reused from pool. */
    private final AtomicLong buffersReused = new AtomicLong(0);

    /** Statistics: total buffers returned to pool. */
    private final AtomicLong buffersReturned = new AtomicLong(0);

    /** Statistics: total buffers discarded (pool full or too small). */
    private final AtomicLong buffersDiscarded = new AtomicLong(0);

    /** Default maximum number of direct buffers to pool. */
    public static final int DEFAULT_MAX_DIRECT_BUFFERS = 32;

    /** Default maximum number of heap buffers to pool. */
    public static final int DEFAULT_MAX_HEAP_BUFFERS = 64;

    /** Default block size for allocation alignment and pooling (8KB). */
    public static final int DEFAULT_BLOCK_SIZE = 8192;

    /**
     * Creates a new ByteBuffer pool with default settings.
     */
    public ByteBufferPool() {
        this(DEFAULT_MAX_DIRECT_BUFFERS, DEFAULT_MAX_HEAP_BUFFERS, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a new ByteBuffer pool with custom settings.
     *
     * @param maxDirectBuffers maximum number of direct buffers to pool
     * @param maxHeapBuffers maximum number of heap buffers to pool
     * @param blockSize block size for allocation alignment and minimum pooling threshold (bytes)
     * @throws IllegalArgumentException if any parameter is negative or zero
     */
    public ByteBufferPool(int maxDirectBuffers, int maxHeapBuffers, int blockSize) {
        if (maxDirectBuffers <= 0) {
            throw new IllegalArgumentException("maxDirectBuffers must be positive: " + maxDirectBuffers);
        }
        if (maxHeapBuffers <= 0) {
            throw new IllegalArgumentException("maxHeapBuffers must be positive: " + maxHeapBuffers);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive: " + blockSize);
        }

        this.maxDirectBuffers = maxDirectBuffers;
        this.maxHeapBuffers = maxHeapBuffers;
        this.blockSize = blockSize;

        logger.debug(
                "Created ByteBufferPool: maxDirect={}, maxHeap={}, blockSize={}",
                maxDirectBuffers,
                maxHeapBuffers,
                blockSize);
    }

    /**
     * Gets the default shared ByteBuffer pool instance.
     * <p>
     * This is convenient for most use cases and reduces the need to pass
     * pool instances around. The default pool uses conservative limits
     * suitable for typical applications.
     *
     * @return the default ByteBuffer pool
     */
    public static ByteBufferPool getDefault() {
        return DEFAULT_INSTANCE;
    }

    /**
     * Borrows a direct ByteBuffer with at least the specified capacity.
     * <p>
     * Returns a {@link PooledByteBuffer} that must be used in a try-with-resources
     * block to ensure proper return to the pool. The {@link PooledByteBuffer#buffer()}
     * method returns a ByteBuffer with capacity matching the requested size exactly.
     * <p>
     * For optimal memory alignment and performance, internal buffers are allocated
     * with sizes that are multiples of the configured block size, but the returned buffer
     * will be a slice matching your requested capacity.
     *
     * <pre>{@code
     * try (var pooled = pool.borrowDirect(8192)) {
     *     ByteBuffer buffer = pooled.buffer();
     *     // Use buffer for I/O operations
     *     channel.read(buffer);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a direct ByteBuffer with the requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    public PooledByteBuffer borrowDirect(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("minCapacity cannot be negative: " + size);
        }

        // Try to find a suitable buffer in the pool
        ByteBuffer buffer = findSuitableBuffer(directBuffers, size, true);
        if (buffer != null) {
            buffersReused.incrementAndGet();
            logger.trace("Reused direct buffer: capacity={}", buffer.capacity());
        } else {
            // Create new direct buffer with size rounded up to multiple of block size
            int alignedCapacity = roundUpToBlockSize(size);
            buffer = ByteBuffer.allocateDirect(alignedCapacity);
            buffersCreated.incrementAndGet();
            logger.trace("Created new direct buffer: requested={}, aligned={}", size, alignedCapacity);
        }
        buffer.clear().limit(size);
        if (size == buffer.capacity()) {
            return new PooledByteBufferImpl(buffer, buffer, this);
        }
        return new PooledByteBufferImpl(buffer, buffer.slice(), this);
    }

    /**
     * Convenience method to borrow a direct ByteBuffer from the default pool.
     * <p>
     * This is equivalent to {@code ByteBufferPool.getDefault().borrowDirect(size)}.
     * The returned {@link PooledByteBuffer} must be used in a try-with-resources
     * block to ensure proper return to the pool.
     *
     * <pre>{@code
     * try (var pooled = ByteBufferPool.directBuffer(8192)) {
     *     ByteBuffer buffer = pooled.buffer();
     *     // Use buffer for I/O operations
     *     channel.read(buffer);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a direct ByteBuffer with at least the requested capacity
     * @throws IllegalArgumentException if size is negative
     * @see #borrowDirect(int)
     */
    public static PooledByteBuffer directBuffer(int size) {
        return getDefault().borrowDirect(size);
    }

    /**
     * Convenience method to borrow a heap ByteBuffer from the default pool.
     * <p>
     * This is equivalent to {@code ByteBufferPool.getDefault().borrowHeap(size)}.
     * The returned {@link PooledByteBuffer} must be used in a try-with-resources
     * block to ensure proper return to the pool.
     *
     * <pre>{@code
     * try (var pooled = ByteBufferPool.heapBuffer(8192)) {
     *     ByteBuffer buffer = pooled.buffer();
     *     // Use buffer for in-memory processing
     *     buffer.put(data);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a heap ByteBuffer with at least the requested capacity
     * @throws IllegalArgumentException if size is negative
     * @see #borrowHeap(int)
     */
    public static PooledByteBuffer heapBuffer(int size) {
        return getDefault().borrowHeap(size);
    }

    /**
     * Borrows a heap ByteBuffer with at least the specified capacity.
     * <p>
     * Returns a {@link PooledByteBuffer} that must be used in a try-with-resources
     * block to ensure proper return to the pool. The {@link PooledByteBuffer#buffer()}
     * method returns a ByteBuffer with capacity matching the requested size exactly.
     * <p>
     * For optimal memory alignment and performance, internal buffers are allocated
     * with sizes that are multiples of the configured block size, but the returned buffer
     * will be a slice matching your requested capacity.
     *
     * <pre>{@code
     * try (var pooled = pool.borrowHeap(4096)) {
     *     ByteBuffer buffer = pooled.buffer();
     *     // Use buffer for in-memory processing
     *     buffer.put(data);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a heap ByteBuffer with the requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    public PooledByteBuffer borrowHeap(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("minCapacity cannot be negative: " + size);
        }

        // Try to find a suitable buffer in the pool
        ByteBuffer buffer = findSuitableBuffer(heapBuffers, size, false);
        if (buffer != null) {
            buffersReused.incrementAndGet();
            logger.trace("Reused heap buffer: capacity={}", buffer.capacity());
        } else {
            // Create new heap buffer
            int alignedCapacity = roundUpToBlockSize(size);
            buffer = ByteBuffer.allocate(alignedCapacity);
            buffersCreated.incrementAndGet();
            logger.trace("Created new heap buffer: capacity={}", buffer.capacity());
        }
        buffer.clear().limit(size);
        if (size == buffer.capacity()) {
            return new PooledByteBufferImpl(buffer, buffer, this);
        }
        return new PooledByteBufferImpl(buffer, buffer.slice(), this);
    }

    /**
     * Returns a ByteBuffer to the pool for potential reuse.
     * <p>
     * The buffer will be cleared and added to the appropriate pool if there
     * is space available. If the pool is full or the buffer is too small,
     * it will be discarded.
     * <p>
     * <strong>Important:</strong> After calling this method, the caller should
     * not use the buffer anymore, as it may be reused by other threads.
     *
     * @param buffer the buffer to return (may be null, in which case this is a no-op)
     */
    protected void returnBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        // Clear the buffer for reuse
        buffer.clear();

        // Check if buffer is large enough to pool
        if (buffer.capacity() < blockSize) {
            buffersDiscarded.incrementAndGet();
            logger.trace("Discarded buffer (too small): capacity={}, blockSize={}", buffer.capacity(), blockSize);
            return;
        }

        // Add to appropriate pool if there's space
        if (buffer.isDirect()) {
            if (directBufferCount.get() < maxDirectBuffers) {
                directBuffers.offer(buffer);
                directBufferCount.incrementAndGet();
                buffersReturned.incrementAndGet();
                logger.trace("Returned direct buffer to pool: capacity={}", buffer.capacity());
            } else {
                buffersDiscarded.incrementAndGet();
                logger.trace("Discarded direct buffer (pool full): capacity={}", buffer.capacity());
            }
        } else {
            if (heapBufferCount.get() < maxHeapBuffers) {
                heapBuffers.offer(buffer);
                heapBufferCount.incrementAndGet();
                buffersReturned.incrementAndGet();
                logger.trace("Returned heap buffer to pool: capacity={}", buffer.capacity());
            } else {
                buffersDiscarded.incrementAndGet();
                logger.trace("Discarded heap buffer (pool full): capacity={}", buffer.capacity());
            }
        }
    }

    /**
     * Clears all pooled buffers, releasing their memory.
     * <p>
     * This method is useful for cleanup or when you want to start with
     * a fresh pool. After calling this method, the pool will be empty
     * and subsequent borrow operations will create new buffers.
     */
    public void clear() {
        int directCleared = 0;
        int heapCleared = 0;

        // Clear direct buffers
        while (!directBuffers.isEmpty()) {
            if (directBuffers.poll() != null) {
                directCleared++;
            }
        }
        directBufferCount.set(0);

        // Clear heap buffers
        while (!heapBuffers.isEmpty()) {
            if (heapBuffers.poll() != null) {
                heapCleared++;
            }
        }
        heapBufferCount.set(0);

        logger.debug("Cleared pool: {} direct buffers, {} heap buffers", directCleared, heapCleared);
    }

    /**
     * Gets statistics about pool usage.
     *
     * @return pool statistics
     */
    public PoolStatistics getStatistics() {
        return new PoolStatistics(
                directBufferCount.get(),
                heapBufferCount.get(),
                maxDirectBuffers,
                maxHeapBuffers,
                buffersCreated.get(),
                buffersReused.get(),
                buffersReturned.get(),
                buffersDiscarded.get());
    }

    /**
     * Rounds up the given capacity to the next multiple of the block size.
     * This provides better memory alignment and can improve performance.
     *
     * @param capacity the capacity to round up
     * @return the capacity rounded up to the next multiple of block size
     */
    private int roundUpToBlockSize(int capacity) {
        return ((capacity + blockSize - 1) / blockSize) * blockSize;
    }

    /**
     * Finds a suitable buffer from the given pool.
     *
     * @param pool the pool to search
     * @param minCapacity minimum required capacity
     * @param isDirect whether we're looking for direct buffers
     * @return a suitable buffer, or null if none found
     */
    private ByteBuffer findSuitableBuffer(ConcurrentLinkedQueue<ByteBuffer> pool, int minCapacity, boolean isDirect) {
        // Simple strategy: take the first buffer that's large enough
        // This could be optimized with size-based pools if needed
        ByteBuffer buffer;
        while ((buffer = pool.poll()) != null) {
            if (isDirect) {
                directBufferCount.decrementAndGet();
            } else {
                heapBufferCount.decrementAndGet();
            }

            if (buffer.capacity() >= minCapacity) {
                buffer.clear();
                return buffer;
            } else {
                // Buffer too small, discard it
                buffersDiscarded.incrementAndGet();
                logger.trace(
                        "Discarded {} buffer (too small for request): capacity={}, required={}",
                        isDirect ? "direct" : "heap",
                        buffer.capacity(),
                        minCapacity);
            }
        }
        return null;
    }

    @Override
    public String toString() {
        PoolStatistics stats = getStatistics();
        return String.format(
                "ByteBufferPool[direct=%d/%d, heap=%d/%d, created=%d, reused=%d, returned=%d, discarded=%d]",
                stats.currentDirectBuffers(),
                stats.maxDirectBuffers(),
                stats.currentHeapBuffers(),
                stats.maxHeapBuffers(),
                stats.buffersCreated(),
                stats.buffersReused(),
                stats.buffersReturned(),
                stats.buffersDiscarded());
    }

    /**
     * Immutable statistics snapshot for a ByteBuffer pool.
     *
     * @param currentDirectBuffers current number of pooled direct buffers
     * @param currentHeapBuffers current number of pooled heap buffers
     * @param maxDirectBuffers maximum number of direct buffers that can be pooled
     * @param maxHeapBuffers maximum number of heap buffers that can be pooled
     * @param buffersCreated total number of buffers created
     * @param buffersReused total number of buffers reused from pool
     * @param buffersReturned total number of buffers returned to pool
     * @param buffersDiscarded total number of buffers discarded (pool full or too small)
     */
    public record PoolStatistics(
            int currentDirectBuffers,
            int currentHeapBuffers,
            int maxDirectBuffers,
            int maxHeapBuffers,
            long buffersCreated,
            long buffersReused,
            long buffersReturned,
            long buffersDiscarded) {
        /**
         * Calculates the hit rate (percentage of borrow operations satisfied from pool).
         *
         * @return hit rate as a percentage (0.0 to 100.0)
         */
        public double hitRate() {
            long totalBorrows = buffersCreated + buffersReused;
            return totalBorrows > 0 ? (buffersReused * 100.0) / totalBorrows : 0.0;
        }

        /**
         * Calculates the return rate (percentage of returned buffers that were pooled).
         *
         * @return return rate as a percentage (0.0 to 100.0)
         */
        public double returnRate() {
            long totalReturns = buffersReturned + buffersDiscarded;
            return totalReturns > 0 ? (buffersReturned * 100.0) / totalReturns : 0.0;
        }
    }

    /**
     * A wrapper around a pooled {@link ByteBuffer} that implements {@link AutoCloseable}
     * to enable try-with-resources syntax for automatic buffer return to the pool.
     * <p>
     * This interface ensures that borrowed buffers are always returned to the pool,
     * preventing resource leaks and maintaining optimal pool performance.
     *
     * <p><strong>Usage Example:</strong></p>
     * <pre>{@code
     * try (var pooled = ByteBufferPool.directBuffer(8192)) {
     *     ByteBuffer buffer = pooled.buffer();
     *     // Use buffer
     *     channel.read(buffer);
     *     buffer.flip();
     *     // Process data
     * } // Buffer automatically returned to pool here
     * }</pre>
     *
     * @since 1.1
     */
    public static interface PooledByteBuffer extends AutoCloseable {
        /**
         * Returns the underlying {@link ByteBuffer} for use.
         * <p>
         * The returned buffer's capacity will match the requested size (not the pooled
         * buffer's capacity, which may be larger). The buffer's position will be 0
         * and its limit will be set to the requested capacity.
         * <p>
         * <strong>Important:</strong> The buffer should only be used within the
         * try-with-resources block. Do not store references to the buffer beyond
         * the scope of the try block.
         *
         * @return the underlying ByteBuffer, ready for use
         */
        public ByteBuffer buffer();

        /**
         * Returns the buffer to the pool.
         * <p>
         * This method is called automatically when exiting a try-with-resources block.
         * After calling this method, the buffer should not be used anymore.
         */
        @Override
        void close();
    }

    // visible for testing
    static record PooledByteBufferImpl(ByteBuffer pooled, ByteBuffer slice, ByteBufferPool pool)
            implements PooledByteBuffer {

        @Override
        public void close() {
            pool.returnBuffer(this.pooled);
        }

        @Override
        public ByteBuffer buffer() {
            return slice;
        }
    }
}
