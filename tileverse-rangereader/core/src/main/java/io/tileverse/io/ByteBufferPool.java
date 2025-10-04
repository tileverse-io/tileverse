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
 * <strong>Buffer Lifecycle:</strong> Borrowed buffers are automatically cleared
 * (position=0, limit=capacity) before being returned to callers. Returned buffers
 * are validated and cleaned before being added back to the pool.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * ByteBufferPool pool = ByteBufferPool.getDefault();
 *
 * // Borrow a direct buffer for I/O
 * ByteBuffer buffer = pool.borrowDirect(8192);
 * try {
 *     // Use buffer for I/O operations
 *     channel.read(buffer);
 *     buffer.flip();
 *     // Process data...
 * } finally {
 *     // Always return buffers in a finally block
 *     pool.returnBuffer(buffer);
 * }
 * }</pre>
 *
 * <p><strong>Custom Pool Configuration:</strong></p>
 * <pre>{@code
 * // Create custom pool with specific limits
 * ByteBufferPool customPool = new ByteBufferPool(
 *     50,    // maxDirectBuffers
 *     100,   // maxHeapBuffers
 *     1024   // minBufferSize
 * );
 *
 * ByteBuffer largeBuffer = customPool.borrowDirect(65536);
 * try {
 *     // Use large buffer...
 * } finally {
 *     customPool.returnBuffer(largeBuffer);
 * }
 * }</pre>
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

    /** Minimum buffer size to pool (smaller buffers are discarded). */
    private final int minBufferSize;

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

    /** Default minimum buffer size to pool (4KB). */
    public static final int DEFAULT_MIN_BUFFER_SIZE = 4096;

    /**
     * Creates a new ByteBuffer pool with default settings.
     */
    public ByteBufferPool() {
        this(DEFAULT_MAX_DIRECT_BUFFERS, DEFAULT_MAX_HEAP_BUFFERS, DEFAULT_MIN_BUFFER_SIZE);
    }

    /**
     * Creates a new ByteBuffer pool with custom settings.
     *
     * @param maxDirectBuffers maximum number of direct buffers to pool
     * @param maxHeapBuffers maximum number of heap buffers to pool
     * @param minBufferSize minimum buffer size to pool (bytes)
     * @throws IllegalArgumentException if any parameter is negative or zero
     */
    public ByteBufferPool(int maxDirectBuffers, int maxHeapBuffers, int minBufferSize) {
        if (maxDirectBuffers <= 0) {
            throw new IllegalArgumentException("maxDirectBuffers must be positive: " + maxDirectBuffers);
        }
        if (maxHeapBuffers <= 0) {
            throw new IllegalArgumentException("maxHeapBuffers must be positive: " + maxHeapBuffers);
        }
        if (minBufferSize <= 0) {
            throw new IllegalArgumentException("minBufferSize must be positive: " + minBufferSize);
        }

        this.maxDirectBuffers = maxDirectBuffers;
        this.maxHeapBuffers = maxHeapBuffers;
        this.minBufferSize = minBufferSize;

        logger.debug(
                "Created ByteBufferPool: maxDirect={}, maxHeap={}, minSize={}",
                maxDirectBuffers,
                maxHeapBuffers,
                minBufferSize);
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
     * The returned buffer will be cleared (position=0, limit=capacity) and
     * ready for use. The buffer may have a larger capacity than requested
     * if a suitable buffer was available in the pool.
     * <p>
     * For optimal memory alignment and performance, new buffers are created
     * with sizes that are multiples of 8KB (8192 bytes).
     *
     * @param size minimum required capacity in bytes
     * @return a direct ByteBuffer with at least the requested capacity, and the limit set to the requested {@code size}
     * @throws IllegalArgumentException if minCapacity is negative
     */
    public ByteBuffer borrowDirect(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("minCapacity cannot be negative: " + size);
        }

        // Try to find a suitable buffer in the pool
        ByteBuffer buffer = findSuitableBuffer(directBuffers, size, true);
        if (buffer != null) {
            buffersReused.incrementAndGet();
            logger.trace("Reused direct buffer: capacity={}", buffer.capacity());
        } else {
            // Create new direct buffer with size rounded up to multiple of 8KB
            int alignedCapacity = roundUpTo8KB(size);
            buffer = ByteBuffer.allocateDirect(alignedCapacity);
            buffersCreated.incrementAndGet();
            logger.trace("Created new direct buffer: requested={}, aligned={}", size, alignedCapacity);
        }
        return buffer.clear().limit(size);
    }

    /**
     * Borrows a heap ByteBuffer with at least the specified capacity.
     * <p>
     * The returned buffer will be cleared (position=0, limit=capacity) and
     * ready for use. The buffer may have a larger capacity than requested
     * if a suitable buffer was available in the pool.
     *
     * @param size minimum required capacity in bytes
     * @return a heap ByteBuffer with at least the requested capacity, and the limit set to the requested {@code capacity}
     * @throws IllegalArgumentException if minCapacity is negative
     */
    public ByteBuffer borrowHeap(int size) {
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
            int alignedCapacity = roundUpTo8KB(size);
            buffer = ByteBuffer.allocate(alignedCapacity);
            buffersCreated.incrementAndGet();
            logger.trace("Created new heap buffer: capacity={}", buffer.capacity());
        }
        return buffer.clear().limit(size);
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
    public void returnBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        // Clear the buffer for reuse
        buffer.clear();

        // Check if buffer is large enough to pool
        if (buffer.capacity() < minBufferSize) {
            buffersDiscarded.incrementAndGet();
            logger.trace("Discarded buffer (too small): capacity={}, minSize={}", buffer.capacity(), minBufferSize);
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
     * Rounds up the given capacity to the next multiple of 8KB (8192 bytes).
     * This provides better memory alignment and can improve performance.
     *
     * @param capacity the capacity to round up
     * @return the capacity rounded up to the next multiple of 8KB
     */
    private static int roundUpTo8KB(int capacity) {
        final int alignment = 8192; // 8KB alignment
        return ((capacity + alignment - 1) / alignment) * alignment;
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
}
