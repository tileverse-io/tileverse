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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread-safe pool of {@link ByteBuffer} instances to reduce garbage
 * collection pressure in range readers that require temporary buffers.
 * <p>
 * This pool manages direct and heap ByteBuffers separately, allowing efficient
 * reuse of buffers for different use cases. Direct buffers are typically used
 * for I/O operations, while heap buffers are used for in-memory processing.
 * <p>
 * <strong>Thread Safety:</strong> This class is fully thread-safe and can be
 * used concurrently by multiple threads without external synchronization.
 * <p>
 * <strong>Memory Management:</strong> The pool automatically limits the number
 * of pooled buffers to prevent unbounded memory growth. Buffers that exceed the
 * pool capacity are simply discarded when returned.
 * <p>
 * <strong>Buffer Lifecycle:</strong> Borrowed buffers are wrapped in a
 * {@link PooledByteBuffer} that automatically returns the buffer to the pool
 * when closed. Always use try-with-resources to ensure proper cleanup.
 *
 * <p>
 * <strong>Usage Example (Recommended):</strong>
 * </p>
 *
 * <pre>{@code
 * // Using static convenience methods with default pool
 * try (var pooled = ByteBufferPool.directBuffer(8192)) {
 * 	ByteBuffer buffer = pooled.buffer();
 * 	// Use buffer for I/O operations
 * 	channel.read(buffer);
 * 	buffer.flip();
 * 	// Process data...
 * } // Buffer automatically returned to pool
 * }</pre>
 *
 * <p>
 * <strong>Custom Pool Configuration:</strong>
 * </p>
 *
 * <pre>{@code
 * // Create custom pool with specific limits
 * ByteBufferPool customPool = new ByteBufferPool(50, // maxDirectBuffers
 * 		100, // maxHeapBuffers
 * 		1024 // blockSize
 * );
 *
 * try (var pooled = customPool.borrowDirect(65536)) {
 * 	ByteBuffer buffer = pooled.buffer();
 * 	// Use large buffer...
 * } // Automatically returned to pool
 * }</pre>
 *
 * <p>
 * <strong>Buffer Capacity:</strong>
 * </p>
 * The {@link PooledByteBuffer#buffer()} method returns a buffer whose capacity
 * matches the requested size exactly. Internally, the pool may allocate larger
 * buffers (rounded to block size boundaries for alignment), but the returned
 * buffer is a slice that matches your requested capacity.
 *
 * @see PooledByteBuffer
 * @see #directBuffer(int)
 * @see #heapBuffer(int)
 */
public class ByteBufferPool {

    private static final Logger logger = LoggerFactory.getLogger(ByteBufferPool.class);

    /** Default singleton instance for convenient access. */
    private static final ByteBufferPool DEFAULT_INSTANCE = new ByteBufferPool();

    /** Default maximum number of direct buffers to pool. */
    public static final int DEFAULT_MAX_DIRECT_BUFFERS = 32;

    /** Default maximum number of heap buffers to pool. */
    public static final int DEFAULT_MAX_HEAP_BUFFERS = 64;

    /** Default block size for allocation alignment and pooling (8KB). */
    public static final int DEFAULT_BLOCK_SIZE = 8192;

    /** Pool of direct ByteBuffers. */
    private final BufferQueue directBuffers;

    /** Pool of heap ByteBuffers. */
    private final BufferQueue heapBuffers;

    /** Block size for allocation alignment and minimum pooling threshold. */
    private final int blockSize;

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
     * @param maxHeapBuffers   maximum number of heap buffers to pool
     * @param blockSize        block size for allocation alignment and minimum
     *                         pooling threshold (bytes)
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

        this.blockSize = blockSize;

        // Initialize buffer queues
        // Use Integer.MAX_VALUE for max individual buffer size effectively allowing any
        // size
        // to be pooled as long as it fits in the count limit
        this.directBuffers = new BufferQueue(
                maxDirectBuffers,
                blockSize,
                Integer.MAX_VALUE,
                ByteBuffer::allocateDirect,
                DirectByteBufferCleaner::releaseDirectBuffer); // Use cleaner for direct buffers

        this.heapBuffers = new BufferQueue(
                maxHeapBuffers,
                blockSize,
                Integer.MAX_VALUE,
                ByteBuffer::allocate,
                b -> {}); // No-op for heap buffers (GC handles them)

        logger.debug(
                "Created ByteBufferPool: maxDirect={}, maxHeap={}, blockSize={}",
                maxDirectBuffers,
                maxHeapBuffers,
                blockSize);
    }

    /**
     * Gets the default shared ByteBuffer pool instance.
     * <p>
     * This is convenient for most use cases and reduces the need to pass pool
     * instances around. The default pool uses conservative limits suitable for
     * typical applications.
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
     * block to ensure proper return to the pool. The
     * {@link PooledByteBuffer#buffer()} method returns a ByteBuffer with capacity
     * matching the requested size exactly.
     * <p>
     * For optimal memory alignment and performance, internal buffers are allocated
     * with sizes that are multiples of the configured block size, but the returned
     * buffer will be a slice matching your requested capacity.
     *
     * <pre>{@code
     * try (var pooled = pool.borrowDirect(8192)) {
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for I/O operations
     * 	channel.read(buffer);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a direct ByteBuffer with the
     *         requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    public PooledByteBuffer borrowDirect(int size) {
        return borrow(directBuffers, size);
    }

    /**
     * Borrows a heap ByteBuffer with at least the specified capacity.
     * <p>
     * Returns a {@link PooledByteBuffer} that must be used in a try-with-resources
     * block to ensure proper return to the pool. The
     * {@link PooledByteBuffer#buffer()} method returns a ByteBuffer with capacity
     * matching the requested size exactly.
     * <p>
     * For optimal memory alignment and performance, internal buffers are allocated
     * with sizes that are multiples of the configured block size, but the returned
     * buffer will be a slice matching your requested capacity.
     *
     * <pre>{@code
     * try (var pooled = pool.borrowHeap(4096)) {
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for in-memory processing
     * 	buffer.put(data);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a heap ByteBuffer with the
     *         requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    public PooledByteBuffer borrowHeap(int size) {
        return borrow(heapBuffers, size);
    }

    private PooledByteBuffer borrow(BufferQueue pool, int size) {
        if (size < 0) {
            throw new IllegalArgumentException("minCapacity cannot be negative: " + size);
        }

        int alignedCapacity = roundUpToBlockSize(size);
        ByteBuffer buffer = pool.getBuffer(alignedCapacity);
        buffer.limit(size);
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
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for I/O operations
     * 	channel.read(buffer);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a direct ByteBuffer with at least
     *         the requested capacity
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
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for in-memory processing
     * 	buffer.put(data);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a heap ByteBuffer with at least
     *         the requested capacity
     * @throws IllegalArgumentException if size is negative
     * @see #borrowHeap(int)
     */
    public static PooledByteBuffer heapBuffer(int size) {
        return getDefault().borrowHeap(size);
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
     * Returns a ByteBuffer to the pool for potential reuse.
     * <p>
     * The buffer will be cleared and added to the appropriate pool if there is
     * space available. If the pool is full or the buffer is too small, it will be
     * discarded.
     * <p>
     * <strong>Important:</strong> After calling this method, the caller should not
     * use the buffer anymore, as it may be reused by other threads.
     *
     * @param buffer the buffer to return (may be null, in which case this is a
     *               no-op)
     */
    protected void returnBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        boolean returned;
        if (buffer.isDirect()) {
            returned = directBuffers.returnBuffer(buffer);
        } else {
            returned = heapBuffers.returnBuffer(buffer);
        }

        if (returned) {
            logger.trace("Returned buffer to pool: capacity={}", buffer.capacity());
        } else {
            logger.trace("Discarded buffer (pool full): capacity={}", buffer.capacity());
        }
    }

    /**
     * Clears all pooled buffers, releasing their memory.
     * <p>
     * This method is useful for cleanup or when you want to start with a fresh
     * pool. After calling this method, the pool will be empty and subsequent borrow
     * operations will create new buffers.
     */
    public void clear() {
        try {
            directBuffers.clear();
        } finally {
            heapBuffers.clear();
        }
        logger.debug("Cleared pool");
    }

    /**
     * Gets statistics about pool usage.
     *
     * @return pool statistics
     */
    public PoolStatistics getStatistics() {
        return getHeapPoolStatistics().add(getDirectPoolStatistics());
    }

    public PoolStatistics getHeapPoolStatistics() {
        return heapBuffers.stats();
    }

    public PoolStatistics getDirectPoolStatistics() {
        return directBuffers.stats();
    }

    @Override
    public String toString() {
        String identityString = getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this));
        PoolStatistics heapPoolStatistics = getHeapPoolStatistics();
        PoolStatistics directPoolStatistics = getDirectPoolStatistics();
        return String.format(
                "%s[heap buffers: %s, direct buffers: %s]", identityString, heapPoolStatistics, directPoolStatistics);
    }

    /**
     * Immutable statistics snapshot for a ByteBuffer pool.
     */
    public record PoolStatistics(
            int maxSize, int poolSize, long bytesSize, long created, long reused, long returned, long discarded) {

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
     * A wrapper around a pooled {@link ByteBuffer} that implements
     * {@link AutoCloseable} to enable try-with-resources syntax for automatic
     * buffer return to the pool.
     */
    public static interface PooledByteBuffer extends AutoCloseable {
        public ByteBuffer buffer();

        @Override
        void close();
    }

    // visible for testing
    static class PooledByteBufferImpl implements PooledByteBuffer {

        private ByteBuffer pooled;
        private ByteBuffer slice;
        private ByteBufferPool pool;

        PooledByteBufferImpl(ByteBuffer pooled, ByteBuffer slice, ByteBufferPool pool) {
            this.pooled = pooled;
            this.slice = slice;
            this.pool = pool;
        }

        @Override
        public void close() {
            if (pooled == null) {
                return;
            }
            pool.returnBuffer(this.pooled);
            pooled = null;
            slice = null;
            pool = null;
        }

        @Override
        public ByteBuffer buffer() {
            return slice;
        }

        // visible for testing
        ByteBuffer pooled() {
            return pooled;
        }
    }

    private static class BufferQueue {

        private final List<ByteBuffer> buffers;
        private final Lock lock = new ReentrantLock();

        /** Statistics: total bytes used by buffers in the pool. */
        final AtomicLong bytesSize = new AtomicLong(0);

        /** Statistics: total buffers created. */
        final AtomicLong buffersCreated = new AtomicLong(0);

        /** Statistics: total buffers reused from pool. */
        final AtomicLong buffersReused = new AtomicLong(0);

        /** Statistics: total buffers returned to pool. */
        final AtomicLong buffersReturned = new AtomicLong(0);

        /** Statistics: total buffers discarded (pool full or too small). */
        final AtomicLong buffersDiscarded = new AtomicLong(0);

        /** Statistics: total bytes discarded (pool full or too small). */
        final AtomicLong discardedSize = new AtomicLong(0);

        private final int queueCapacity;
        private final int minBufferCapacity;
        private final int maxBufferCapacity;
        private final IntFunction<ByteBuffer> factory;
        private final Consumer<ByteBuffer> evictionListener;

        /**
         *
         * @param queueCapacity     the maximum number of buffers to hold
         * @param minBufferCapacity minimum buffer capacity to keep in the pool
         * @param maxBufferCapacity the maximum size of a single byte buffer to be held
         * @param factory           function to create new buffers
         * @param evictionListener  listener called when a buffer is discarded/evicted
         */
        BufferQueue(
                int queueCapacity,
                int minBufferCapacity,
                int maxBufferCapacity,
                IntFunction<ByteBuffer> factory,
                Consumer<ByteBuffer> evictionListener) {
            this.queueCapacity = queueCapacity;
            this.minBufferCapacity = minBufferCapacity;
            this.maxBufferCapacity = maxBufferCapacity;
            this.factory = factory;
            this.evictionListener = evictionListener;
            this.buffers = new ArrayList<>();
        }

        public PoolStatistics stats() {
            lock.lock();
            try {
                return new PoolStatistics(
                        queueCapacity,
                        buffers.size(),
                        bytesSize.get(),
                        buffersCreated.get(),
                        buffersReused.get(),
                        buffersReturned.get(),
                        buffersDiscarded.get());
            } finally {
                lock.unlock();
            }
        }

        public int size() {
            lock.lock();
            try {
                return buffers.size();
            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            lock.lock();
            try {
                for (ByteBuffer buffer : buffers) {
                    evictionListener.accept(buffer);
                }
                buffers.clear();
                bytesSize.set(0);
                buffersCreated.set(0);
                buffersReused.set(0);
                buffersReturned.set(0);
                buffersDiscarded.set(0);
                discardedSize.set(0);
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns the smallest {@link ByteBuffer} whose capacity is greater than or
         * equal to the given key. Uses the provided factory function to create one if
         * there is no such key.
         */
        public ByteBuffer getBuffer(int minimumSize) {
            if (minimumSize > maxBufferCapacity) {
                return create(minimumSize);
            }

            lock.lock();
            try {
                // find first buffer with capacity >= minimumSize
                int position = binarySearchLowerBound(minimumSize);
                if (position >= 0 && position < buffers.size()) {
                    // found a suitable buffer
                    buffersReused.incrementAndGet();
                    ByteBuffer reused = buffers.remove(position).clear();
                    bytesSize.addAndGet(-reused.capacity());
                    return reused;
                }
            } finally {
                lock.unlock();
            }
            return create(minimumSize);
        }

        private ByteBuffer create(int size) {
            buffersCreated.incrementAndGet();
            return factory.apply(size);
        }

        /**
         *
         * @param returningBuffer
         * @return true if the buffer was returned, false if it was discarded
         */
        public boolean returnBuffer(ByteBuffer returningBuffer) {
            final int capacity = returningBuffer.capacity();
            if (capacity < minBufferCapacity || capacity > maxBufferCapacity) {
                buffersDiscarded.incrementAndGet();
                discardedSize.addAndGet(returningBuffer.capacity());
                evictionListener.accept(returningBuffer);
                return false;
            }

            lock.lock();
            try {
                returningBuffer.clear();
                if (buffers.size() == queueCapacity) {
                    ByteBuffer smallest = buffers.get(0);

                    // If the returning buffer is not larger than the smallest buffer we already
                    // have, there is no point in keeping it (we prefer larger buffers).
                    if (capacity <= smallest.capacity()) {
                        buffersDiscarded.incrementAndGet();
                        discardedSize.addAndGet(returningBuffer.capacity());
                        evictionListener.accept(returningBuffer);
                        return false;
                    } else {
                        // Evict the smallest to make room for this larger one
                        ByteBuffer removed = buffers.remove(0);
                        bytesSize.addAndGet(-removed.capacity());
                        buffersDiscarded.incrementAndGet();
                        discardedSize.addAndGet(removed.capacity());
                        evictionListener.accept(smallest);
                    }
                }

                // Add buffer in sorted order
                int insertionPoint = binarySearchLowerBound(capacity);
                buffers.add(insertionPoint, returningBuffer);
                bytesSize.addAndGet(returningBuffer.capacity());
                buffersReturned.incrementAndGet();
                return true;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Finds the index of the first {@link ByteBuffer} in the sorted list whose
         * capacity is greater than or equal to the {@code minimumSize}. This is also
         * known as a "lower bound" search.
         *
         * @param minimumSize the minimum capacity to search for.
         * @return The index of the first buffer with capacity >= {@code minimumSize}.
         *         If no such buffer is found (i.e., all buffers have smaller capacity),
         *         it returns {@code buffers.size()}, which is a valid insertion point.
         *         This method guarantees a non-negative return value.
         */
        private int binarySearchLowerBound(final int minimumSize) {
            int low = 0;
            int high = buffers.size(); // exclusive upper bound

            while (low < high) {
                int mid = (low + high) >>> 1;
                if (buffers.get(mid).capacity() >= minimumSize) {
                    high = mid; // Try to find a smaller index in the left half
                } else {
                    low = mid + 1; // It must be in the right half
                }
            }
            return low;
        }
    }

    private static final class DirectByteBufferCleaner {
        // internal unsafe instance for direct buffer cleanup
        private static final Object UNSAFE;
        private static final java.lang.reflect.Method INVOKE_CLEANER;

        static {
            Object unsafe = null;
            java.lang.reflect.Method invokeCleaner = null;
            try {
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                java.lang.reflect.Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                unsafe = theUnsafe.get(null);

                // Available in Java 9+
                try {
                    invokeCleaner = unsafeClass.getMethod("invokeCleaner", java.nio.ByteBuffer.class);
                } catch (NoSuchMethodException e) {
                    // Fallback for Java 8 not needed since we are on Java 17, but good for safety
                }
            } catch (Exception e) {
                logger.warn(
                        "Could not get access to sun.misc.Unsafe. Direct buffer memory release will rely on GC.", e);
            }
            UNSAFE = unsafe;
            INVOKE_CLEANER = invokeCleaner;
        }

        private DirectByteBufferCleaner() {
            // no-op
        }

        /**
         * Attempts to immediately release the memory of a direct ByteBuffer.
         *
         * @param buffer the buffer to release
         */
        static void releaseDirectBuffer(ByteBuffer buffer) {
            if (buffer == null || !buffer.isDirect()) {
                return;
            }

            if (UNSAFE != null && INVOKE_CLEANER != null) {
                try {
                    INVOKE_CLEANER.invoke(UNSAFE, buffer);
                    return;
                } catch (Exception e) {
                    // log once or debug
                    logger.debug("Failed to invoke cleaner via Unsafe", e);
                }
            }

            // Fallback to classic reflection (Java 8 style) or if Unsafe failed
            // This will likely fail on Java 17+ without --add-opens, but we try anyway
            try {
                java.lang.reflect.Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                cleanerMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(buffer);

                if (cleaner != null) {
                    java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                    cleanMethod.setAccessible(true);
                    cleanMethod.invoke(cleaner);
                }
            } catch (Exception e) {
                // InaccessibleObjectException (Java 9+) or other errors
                logger.debug("Failed to release direct buffer memory via reflection", e);
            }
        }
    }
}
