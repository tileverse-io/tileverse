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

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * A thread-safe pool of {@link ByteBuffer} instances to reduce garbage collection pressure in range readers that
 * require temporary buffers.
 *
 * <p>This pool manages direct and heap ByteBuffers separately, allowing efficient reuse of buffers for different use
 * cases. Direct buffers are typically used for I/O operations, while heap buffers are used for in-memory processing.
 *
 * <p><strong>Thread Safety:</strong> This class is fully thread-safe and can be used concurrently by multiple threads
 * without external synchronization.
 *
 * <p><strong>Memory Management:</strong> The pool automatically limits the number of pooled buffers to prevent
 * unbounded memory growth. Buffers that exceed the pool capacity are simply discarded when returned.
 *
 * <p><strong>Buffer Lifecycle:</strong> Borrowed buffers are wrapped in a {@link PooledByteBuffer} that automatically
 * returns the buffer to the pool when closed. Always use try-with-resources to ensure proper cleanup.
 *
 * <p><strong>Usage Example (Recommended):</strong>
 *
 * <pre>{@code
 * // Using static convenience methods with default pool
 * try (PooledByteBuffer pooled = ByteBufferPool.directBuffer(8192)) {
 * 	ByteBuffer buffer = pooled.buffer();
 * 	// Use buffer for I/O operations
 * 	channel.read(buffer);
 * 	buffer.flip();
 * 	// Process data...
 * } // Buffer automatically returned to pool
 * }</pre>
 *
 * <p><strong>Custom Pool Configuration:</strong>
 *
 * <pre>{@code
 * // Create custom pool with specific limits
 * ByteBufferPool customPool = new ByteBufferPool(50, // maxDirectBuffers
 * 		100, // maxHeapBuffers
 * 		1024 // blockSize
 * );
 *
 * try (PooledByteBuffer pooled = customPool.borrowDirect(65536)) {
 * 	ByteBuffer buffer = pooled.buffer();
 * 	// Use large buffer...
 * } // Automatically returned to pool
 * }</pre>
 *
 * <p><strong>Buffer Capacity:</strong> The {@link PooledByteBuffer#buffer()} method returns a buffer whose capacity
 * matches the requested size exactly. Internally, the pool may allocate larger buffers (rounded to block size
 * boundaries for alignment), but the returned buffer is a slice that matches your requested capacity.
 *
 * @see PooledByteBuffer
 * @see #directBuffer(int)
 * @see #heapBuffer(int)
 */
@Slf4j
public class ByteBufferPool {

    /** Default maximum number of direct buffers to pool. */
    public static final int DEFAULT_MAX_DIRECT_BUFFERS = 32;

    /** Default maximum number of heap buffers to pool. */
    public static final int DEFAULT_MAX_HEAP_BUFFERS = 64;

    /** Default block size for allocation alignment and pooling (8KB). */
    public static final int DEFAULT_BLOCK_SIZE = 8192;

    /** Default upper bound on direct bytes retained in the free list (64 MiB). */
    public static final long DEFAULT_MAX_DIRECT_BYTES = 64L * 1024 * 1024;

    /** Default upper bound on heap bytes retained in the free list (32 MiB). */
    public static final long DEFAULT_MAX_HEAP_BYTES = 32L * 1024 * 1024;

    /** Default idle period after which a quiet pool releases its whole free list. */
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(30);

    /** Minimum spacing between leak warnings, so a recurring leak does not flood the log. */
    private static final long LEAK_WARN_INTERVAL_NANOS = Duration.ofSeconds(60).toNanos();

    /** Default singleton instance, backed by the discovered allocation provider (or the built-in backend). */
    private static final ByteBufferPool DEFAULT_INSTANCE = new ByteBufferPool(
            DEFAULT_MAX_DIRECT_BUFFERS, DEFAULT_MAX_HEAP_BUFFERS, DEFAULT_BLOCK_SIZE, discoverAllocator());

    /** Allocation backend supplying and releasing the pooled buffers. */
    private final ByteBufferAllocator allocator;

    /** Pool of direct ByteBuffers. */
    private final BufferQueue directBuffers;

    /** Pool of heap ByteBuffers. */
    private final BufferQueue heapBuffers;

    /** Block size for allocation alignment and minimum pooling threshold. */
    private final int blockSize;

    /** Idle period before a quiet pool releases its free list, in nanoseconds; zero or less disables idle eviction. */
    private final long idleTimeoutNanos;

    /** Nanotime of the most recent borrow or return, used to detect inactivity. */
    private volatile long lastAccessNanos;

    /** Guards against scheduling more than one pending idle sweep at a time. */
    private final AtomicBoolean sweepScheduled = new AtomicBoolean(false);

    /** Whether borrowed buffers are watched for being garbage-collected without being closed. */
    private final boolean leakDetectionEnabled;

    /** Total number of borrows detected as leaked (garbage-collected without close). */
    private final AtomicLong leakCount = new AtomicLong(0);

    /** Nanotime of the most recent leak warning, used to rate-limit warnings. */
    private final AtomicLong lastLeakWarnNanos = new AtomicLong(Long.MIN_VALUE);

    /** Creates a new ByteBuffer pool with default settings. */
    public ByteBufferPool() {
        this(DEFAULT_MAX_DIRECT_BUFFERS, DEFAULT_MAX_HEAP_BUFFERS, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a new ByteBuffer pool with custom settings, using the built-in allocation backend.
     *
     * @param maxDirectBuffers maximum number of direct buffers to pool
     * @param maxHeapBuffers maximum number of heap buffers to pool
     * @param blockSize block size for allocation alignment and minimum pooling threshold (bytes)
     * @throws IllegalArgumentException if any parameter is negative or zero
     */
    public ByteBufferPool(int maxDirectBuffers, int maxHeapBuffers, int blockSize) {
        this(maxDirectBuffers, maxHeapBuffers, blockSize, BuiltinByteBufferAllocator.INSTANCE);
    }

    /**
     * Creates a new ByteBuffer pool with custom settings and an explicit allocation backend.
     *
     * <p>The {@code allocator} supplies every pooled buffer and releases it when the pool evicts or clears it. Use this
     * constructor to bind a pool to a specific backend; {@link #getDefault()} instead discovers one through
     * {@link ServiceLoader}.
     *
     * @param maxDirectBuffers maximum number of direct buffers to pool
     * @param maxHeapBuffers maximum number of heap buffers to pool
     * @param blockSize block size for allocation alignment and minimum pooling threshold (bytes)
     * @param allocator the allocation backend supplying and releasing pooled buffers
     * @throws IllegalArgumentException if any numeric parameter is negative or zero
     * @throws NullPointerException if {@code allocator} is null
     */
    public ByteBufferPool(int maxDirectBuffers, int maxHeapBuffers, int blockSize, ByteBufferAllocator allocator) {
        this(
                maxDirectBuffers,
                maxHeapBuffers,
                blockSize,
                allocator,
                DEFAULT_MAX_DIRECT_BYTES,
                DEFAULT_MAX_HEAP_BYTES,
                DEFAULT_IDLE_TIMEOUT,
                true);
    }

    /**
     * Canonical constructor wiring the full configuration. The byte budgets bound the bytes retained in the free list;
     * they never block or refuse a borrow, only what the pool keeps after a buffer is returned. The idle timeout
     * controls when a quiet pool releases its whole free list. Leak detection watches for borrows discarded without
     * being closed.
     */
    private ByteBufferPool(
            int maxDirectBuffers,
            int maxHeapBuffers,
            int blockSize,
            ByteBufferAllocator allocator,
            long maxDirectBytes,
            long maxHeapBytes,
            Duration idleTimeout,
            boolean leakDetectionEnabled) {
        if (maxDirectBuffers <= 0) {
            throw new IllegalArgumentException("maxDirectBuffers must be positive: " + maxDirectBuffers);
        }
        if (maxHeapBuffers <= 0) {
            throw new IllegalArgumentException("maxHeapBuffers must be positive: " + maxHeapBuffers);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive: " + blockSize);
        }
        if (maxDirectBytes <= 0) {
            throw new IllegalArgumentException("maxDirectBytes must be positive: " + maxDirectBytes);
        }
        if (maxHeapBytes <= 0) {
            throw new IllegalArgumentException("maxHeapBytes must be positive: " + maxHeapBytes);
        }
        if (allocator == null) {
            throw new NullPointerException("allocator cannot be null");
        }

        this.allocator = allocator;
        this.blockSize = blockSize;
        this.idleTimeoutNanos = idleTimeout == null ? 0L : idleTimeout.toNanos();
        this.lastAccessNanos = System.nanoTime();
        this.leakDetectionEnabled = leakDetectionEnabled;

        // Use Integer.MAX_VALUE for max individual buffer size, effectively allowing any
        // size to be pooled as long as it fits within the count and byte limits.
        this.directBuffers = new BufferQueue(
                maxDirectBuffers,
                blockSize,
                Integer.MAX_VALUE,
                maxDirectBytes,
                allocator::allocateDirect,
                allocator::free);

        this.heapBuffers = new BufferQueue(
                maxHeapBuffers, blockSize, Integer.MAX_VALUE, maxHeapBytes, allocator::allocateHeap, allocator::free);

        log.debug(
                "Created ByteBufferPool: maxDirect={} ({} bytes), maxHeap={} ({} bytes), blockSize={}, allocator={}",
                maxDirectBuffers,
                maxDirectBytes,
                maxHeapBuffers,
                maxHeapBytes,
                blockSize,
                allocator.getClass().getName());
    }

    /**
     * Creates a {@link Builder} for configuring a pool. Use this when you need to set the retention byte budgets or a
     * non-default allocation backend; the constructors cover the common cases.
     *
     * @return a new builder seeded with the default settings
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the default shared ByteBuffer pool instance.
     *
     * <p>This is convenient for most use cases and reduces the need to pass pool instances around. The default pool
     * uses conservative limits suitable for typical applications.
     *
     * @return the default ByteBuffer pool
     */
    public static ByteBufferPool getDefault() {
        return DEFAULT_INSTANCE;
    }

    /**
     * Discovers the allocation backend for the default pool through {@link ServiceLoader}, falling back to the built-in
     * backend when no provider is registered. A broken provider does not break the pool: discovery failures fall back
     * to the built-in backend.
     */
    private static ByteBufferAllocator discoverAllocator() {
        try {
            List<ByteBufferAllocator> providers = new ArrayList<>();
            for (ByteBufferAllocator provider : ServiceLoader.load(ByteBufferAllocator.class)) {
                providers.add(provider);
            }
            return selectAllocator(providers);
        } catch (RuntimeException | ServiceConfigurationError error) {
            log.warn("Failed to load a ByteBufferAllocator provider; using the built-in backend.", error);
            return BuiltinByteBufferAllocator.INSTANCE;
        }
    }

    /**
     * Picks the allocation backend from the discovered providers: the built-in backend when none are present, otherwise
     * the first provider (warning when more than one is registered, since the choice is then ambiguous).
     */
    // visible for testing
    static ByteBufferAllocator selectAllocator(List<ByteBufferAllocator> providers) {
        if (providers.isEmpty()) {
            return BuiltinByteBufferAllocator.INSTANCE;
        }
        ByteBufferAllocator selected = providers.get(0);
        if (providers.size() > 1) {
            log.warn(
                    "Found {} ByteBufferAllocator providers; using {}. Register at most one.",
                    providers.size(),
                    selected.getClass().getName());
        } else {
            log.info(
                    "Using ByteBufferAllocator provider {}", selected.getClass().getName());
        }
        return selected;
    }

    /** Returns the allocation backend supplying this pool's buffers. */
    // visible for testing
    ByteBufferAllocator allocator() {
        return allocator;
    }

    /**
     * Borrows a direct ByteBuffer with at least the specified capacity.
     *
     * <p>Returns a {@link PooledByteBuffer} that must be used in a try-with-resources block to ensure proper return to
     * the pool. The {@link PooledByteBuffer#buffer()} method returns a ByteBuffer with capacity matching the requested
     * size exactly.
     *
     * <p>For optimal memory alignment and performance, internal buffers are allocated with sizes that are multiples of
     * the configured block size, but the returned buffer will be a slice matching your requested capacity.
     *
     * <pre>{@code
     * try (PooledByteBuffer pooled = pool.borrowDirect(8192)) {
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for I/O operations
     * 	channel.read(buffer);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a direct ByteBuffer with the requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    public PooledByteBuffer borrowDirect(int size) {
        return borrow(directBuffers, size);
    }

    /**
     * Borrows a heap ByteBuffer with at least the specified capacity.
     *
     * <p>Returns a {@link PooledByteBuffer} that must be used in a try-with-resources block to ensure proper return to
     * the pool. The {@link PooledByteBuffer#buffer()} method returns a ByteBuffer with capacity matching the requested
     * size exactly.
     *
     * <p>For optimal memory alignment and performance, internal buffers are allocated with sizes that are multiples of
     * the configured block size, but the returned buffer will be a slice matching your requested capacity.
     *
     * <pre>{@code
     * try (PooledByteBuffer pooled = pool.borrowHeap(4096)) {
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for in-memory processing
     * 	buffer.put(data);
     * } // Automatically returned to pool
     * }</pre>
     *
     * @param size minimum required capacity in bytes
     * @return a {@link PooledByteBuffer} wrapping a heap ByteBuffer with the requested capacity
     * @throws IllegalArgumentException if size is negative
     */
    public PooledByteBuffer borrowHeap(int size) {
        return borrow(heapBuffers, size);
    }

    private PooledByteBuffer borrow(BufferQueue pool, int size) {
        if (size < 0) {
            throw new IllegalArgumentException("minCapacity cannot be negative: " + size);
        }

        recordActivity();
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
     *
     * <p>This is equivalent to {@code ByteBufferPool.getDefault().borrowDirect(size)}. The returned
     * {@link PooledByteBuffer} must be used in a try-with-resources block to ensure proper return to the pool.
     *
     * <pre>{@code
     * try (PooledByteBuffer pooled = ByteBufferPool.directBuffer(8192)) {
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for I/O operations
     * 	channel.read(buffer);
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
     *
     * <p>This is equivalent to {@code ByteBufferPool.getDefault().borrowHeap(size)}. The returned
     * {@link PooledByteBuffer} must be used in a try-with-resources block to ensure proper return to the pool.
     *
     * <pre>{@code
     * try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(8192)) {
     * 	ByteBuffer buffer = pooled.buffer();
     * 	// Use buffer for in-memory processing
     * 	buffer.put(data);
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
     * Rounds up the given capacity to the next multiple of the block size. This provides better memory alignment and
     * can improve performance.
     *
     * @param capacity the capacity to round up
     * @return the capacity rounded up to the next multiple of block size
     */
    private int roundUpToBlockSize(int capacity) {
        return ((capacity + blockSize - 1) / blockSize) * blockSize;
    }

    /**
     * Returns a ByteBuffer to the pool for potential reuse.
     *
     * <p>The buffer will be cleared and added to the appropriate pool if there is space available. If the pool is full
     * or the buffer is too small, it will be discarded.
     *
     * <p><strong>Important:</strong> After calling this method, the caller should not use the buffer anymore, as it may
     * be reused by other threads.
     *
     * @param buffer the buffer to return (may be null, in which case this is a no-op)
     */
    protected void returnBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        recordActivity();
        boolean returned;
        if (buffer.isDirect()) {
            returned = directBuffers.returnBuffer(buffer);
        } else {
            returned = heapBuffers.returnBuffer(buffer);
        }

        if (returned) {
            log.trace("Returned buffer to pool: capacity={}", buffer.capacity());
        } else {
            log.trace("Discarded buffer (pool full): capacity={}", buffer.capacity());
        }
    }

    /**
     * Records a borrow or return as activity and, when idle eviction is enabled, ensures a sweep is scheduled to
     * release the free list once the pool stays quiet for the idle timeout.
     */
    private void recordActivity() {
        if (idleTimeoutNanos <= 0) {
            return;
        }
        lastAccessNanos = System.nanoTime();
        if (sweepScheduled.compareAndSet(false, true)) {
            scheduleSweep(idleTimeoutNanos);
        }
    }

    private void scheduleSweep(long delayNanos) {
        IdleEvictionScheduler.EXECUTOR.schedule(this::sweepIfIdle, delayNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Releases the free list when the pool has been idle for at least the timeout; otherwise reschedules itself for the
     * remaining time. Runs on the shared eviction thread.
     */
    private void sweepIfIdle() {
        long idleNanos = System.nanoTime() - lastAccessNanos;
        if (idleNanos >= idleTimeoutNanos) {
            evictAllRetained();
            sweepScheduled.set(false); // the next borrow or return reschedules
        } else {
            scheduleSweep(idleTimeoutNanos - idleNanos);
        }
    }

    /** Releases every retained buffer in both queues, freeing their memory but keeping cumulative statistics. */
    // visible for testing
    void evictAllRetained() {
        directBuffers.evictAll();
        heapBuffers.evictAll();
    }

    /**
     * Records a leaked borrow (a {@link PooledByteBuffer} garbage-collected without {@link PooledByteBuffer#close()})
     * and warns about it, rate-limited so a recurring leak does not flood the log. The leak is only reported; the
     * buffer is not freed or re-pooled here, because the slice handed to the caller aliases the backing memory and the
     * JVM reclaims a truly unreachable direct buffer on its own.
     */
    private void recordLeak() {
        long total = leakCount.incrementAndGet();
        long now = System.nanoTime();
        long last = lastLeakWarnNanos.get();
        if (now - last >= LEAK_WARN_INTERVAL_NANOS && lastLeakWarnNanos.compareAndSet(last, now)) {
            log.warn(
                    "A pooled buffer was garbage-collected without being closed; a borrow site is missing a "
                            + "try-with-resources. Total leaks detected: {}.",
                    total);
        } else {
            log.debug("Pooled buffer leak detected; total {}", total);
        }
    }

    /**
     * Returns the number of borrows detected as leaked (garbage-collected without {@link PooledByteBuffer#close()}). A
     * non-zero value indicates a borrow site that does not close its buffer.
     *
     * @return the cumulative leak count
     */
    public long getLeakCount() {
        return leakCount.get();
    }

    /**
     * Clears all pooled buffers, releasing their memory.
     *
     * <p>This method is useful for cleanup or when you want to start with a fresh pool. After calling this method, the
     * pool will be empty and subsequent borrow operations will create new buffers.
     */
    public void clear() {
        try {
            directBuffers.clear();
        } finally {
            heapBuffers.clear();
        }
        log.debug("Cleared pool");
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
        return "%s[heap buffers: %s, direct buffers: %s]"
                .formatted(identityString, heapPoolStatistics, directPoolStatistics);
    }

    /**
     * Fluent configuration for a {@link ByteBufferPool}. Every setting defaults to the pool's documented default, so
     * only the knobs that matter for a given use case need to be set.
     */
    public static final class Builder {

        private int maxDirectBuffers = DEFAULT_MAX_DIRECT_BUFFERS;
        private int maxHeapBuffers = DEFAULT_MAX_HEAP_BUFFERS;
        private int blockSize = DEFAULT_BLOCK_SIZE;
        private long maxDirectBytes = DEFAULT_MAX_DIRECT_BYTES;
        private long maxHeapBytes = DEFAULT_MAX_HEAP_BYTES;
        private ByteBufferAllocator allocator = BuiltinByteBufferAllocator.INSTANCE;
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

        /** Sets the allocation backend supplying and releasing pooled buffers. */
        public Builder allocator(ByteBufferAllocator allocator) {
            this.allocator = allocator;
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

        /** Builds the configured pool. */
        public ByteBufferPool build() {
            return new ByteBufferPool(
                    maxDirectBuffers,
                    maxHeapBuffers,
                    blockSize,
                    allocator,
                    maxDirectBytes,
                    maxHeapBytes,
                    idleTimeout,
                    leakDetection);
        }
    }

    /** Immutable statistics snapshot for a ByteBuffer pool. */
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
     * A wrapper around a pooled {@link ByteBuffer} that implements {@link AutoCloseable} to enable try-with-resources
     * syntax for automatic buffer return to the pool.
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

        /** Fires {@link LeakGuard#run()} if this handle is collected without {@link #close()}; null when disabled. */
        private final LeakGuard leakGuard;

        private final Cleaner.Cleanable leakCleanable;

        PooledByteBufferImpl(ByteBuffer pooled, ByteBuffer slice, ByteBufferPool pool) {
            this.pooled = pooled;
            this.slice = slice;
            this.pool = pool;
            if (pool.leakDetectionEnabled) {
                this.leakGuard = new LeakGuard(pool);
                this.leakCleanable = LeakCleaner.CLEANER.register(this, leakGuard);
            } else {
                this.leakGuard = null;
                this.leakCleanable = null;
            }
        }

        @Override
        public void close() {
            if (pooled == null) {
                return;
            }
            if (leakGuard != null) {
                // Mark this borrow as properly closed, then run the cleanable so it deregisters and never fires.
                leakGuard.release();
                leakCleanable.clean();
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
        private final long maxBytes;
        private final IntFunction<ByteBuffer> factory;
        private final Consumer<ByteBuffer> evictionListener;

        /**
         * @param queueCapacity the maximum number of buffers to hold
         * @param minBufferCapacity minimum buffer capacity to keep in the pool
         * @param maxBufferCapacity the maximum size of a single byte buffer to be held
         * @param maxBytes the maximum total bytes to retain across all held buffers
         * @param factory function to create new buffers
         * @param evictionListener listener called when a buffer is discarded/evicted
         */
        BufferQueue(
                int queueCapacity,
                int minBufferCapacity,
                int maxBufferCapacity,
                long maxBytes,
                IntFunction<ByteBuffer> factory,
                Consumer<ByteBuffer> evictionListener) {
            this.queueCapacity = queueCapacity;
            this.minBufferCapacity = minBufferCapacity;
            this.maxBufferCapacity = maxBufferCapacity;
            this.maxBytes = maxBytes;
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
         * Releases every retained buffer, freeing its memory and counting it as discarded, but keeping the cumulative
         * lifetime counters. Used by idle eviction, which empties a quiet pool without resetting its statistics.
         */
        public void evictAll() {
            lock.lock();
            try {
                for (ByteBuffer buffer : buffers) {
                    buffersDiscarded.incrementAndGet();
                    discardedSize.addAndGet(buffer.capacity());
                    evictionListener.accept(buffer);
                }
                buffers.clear();
                bytesSize.set(0);
            } finally {
                lock.unlock();
            }
        }

        /**
         * Returns the smallest {@link ByteBuffer} whose capacity is greater than or equal to the given key. Uses the
         * provided factory function to create one if there is no such key.
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
         * @param returningBuffer
         * @return true if the buffer was returned, false if it was discarded
         */
        public boolean returnBuffer(ByteBuffer returningBuffer) {
            final int capacity = returningBuffer.capacity();
            // A buffer below the pooling threshold, above the per-buffer cap, or larger than the whole
            // byte budget can never be retained, so discard it without taking the lock.
            if (capacity < minBufferCapacity || capacity > maxBufferCapacity || capacity > maxBytes) {
                return discard(returningBuffer);
            }

            lock.lock();
            try {
                returningBuffer.clear();
                evictSmallestToMakeRoom(capacity);
                if (!fitsWithin(capacity)) {
                    // The free list is full of buffers no smaller than this one (by count or bytes); keep them.
                    return discard(returningBuffer);
                }

                int insertionPoint = binarySearchLowerBound(capacity);
                buffers.add(insertionPoint, returningBuffer);
                bytesSize.addAndGet(capacity);
                buffersReturned.incrementAndGet();
                return true;
            } finally {
                lock.unlock();
            }
        }

        /**
         * True when a buffer of the given capacity fits within both the count and byte budgets. Caller holds the lock.
         */
        private boolean fitsWithin(int capacity) {
            return buffers.size() < queueCapacity && bytesSize.get() + capacity <= maxBytes;
        }

        /**
         * Evicts the smallest retained buffers, one at a time, while the incoming buffer does not fit and is larger
         * than the smallest retained one. Stops without evicting when the incoming buffer is no larger than the
         * smallest, so the pool keeps its larger buffers. Caller holds the lock.
         */
        private void evictSmallestToMakeRoom(int capacity) {
            while (!fitsWithin(capacity) && !buffers.isEmpty()) {
                ByteBuffer smallest = buffers.get(0);
                if (capacity <= smallest.capacity()) {
                    return;
                }
                buffers.remove(0);
                bytesSize.addAndGet(-smallest.capacity());
                recordDiscard(smallest);
            }
        }

        /** Discards a buffer (counts it and hands it to the eviction listener) and reports it as not retained. */
        private boolean discard(ByteBuffer buffer) {
            recordDiscard(buffer);
            return false;
        }

        private void recordDiscard(ByteBuffer buffer) {
            buffersDiscarded.incrementAndGet();
            discardedSize.addAndGet(buffer.capacity());
            evictionListener.accept(buffer);
        }

        /**
         * Finds the index of the first {@link ByteBuffer} in the sorted list whose capacity is greater than or equal to
         * the {@code minimumSize}. This is also known as a "lower bound" search.
         *
         * @param minimumSize the minimum capacity to search for.
         * @return The index of the first buffer with capacity >= {@code minimumSize}. If no such buffer is found (i.e.,
         *     all buffers have smaller capacity), it returns {@code buffers.size()}, which is a valid insertion point.
         *     This method guarantees a non-negative return value.
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

    /**
     * Holds the single daemon scheduler shared by all pools for idle eviction. Lazily created on first use; its thread
     * is a daemon and uses the system class loader so it never keeps the JVM alive or pins an application class loader.
     */
    private static final class IdleEvictionScheduler {

        static final ScheduledExecutorService EXECUTOR = create();

        private IdleEvictionScheduler() {
            // no-op
        }

        private static ScheduledExecutorService create() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, IdleEvictionScheduler::newThread);
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        }

        private static Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "tileverse-bytebufferpool-idle-eviction");
            thread.setDaemon(true);
            thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
            return thread;
        }
    }

    /**
     * The cleaning action registered for a borrowed buffer. It references only the pool, never the
     * {@link PooledByteBufferImpl} it guards, so the handle stays collectible. {@link #run()} runs either when the
     * handle is closed (after {@link #release()}, so it does nothing) or when the handle is garbage-collected without
     * being closed (then it reports a leak).
     */
    private static final class LeakGuard implements Runnable {

        private final ByteBufferPool pool;
        private volatile boolean released;

        LeakGuard(ByteBufferPool pool) {
            this.pool = pool;
        }

        void release() {
            released = true;
        }

        @Override
        public void run() {
            if (!released) {
                pool.recordLeak();
            }
        }
    }

    /** Holds the single {@link Cleaner} shared by all pools for leak detection; lazily created on first use. */
    private static final class LeakCleaner {

        static final Cleaner CLEANER = Cleaner.create();

        private LeakCleaner() {
            // no-op
        }
    }
}
