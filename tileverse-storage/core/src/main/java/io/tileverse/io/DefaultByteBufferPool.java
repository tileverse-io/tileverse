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
 * Built-in {@link ByteBufferPool}: the implementation used unless a provider is registered. Manages direct and heap
 * buffers in separate free lists, bounding retention by both buffer count and bytes, releasing the free list after an
 * idle period, and detecting borrows discarded without being closed.
 *
 * <p>Borrowing never blocks: a borrow always returns a buffer, creating one beyond the retention budget when nothing
 * suitable is held. The byte budget bounds only what the free list keeps; a returned buffer that would exceed it is
 * freed instead of pooled.
 *
 * <p>Direct buffers are allocated with {@link ByteBuffer#allocateDirect(int)} and released eagerly through
 * {@link DirectByteBufferCleaner}; heap buffers are allocated with {@link ByteBuffer#allocate(int)} and left to the
 * garbage collector.
 */
@Slf4j
final class DefaultByteBufferPool implements ByteBufferPool {

    /** Minimum spacing between leak warnings, so a recurring leak does not flood the log. */
    private static final long LEAK_WARN_INTERVAL_NANOS = Duration.ofSeconds(60).toNanos();

    /** Shared default pool: the registered provider if any, otherwise a built-in pool with default settings. */
    static final ByteBufferPool DEFAULT = discoverPool();

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

    /**
     * Builds the pool. The byte budgets bound the bytes retained in the free list; they never block or refuse a borrow,
     * only what the pool keeps after a buffer is returned. The idle timeout controls when a quiet pool releases its
     * whole free list. Leak detection watches for borrows discarded without being closed.
     */
    DefaultByteBufferPool(
            int maxDirectBuffers,
            int maxHeapBuffers,
            int blockSize,
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
                ByteBuffer::allocateDirect,
                DirectByteBufferCleaner::releaseDirectBuffer);

        this.heapBuffers = new BufferQueue(
                maxHeapBuffers, blockSize, Integer.MAX_VALUE, maxHeapBytes, ByteBuffer::allocate, buffer -> {});

        log.debug(
                "Created ByteBufferPool: maxDirect={} ({} bytes), maxHeap={} ({} bytes), blockSize={}",
                maxDirectBuffers,
                maxDirectBytes,
                maxHeapBuffers,
                maxHeapBytes,
                blockSize);
    }

    /**
     * Discovers a {@link ByteBufferPool} provider through {@link ServiceLoader}, falling back to a built-in pool when
     * none is registered. A broken provider does not break callers: discovery failures fall back to the built-in pool.
     */
    private static ByteBufferPool discoverPool() {
        try {
            List<ByteBufferPool> providers = new ArrayList<>();
            for (ByteBufferPool provider : ServiceLoader.load(ByteBufferPool.class)) {
                providers.add(provider);
            }
            if (!providers.isEmpty()) {
                ByteBufferPool selected = providers.get(0);
                if (providers.size() > 1) {
                    log.warn(
                            "Found {} ByteBufferPool providers; using {}. Register at most one.",
                            providers.size(),
                            selected.getClass().getName());
                } else {
                    log.info(
                            "Using ByteBufferPool provider {}",
                            selected.getClass().getName());
                }
                return selected;
            }
        } catch (RuntimeException | ServiceConfigurationError error) {
            log.warn("Failed to load a ByteBufferPool provider; using the built-in pool.", error);
        }
        return ByteBufferPool.builder().build();
    }

    @Override
    public PooledByteBuffer borrowDirect(int size) {
        return borrow(directBuffers, size);
    }

    @Override
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
     * Rounds up the given capacity to the next multiple of the block size, for better memory alignment.
     *
     * @param capacity the capacity to round up
     * @return the capacity rounded up to the next multiple of block size
     */
    private int roundUpToBlockSize(int capacity) {
        return ((capacity + blockSize - 1) / blockSize) * blockSize;
    }

    /**
     * Returns a ByteBuffer to the pool for potential reuse. The buffer is cleared and added to the appropriate free
     * list when it fits the retention budget, otherwise discarded. After calling this, the caller must not use the
     * buffer.
     *
     * @param buffer the buffer to return (may be null, in which case this is a no-op)
     */
    void returnBuffer(ByteBuffer buffer) {
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

    @Override
    public long getLeakCount() {
        return leakCount.get();
    }

    @Override
    public void clear() {
        try {
            directBuffers.clear();
        } finally {
            heapBuffers.clear();
        }
        log.debug("Cleared pool");
    }

    @Override
    public PoolStatistics getStatistics() {
        return getHeapPoolStatistics().add(getDirectPoolStatistics());
    }

    @Override
    public PoolStatistics getHeapPoolStatistics() {
        return heapBuffers.stats();
    }

    @Override
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

    // visible for testing
    static class PooledByteBufferImpl implements PooledByteBuffer {

        private ByteBuffer pooled;
        private ByteBuffer slice;
        private DefaultByteBufferPool pool;

        /** Fires {@link LeakGuard#run()} if this handle is collected without {@link #close()}; null when disabled. */
        private final LeakGuard leakGuard;

        private final Cleaner.Cleanable leakCleanable;

        PooledByteBufferImpl(ByteBuffer pooled, ByteBuffer slice, DefaultByteBufferPool pool) {
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

        private final DefaultByteBufferPool pool;
        private volatile boolean released;

        LeakGuard(DefaultByteBufferPool pool) {
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
