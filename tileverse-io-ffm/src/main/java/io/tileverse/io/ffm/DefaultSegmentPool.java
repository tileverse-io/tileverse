/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A bounded pool of native {@link MemorySegment}s for fetch and decode buffers, the JDK-only
 * {@link SegmentPool#getDefault() default}. {@link Arena#allocate} zeroes what it returns, and these buffers are
 * overwritten in full anyway. Reusing them pays the zeroing once instead of on each use.
 *
 * <ul>
 *   <li><b>Size classes.</b> Capacities up to {@code largeBufferThreshold} are small, and up to {@code maxPooledBytes}
 *       of them stay idle. Larger ones, such as the fetch buffer of a large read, have their own cap,
 *       {@code maxLargePooledBytes}. A rare outlier then cannot pin the memory of the common sizes. Capacities round up
 *       to {@code blockSize}.
 *   <li><b>Borrow and return.</b> A borrow takes the smallest idle backing that fits, or allocates one, and is never
 *       refused. A return is retained while its class is under its cap and the footprint within {@code maxTotalBytes};
 *       otherwise its memory is freed at once.
 *   <li><b>Stripes.</b> The small class is split over stripes, one per core, in a {@link StripedFreeLists}. The large
 *       class takes a few operations per query and keeps one {@link LockedFreeLists}. Nothing on the borrow and return
 *       path is written by all threads.
 *   <li><b>Footprint ceiling.</b> {@code maxTotalBytes} bounds the borrowed and retained bytes together. The footprint
 *       never exceeds the larger of this ceiling and the bytes in flight. The default pool sets it to the fetch and
 *       decode budgets. Retention then fills what they leave idle and adds nothing to them.
 *   <li><b>Arenas.</b> Each backing owns an explicitly closed arena and frees on its own. Such memory does not count
 *       against {@code -XX:MaxDirectMemorySize}, unlike an {@link Arena#ofAuto() auto} arena's. {@link MappedMemory}
 *       maps the backings of at least {@link MappedMemory#MIN_MAPPED_BYTES}, and their pages go back to the kernel when
 *       they are freed.
 *   <li><b>Idle decay.</b> With a positive {@code idleRetentionNanos}, a daemon timer frees backings idle past that
 *       window every {@link #SWEEP_INTERVAL_SECONDS} seconds. The timer holds the pool weakly, and a pool abandoned
 *       without {@link #close()} is still collected. {@link #close()} stops the timer and frees everything retained.
 *   <li><b>Abandoned pools.</b> Once a pool dropped without {@link #close()} is collected, a {@link Cleaner} frees the
 *       backings it retained and stops its timer.
 * </ul>
 */
final class DefaultSegmentPool implements SegmentPool {

    // One daemon thread for all pools, to free the backings retained by the pools dropped without close()
    private static final Cleaner CLEANER = Cleaner.create();

    /**
     * Holds the default pool, built on first use. Its options read the machine limits, which fail on a malformed
     * override: the pools built from explicit options must not depend on them.
     */
    static final class DefaultHolder {

        static final DefaultSegmentPool INSTANCE = fromOptions(SegmentPool.Options.elastic());

        private DefaultHolder() {}
    }

    /**
     * How often the idle sweep runs. This is the reaping granularity, not a policy knob: a backing idle past
     * {@link #idleRetentionNanos} is freed within one interval after the window elapses. Coarse to stay cheap when
     * idle.
     */
    private static final long SWEEP_INTERVAL_SECONDS = 15;

    static DefaultSegmentPool fromOptions(SegmentPool.Options options) {
        DefaultSegmentPool pool = new DefaultSegmentPool(
                options.largeBufferThreshold(),
                options.maxPooledBytes(),
                options.maxLargePooledBytes(),
                options.maxTotalBytes(),
                options.idleRetentionNanos(),
                options.blockSize(),
                StripedFreeLists.stripeCount(
                        Runtime.getRuntime().availableProcessors(),
                        options.maxPooledBytes(),
                        options.largeBufferThreshold()));
        pool.startIdleSweep();
        return pool;
    }

    /** Rounded capacities at or below this reuse in the small class; larger ones in the large class. */
    private final long largeBufferThreshold;

    /** Requested sizes round up to a multiple of this, mapping near-equal requests onto one reusable capacity. */
    private final int blockSize;

    /** How long a retained backing may sit idle before the sweep frees it; zero disables idle decay. */
    private final long idleRetentionNanos;

    /**
     * The retention limit on the pool's whole footprint, borrowed and retained bytes together; see {@link #giveBack}.
     */
    private final long maxTotalBytes;

    /** The small class, split over stripes. */
    private final StripedFreeLists smallBuffers;

    /** The large class under one lock and an exact cap; it takes a few operations per query. */
    private final LockedFreeLists largeBuffers;

    /**
     * The capacity of the backings alive, borrowed or idle: the pool's footprint. It moves only when a backing is
     * allocated or freed, never on a borrow or a return, hence the ceiling check on a return is one read of it.
     */
    private final AtomicLong footprint = new AtomicLong();

    /** Borrows handed out but not yet returned; reaching zero proves every borrowed segment was closed. */
    private final LongAdder outstandingBorrows = new LongAdder();

    /** Borrows ever made, never decremented; a monitoring counter, zero proves the pool was never drawn from. */
    private final LongAdder totalBorrows = new LongAdder();

    /**
     * The idle-sweep timer, created when idle decay is enabled and scheduled after construction; null when disabled.
     */
    private final ScheduledExecutorService idleSweep;

    /** A single-stripe pool, the retention policy in its exact form; the tests of that policy build this one. */
    DefaultSegmentPool(long largeBufferThreshold, long maxPooledBytes, long maxLargePooledBytes, int blockSize) {
        this(largeBufferThreshold, maxPooledBytes, maxLargePooledBytes, 0L, blockSize);
    }

    DefaultSegmentPool(
            long largeBufferThreshold,
            long maxPooledBytes,
            long maxLargePooledBytes,
            long idleRetentionNanos,
            int blockSize) {
        this(largeBufferThreshold, maxPooledBytes, maxLargePooledBytes, Long.MAX_VALUE, idleRetentionNanos, blockSize);
    }

    DefaultSegmentPool(
            long largeBufferThreshold,
            long maxPooledBytes,
            long maxLargePooledBytes,
            long maxTotalBytes,
            long idleRetentionNanos,
            int blockSize) {
        this(
                largeBufferThreshold,
                maxPooledBytes,
                maxLargePooledBytes,
                maxTotalBytes,
                idleRetentionNanos,
                blockSize,
                1);
    }

    DefaultSegmentPool(
            long largeBufferThreshold,
            long maxPooledBytes,
            long maxLargePooledBytes,
            long maxTotalBytes,
            long idleRetentionNanos,
            int blockSize,
            int stripes) {
        if (largeBufferThreshold <= 0) {
            throw new IllegalArgumentException("largeBufferThreshold must be > 0, got " + largeBufferThreshold);
        }
        if (maxPooledBytes < 0) {
            throw new IllegalArgumentException("maxPooledBytes must be >= 0, got " + maxPooledBytes);
        }
        if (maxLargePooledBytes < 0) {
            throw new IllegalArgumentException("maxLargePooledBytes must be >= 0, got " + maxLargePooledBytes);
        }
        if (maxTotalBytes < 0) {
            throw new IllegalArgumentException("maxTotalBytes must be >= 0, got " + maxTotalBytes);
        }
        if (idleRetentionNanos < 0) {
            throw new IllegalArgumentException("idleRetentionNanos must be >= 0, got " + idleRetentionNanos);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be > 0, got " + blockSize);
        }
        this.largeBufferThreshold = largeBufferThreshold;
        this.maxTotalBytes = maxTotalBytes;
        this.blockSize = blockSize;
        this.idleRetentionNanos = idleRetentionNanos;
        this.smallBuffers = new StripedFreeLists(maxPooledBytes, stripes);
        this.largeBuffers = new LockedFreeLists(maxLargePooledBytes);
        this.idleSweep =
                idleRetentionNanos > 0 ? Executors.newSingleThreadScheduledExecutor(reaperThreadFactory()) : null;
        CLEANER.register(this, new AbandonedPoolCleanup(smallBuffers, largeBuffers, idleSweep));
    }

    @Override
    public Pooled borrow(long byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize must be >= 0, got " + byteSize);
        }
        long capacity = roundUpToBlockSize(byteSize);
        Backing backing = capacity > largeBufferThreshold ? largeBuffers.take(capacity) : smallBuffers.take(capacity);
        if (backing == null) {
            backing = allocateFresh(capacity);
        }
        outstandingBorrows.increment();
        totalBorrows.increment();
        return new PooledSegment(this, backing, backing.view(byteSize));
    }

    @Override
    public PoolStats stats() {
        long retainedBytes = largeBuffers.retainedBytes() + smallBuffers.retainedBytes();
        int freeSegments = largeBuffers.count() + smallBuffers.count();
        long borrowedBytes = footprint.get() - retainedBytes;
        return new PoolStats(outstandingBorrows.sum(), totalBorrows.sum(), freeSegments, retainedBytes, borrowedBytes);
    }

    @Override
    public void close() {
        stopIdleSweep();
        List<Backing> drained = new ArrayList<>(largeBuffers.drain());
        drained.addAll(smallBuffers.drain());
        freeAll(drained);
    }

    /** The number of stripes the small class is split over. */
    int stripes() {
        return smallBuffers.stripes();
    }

    /**
     * Stops the idle timer and waits for an in-flight sweep to finish, making {@code close()} a quiescence point: once
     * it returns, no sweep is still closing arenas on the reaper thread. Restores the interrupt and proceeds if the
     * wait is interrupted; the drain below still frees everything retained.
     */
    private void stopIdleSweep() {
        ScheduledExecutorService timer = idleSweep;
        if (timer == null) {
            return;
        }
        timer.shutdownNow();
        try {
            timer.awaitTermination(SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called by a handle's first {@code close()}; the handles guarantee at most one call per borrow. */
    void borrowReturned() {
        outstandingBorrows.decrement();
    }

    /**
     * Retains {@code backing} when the ceiling and its class's cap allow, and frees it otherwise. A return moves bytes
     * from borrowed to retained and leaves the footprint as it was: it is retained while the footprint is within the
     * ceiling. Only borrows beyond the ceiling make later returns free their memory, until the footprint is back under
     * it.
     */
    void giveBack(Backing backing) {
        if (footprint.get() > maxTotalBytes) {
            free(backing);
            return;
        }
        boolean retained =
                backing.byteSize() > largeBufferThreshold ? largeBuffers.put(backing) : smallBuffers.put(backing);
        if (!retained) {
            free(backing);
        }
    }

    /**
     * Frees the retained backings idle for at least {@code thresholdNanos} as of {@code nowNanos}, taking each lock
     * normally. The package-private entry point the tests drive directly; the live pool reaps through {@link #sweep()}.
     */
    void reapIdle(long nowNanos, long thresholdNanos) {
        if (thresholdNanos <= 0) {
            return;
        }
        List<Backing> stale = new ArrayList<>(largeBuffers.reapStale(nowNanos, thresholdNanos));
        stale.addAll(smallBuffers.reapStale(nowNanos, thresholdNanos));
        freeAll(stale);
    }

    private void startIdleSweep() {
        if (idleSweep != null) {
            idleSweep.scheduleWithFixedDelay(
                    new IdleSweep(this, idleSweep), SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static ThreadFactory reaperThreadFactory() {
        return Thread.ofPlatform()
                .daemon()
                .name("tileverse-segment-pool-reaper-", 0)
                .factory();
    }

    /**
     * Reaps the idle backings behind the locks it can take without waiting. Skipping a busy stripe is harmless: the
     * next sweep one interval later reaps it, and a stripe busy enough to hold its lock at the tick is churning its
     * buffers rather than letting them sit idle.
     */
    private void sweep() {
        long nowNanos = System.nanoTime();
        List<Backing> stale = new ArrayList<>(largeBuffers.tryReapStale(nowNanos, idleRetentionNanos));
        stale.addAll(smallBuffers.tryReapStale(nowNanos, idleRetentionNanos));
        freeAll(stale);
    }

    /** Allocates a fresh backing, counted in the footprint before the allocation and uncounted when it fails. */
    private Backing allocateFresh(long capacity) {
        footprint.addAndGet(capacity);
        boolean allocated = false;
        try {
            Arena arena = Arena.ofShared();
            MemorySegment segment = MappedMemory.allocate(capacity, arena);
            allocated = true;
            return new Backing(arena, segment);
        } finally {
            if (!allocated) {
                footprint.addAndGet(-capacity);
            }
        }
    }

    private void freeAll(List<Backing> backings) {
        for (Backing backing : backings) {
            free(backing);
        }
    }

    private void free(Backing backing) {
        footprint.addAndGet(-backing.byteSize());
        backing.close();
    }

    private long roundUpToBlockSize(long byteSize) {
        if (byteSize == 0) {
            return blockSize;
        }
        return ((byteSize + blockSize - 1) / blockSize) * blockSize;
    }

    /**
     * The periodic idle sweep, scheduled on the pool's timer. It holds only a {@link WeakReference} to the pool so the
     * pool stays collectible while the timer thread is alive; once the pool has been collected the sweep shuts its own
     * timer down, releasing the thread and executor. Each run resolves the reference to a strong local only for the
     * duration of one sweep.
     */
    private static final class IdleSweep implements Runnable {

        private final WeakReference<DefaultSegmentPool> poolRef;
        private final ScheduledExecutorService timer;

        IdleSweep(DefaultSegmentPool pool, ScheduledExecutorService timer) {
            this.poolRef = new WeakReference<>(pool);
            this.timer = timer;
        }

        @Override
        public void run() {
            DefaultSegmentPool pool = poolRef.get();
            if (pool == null) {
                timer.shutdown();
                return;
            }
            try {
                pool.sweep();
            } catch (RuntimeException _) {
                // A failed sweep must not stop the timer from trying again next interval.
            }
        }
    }

    /**
     * Frees the backings retained by a pool once the pool is unreachable, and stops its timer. It references the free
     * lists and the timer, never the pool. No borrow or return can run at that point, because a borrowed handle and a
     * running sweep reference the pool and keep it reachable. A backing drained by {@link #close()} has left the free
     * lists, and is never freed twice.
     */
    private record AbandonedPoolCleanup(
            StripedFreeLists smallBuffers, LockedFreeLists largeBuffers, ScheduledExecutorService idleSweep)
            implements Runnable {

        @Override
        public void run() {
            if (idleSweep != null) {
                idleSweep.shutdownNow();
            }
            closeAll(largeBuffers.drain());
            closeAll(smallBuffers.drain());
        }

        private static void closeAll(List<Backing> backings) {
            for (Backing backing : backings) {
                backing.close();
            }
        }
    }

    /** A pooled backing and the explicit arena that owns it. */
    record Backing(Arena arena, MemorySegment segment) {

        long byteSize() {
            return segment.byteSize();
        }

        /** A view of the first {@code byteSize} bytes, handed to a borrower. */
        MemorySegment view(long byteSize) {
            return segment.asSlice(0, byteSize);
        }

        /** Frees the backing's native memory by closing its arena. */
        void close() {
            arena.close();
        }
    }
}
