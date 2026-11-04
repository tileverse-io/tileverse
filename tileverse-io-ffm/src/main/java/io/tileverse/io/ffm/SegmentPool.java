/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import io.tileverse.io.ffm.limits.IoLimits;
import io.tileverse.io.ffm.limits.ResourceLimits;

/**
 * A pool of native {@link MemorySegment}s for fetch and decompression buffers. Borrowing reuses
 * native memory to keep the streaming memory budget bounded; closing the handle returns the backing segment.
 *
 * <p>The interface is sealed with {@link DefaultSegmentPool} as its only implementation. A reader may derive element
 * counts from segment sizes, which makes the exact-size promise of {@link #borrow} a correctness invariant, not a
 * convention; sealing puts its enforcement in one reviewable place instead of trusting every implementor.
 * {@link #getDefault()} is the process-wide shared pool, letting a single physical pool serve several readers on the
 * same instance. {@link #create()} builds a private pool with its own retention state and {@link #stats() accounting},
 * for tests and for callers that need isolation from the shared default.
 *
 * <p>Pools support concurrent {@link #borrow} calls from multiple threads. A returned {@link Pooled} handle, by
 * contrast, is owned by a single borrower and is not safe to share across threads.
 */
public sealed interface SegmentPool permits DefaultSegmentPool {

    /** Borrows a native segment of exactly {@code byteSize} bytes; return it by closing the handle. */
    Pooled borrow(long byteSize);

    /** A point-in-time snapshot of this pool's accounting, for leak assertions and monitoring. */
    PoolStats stats();

    /**
     * Frees the native memory of every backing currently retained for reuse, releasing the pool's idle footprint
     * deterministically. Call this when a private pool is done; outstanding borrows are unaffected and must be closed
     * by their owners. The process-wide {@link #getDefault() default} lives for the JVM lifetime and is never closed.
     *
     * <p>Intentionally not {@link AutoCloseable}: most pools are short-lived, a pool dropped without closing frees its
     * retained backings once it is garbage collected, and requiring try-with-resources on every borrow site would
     * obscure the borrow/return contract that actually governs memory. Closing is idempotent.
     */
    void close();

    /** Process-wide shared default: a JDK-only pool with the {@link Options#elastic() elastic} retention policy. */
    static SegmentPool getDefault() {
        return DefaultSegmentPool.DefaultHolder.INSTANCE;
    }

    /** A new private pool with the {@link Options#elastic() elastic} options, independent of {@link #getDefault()}. */
    static SegmentPool create() {
        return create(Options.elastic());
    }

    /** A new private pool with the given retention policy, independent of {@link #getDefault()}. */
    static SegmentPool create(Options options) {
        return DefaultSegmentPool.fromOptions(options);
    }

    /**
     * The pool's retention policy. Two size classes reuse returned buffers under separate idle-retention caps, with
     * {@code largeBufferThreshold} as the dividing line: a buffer of rounded capacity at most the threshold reuses up
     * to {@code maxPooledBytes} of idle retention, a larger one up to {@code maxLargePooledBytes}. A return that would
     * breach its class's cap is freed at once rather than retained. Capacities round up to {@code blockSize} to make
     * reuse across nearby request sizes likely. A zero cap disables retention for that class entirely (every return in
     * it frees).
     *
     * <p>{@code maxTotalBytes} limits retention by the pool's whole footprint, the borrowed and the retained bytes
     * together. A borrow is never refused. A return is retained only while that total stays within the ceiling and is
     * freed above it, hence the footprint never exceeds the larger of the ceiling and the bytes in flight. The total
     * never grows on a return: a pool working inside its ceiling frees nothing, and the ceiling only trims retention
     * after borrows beyond it. {@link Long#MAX_VALUE} leaves the class caps as the only bound.
     *
     * <p>A positive {@code idleRetentionNanos} enables background idle decay: a retained backing left untouched for
     * that long is freed by a per-pool background reaper thread, returning its native memory to the OS during quiet
     * periods. Zero disables decay, keeping retained backings until they are reused, evicted by an over-cap return, or
     * the pool closes.
     */
    record Options(
            long largeBufferThreshold,
            long maxPooledBytes,
            long maxLargePooledBytes,
            long maxTotalBytes,
            long idleRetentionNanos,
            int blockSize) {

        /** Options with no ceiling on the pool's total: the class caps are the only bound on retention. */
        public Options(
                long largeBufferThreshold,
                long maxPooledBytes,
                long maxLargePooledBytes,
                long idleRetentionNanos,
                int blockSize) {
            this(
                    largeBufferThreshold,
                    maxPooledBytes,
                    maxLargePooledBytes,
                    Long.MAX_VALUE,
                    idleRetentionNanos,
                    blockSize);
        }

        public Options {
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
        }

        /**
         * The pod-sized policy: a 4 MB threshold (most decode buffers and fetch buffers stay poolable),
         * a small-buffer idle cap of half the off-heap allowance clamped to [16 MB, 512 MB], a large-buffer idle cap of
         * the whole allowance clamped to [64 MB, 1 GB], a retention ceiling on the pool's total equal to what the fetch
         * and off-heap decode budgets allow in flight, and a 60-second idle-retention window after which the background
         * reaper returns idle buffers to the OS. Computed once; the limits probe reads the container memory.
         *
         * <p>The caps hold the working set of a loaded server: the fetch buffers in flight in the large class, and
         * their decode buffers in the small class. A cap below that working set makes the pool map
         * and unmap memory on nearly each borrow and return instead of reusing it. The ceiling keeps retention from
         * adding to the memory those budgets already account for.
         */
        public static Options elastic() {
            return ElasticHolder.OPTIONS;
        }

        private static final class ElasticHolder {
            private static final Options OPTIONS = elasticOptions();

            private static Options elasticOptions() {
                ResourceLimits limits = ResourceLimits.getDefault();
                long maxOffHeapBytes = IoLimits.offHeapCap(limits);
                long maxPooledBytes = Math.clamp(maxOffHeapBytes / 2, 16L << 20, 512L << 20);
                long maxLargePooledBytes = Math.clamp(maxOffHeapBytes, 64L << 20, 1024L << 20);
                long maxTotalBytes = maxOffHeapBytes + IoLimits.decodeCap(limits);
                long idleRetentionNanos = TimeUnit.SECONDS.toNanos(60);
                return new Options(
                        4L << 20, maxPooledBytes, maxLargePooledBytes, maxTotalBytes, idleRetentionNanos, 8192);
            }
        }
    }

    /**
     * A point-in-time snapshot of a pool's accounting.
     *
     * @param outstandingBorrows borrows not yet closed; zero proves every borrowed segment was returned
     * @param totalBorrows borrows ever made, never decremented; zero proves the pool was never drawn from
     * @param freeSegments backings currently retained for reuse
     * @param retainedBytes total capacity of the retained backings
     * @param borrowedBytes total capacity of the backings borrowed and not yet returned; with {@code retainedBytes} it
     *     is the pool's whole native footprint
     */
    record PoolStats(
            long outstandingBorrows, long totalBorrows, int freeSegments, long retainedBytes, long borrowedBytes) {}

    /**
     * A borrowed segment owned by a single borrower. The owner closes it exactly once when done; repeated
     * {@link #close()} calls from that owner are a no-op. A {@code Pooled} handle is not designed to be closed
     * concurrently from multiple threads.
     */
    interface Pooled extends AutoCloseable {

        /**
         * The borrowed segment, sized to the requested byte length. Valid only until {@link #close()}; reading or
         * writing it afterwards is undefined, as the underlying native memory may have been handed to another borrow.
         */
        MemorySegment segment();

        /** Returns the segment to the pool. Idempotent for the owning borrower. */
        @Override
        void close();
    }
}
