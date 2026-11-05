/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.io.ffm.limits.IoLimits;
import io.tileverse.io.ffm.limits.ResourceLimits;

class SegmentPoolTest {

    @Test
    void getDefaultIsASingleton() {
        assertThat(SegmentPool.getDefault()).isSameAs(SegmentPool.getDefault());
    }

    @Test
    void borrowReturnsSegmentOfExactSize() {
        try (SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(1000)) {
            assertThat(pooled.segment().byteSize()).isEqualTo(1000);
        }
    }

    @Test
    void borrowZeroReturnsEmptySegment() {
        try (SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(0)) {
            assertThat(pooled.segment().byteSize()).isZero();
        }
    }

    @Test
    void negativeBorrowThrows() {
        SegmentPool pool = SegmentPool.getDefault();
        assertThatThrownBy(() -> pool.borrow(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void borrowedSegmentReadsBack() {
        try (SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(8)) {
            MemorySegment segment = pooled.segment();
            segment.set(ValueLayout.JAVA_LONG, 0, 0x0102030405060708L);
            assertThat(segment.get(ValueLayout.JAVA_LONG, 0)).isEqualTo(0x0102030405060708L);
        }
    }

    @Test
    void outstandingBorrowsAreDistinct() {
        SegmentPool pool = SegmentPool.getDefault();
        try (SegmentPool.Pooled a = pool.borrow(64);
                SegmentPool.Pooled b = pool.borrow(64)) {
            assertThat(a.segment().address()).isNotEqualTo(b.segment().address());
        }
    }

    @Test
    void closeIsIdempotent() {
        SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(16);
        pooled.close();
        assertThatCode(pooled::close).doesNotThrowAnyException();
    }

    // S7466: the project style declares explicit local types rather than var
    @Test
    @SuppressWarnings("java:S7466")
    void createReturnsIndependentPools() {
        SegmentPool a = SegmentPool.create();
        SegmentPool b = SegmentPool.create();
        assertThat(a).isNotSameAs(b);
        try (SegmentPool.Pooled _ = a.borrow(64)) {
            assertThat(a.stats().outstandingBorrows()).isEqualTo(1);
            assertThat(b.stats().outstandingBorrows()).isZero();
        }
    }

    @Test
    void optionsRejectInvalidSizes() {
        assertThatThrownBy(() -> new SegmentPool.Options(0, 1024, 1024, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, -1, 1024, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, -1, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, 1024, -1, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, 1024, -1, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, 1024, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void elasticCapsStayWithinBounds() {
        SegmentPool.Options elastic = SegmentPool.Options.elastic();
        assertThat(elastic.largeBufferThreshold()).isEqualTo(4L << 20);
        assertThat(elastic.maxPooledBytes()).isBetween(16L << 20, 512L << 20);
        assertThat(elastic.maxLargePooledBytes()).isBetween(64L << 20, 1024L << 20);
        assertThat(elastic.idleRetentionNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(60));
        assertThat(elastic.blockSize()).isEqualTo(8192);
        IoLimits limits = IoLimits.from(ResourceLimits.getDefault());
        assertThat(elastic.maxTotalBytes())
                .as("the ceiling is what the fetch and off-heap decode budgets allow in flight")
                .isEqualTo(limits.maxOffHeapBytes() + limits.maxDecodeBytes());
    }

    @Test
    void megabyteBuffersArePooled() {
        SegmentPool pool = SegmentPool.create();
        SegmentPool.Pooled first = pool.borrow(1L << 20);
        long address = first.segment().address();
        first.close();
        assertThat(pool.stats().freeSegments()).isEqualTo(1);
        try (SegmentPool.Pooled second = pool.borrow(1L << 20)) {
            assertThat(second.segment().address()).as("reused backing").isEqualTo(address);
        }
    }

    @Test
    void createHonorsCustomOptions() {
        SegmentPool pool = SegmentPool.create(new SegmentPool.Options(512 * 1024, 1L << 20, 1L << 20, 0, 1024));
        pool.borrow(600 * 1024).close();
        assertThat(pool.stats().freeSegments())
                .as("above the threshold: retained in the large class")
                .isEqualTo(1);
        pool.borrow(400 * 1024).close();
        assertThat(pool.stats().freeSegments())
                .as("below the threshold: retained in the small class")
                .isEqualTo(2);
    }

    @Test
    void statsTrackTheBorrowLifecycle() {
        SegmentPool pool = SegmentPool.create();
        assertThat(pool.stats()).isEqualTo(new SegmentPool.PoolStats(0, 0, 0, 0, 0));

        SegmentPool.Pooled pooled = pool.borrow(64);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(1);
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);
        assertThat(pool.stats().borrowedBytes())
                .as("the rounded capacity of the borrow")
                .isEqualTo(8192);

        pooled.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);
        assertThat(pool.stats().borrowedBytes()).isZero();

        pooled.close();
        assertThat(pool.stats().outstandingBorrows())
                .as("an idempotent re-close must not decrement twice")
                .isZero();
    }

    @Test
    void statsCountLargeBorrows() {
        DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1 << 20, 1024);
        SegmentPool.Pooled large = pool.borrow(8192);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(1);
        large.close();
        large.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);
        assertThat(pool.stats().freeSegments())
                .as("the large buffer is retained for reuse")
                .isEqualTo(1);
    }

    @Test
    void concurrentBorrowsStayConsistent() throws Exception {
        long maxPooledBytes = 8L * 8192;
        DefaultSegmentPool pool = new DefaultSegmentPool(1 << 20, maxPooledBytes, 1 << 20, 8192);
        AtomicBoolean failed = new AtomicBoolean(false);
        try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
            for (int t = 0; t < 8; t++) {
                executor.execute(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try (SegmentPool.Pooled pooled = pool.borrow(256)) {
                            MemorySegment segment = pooled.segment();
                            segment.set(ValueLayout.JAVA_INT, 0, i);
                            if (segment.get(ValueLayout.JAVA_INT, 0) != i) {
                                failed.set(true);
                            }
                        } catch (RuntimeException _) {
                            failed.set(true);
                        }
                    }
                });
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(failed).isFalse();
        assertThat(pool.stats().retainedBytes()).isLessThanOrEqualTo(maxPooledBytes);
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(8L * 2000);
    }

    /**
     * Retention policy: each size class reuses returned buffers under its own byte cap; a return that would breach a
     * class's cap has its arena closed at once, freeing the native memory deterministically rather than parking it.
     */
    @Nested
    class Retention {

        @Test
        void smallBuffersArePooledAndReused() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled first = pool.borrow(512);
            long address = first.segment().address();
            first.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            try (SegmentPool.Pooled second = pool.borrow(512)) {
                assertThat(second.segment().address()).as("reused backing").isEqualTo(address);
                assertThat(pool.stats().freeSegments()).isZero();
            }
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
        }

        @Test
        void largeBuffersArePooledAndReused() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled first = pool.borrow(8192);
            long address = first.segment().address();
            first.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            try (SegmentPool.Pooled second = pool.borrow(8192)) {
                assertThat(second.segment().address())
                        .as("reused large backing")
                        .isEqualTo(address);
                assertThat(pool.stats().freeSegments()).isZero();
            }
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
        }

        @Test
        void pooledRetentionIsBoundedByBytes() {
            long maxPooledBytes = 4096;
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, maxPooledBytes, 1 << 20, 1024);
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                held.add(pool.borrow(512));
            }
            for (SegmentPool.Pooled pooled : held) {
                pooled.close();
            }
            assertThat(pool.stats().retainedBytes()).as("retained bytes").isLessThanOrEqualTo(maxPooledBytes);
            assertThat(pool.stats().freeSegments())
                    .as("1024-byte backings retained under a 4096 cap")
                    .isEqualTo(4);
        }

        @Test
        void largeCapBoundsRetention() {
            long maxLargePooledBytes = 4L * 8192;
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, maxLargePooledBytes, 8192);
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                held.add(pool.borrow(8192));
            }
            for (SegmentPool.Pooled pooled : held) {
                pooled.close();
            }
            assertThat(pool.stats().retainedBytes())
                    .as("large retained bytes")
                    .isLessThanOrEqualTo(maxLargePooledBytes);
            assertThat(pool.stats().freeSegments())
                    .as("8192-byte large backings retained under a 32768 cap")
                    .isEqualTo(4);
        }

        @Test
        void classesRetainIndependently() {
            DefaultSegmentPool pool = new DefaultSegmentPool(4096, 1 << 20, 1 << 20, 1024);
            pool.borrow(2048).close();
            pool.borrow(16384).close();
            assertThat(pool.stats().freeSegments())
                    .as("one small + one large backing, each in its own class")
                    .isEqualTo(2);
        }

        @Test
        void evictedBackingIsFreed() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1024, 1024, 1024);
            SegmentPool.Pooled retained = pool.borrow(512);
            SegmentPool.Pooled evicted = pool.borrow(512);
            MemorySegment evictedSegment = evicted.segment();
            retained.close();
            evicted.close();
            assertThat(pool.stats().freeSegments())
                    .as("one 1024-byte backing fills the cap; the second return is evicted")
                    .isEqualTo(1);
            assertThatThrownBy(() -> evictedSegment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("an evicted pooled backing frees its arena instead of waiting for GC")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void closeFreesRetainedBackings() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(512);
            MemorySegment segment = pooled.segment();
            pooled.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            pool.close();
            assertThat(pool.stats().freeSegments()).isZero();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("pool close frees the retained backings")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void fullClassFreesReturnedBuffer() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 0, 1024);
            SegmentPool.Pooled pooled = pool.borrow(8192);
            MemorySegment segment = pooled.segment();
            pooled.close();
            assertThat(pool.stats().freeSegments()).isZero();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("a large class with no retention room frees the buffer on return")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void totalCeilingFreesTheOverflow() {
            long megabyte = 1L << 20;
            SegmentPool pool = SegmentPool.create(
                    new SegmentPool.Options(8192, 16 * megabyte, 16 * megabyte, 2 * megabyte, 0, 8192));
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                held.add(pool.borrow(megabyte));
            }
            for (SegmentPool.Pooled pooled : held) {
                pooled.close();
            }
            assertThat(pool.stats().retainedBytes())
                    .as("the first return exceeds the ceiling and is freed, the next two fit")
                    .isEqualTo(2 * megabyte);
            assertThat(pool.stats().freeSegments()).isEqualTo(2);
        }

        @Test
        void retainedCloseIsIdempotent() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(8192);
            pooled.close();
            assertThatCode(pooled::close).doesNotThrowAnyException();
        }

        // The most recently returned backing is reused first; the oldest stay at the tail for the sweep
        @Test
        void reusesNewestBackingFirst() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled older = pool.borrow(512);
            SegmentPool.Pooled newer = pool.borrow(512);
            long newest = newer.segment().address();
            older.close();
            newer.close();
            try (SegmentPool.Pooled next = pool.borrow(512)) {
                assertThat(next.segment().address()).isEqualTo(newest);
            }
        }

        @Test
        void takesTheSmallestFit() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1 << 20, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled small = pool.borrow(16 * 1024);
            SegmentPool.Pooled large = pool.borrow(64 * 1024);
            long smallest = small.segment().address();
            large.close();
            small.close();
            try (SegmentPool.Pooled fit = pool.borrow(10 * 1024)) {
                assertThat(fit.segment().address()).isEqualTo(smallest);
            }
        }

        @Test
        void reapsOnlyTheStaleBackings() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled older = pool.borrow(512);
            SegmentPool.Pooled newer = pool.borrow(512);
            MemorySegment stale = older.segment();
            MemorySegment kept = newer.segment();
            older.close();
            long cutoff = System.nanoTime();
            while (System.nanoTime() <= cutoff) {
                Thread.onSpinWait();
            }
            newer.close();

            long window = TimeUnit.SECONDS.toNanos(60);
            pool.reapIdle(cutoff + window, window);
            assertThat(pool.stats().freeSegments())
                    .as("the newer backing stays")
                    .isEqualTo(1);
            assertThatThrownBy(() -> stale.get(ValueLayout.JAVA_BYTE, 0)).isInstanceOf(IllegalStateException.class);
            assertThat(kept.get(ValueLayout.JAVA_BYTE, 0)).isZero();
        }

        @Test
        void borrowsAfterAGroupIsReaped() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            pool.borrow(512).close();
            long window = TimeUnit.SECONDS.toNanos(60);
            pool.reapIdle(System.nanoTime() + window, window);
            assertThat(pool.stats().freeSegments()).isZero();
            try (SegmentPool.Pooled next = pool.borrow(512)) {
                assertThat(next.segment().byteSize()).isEqualTo(512);
            }
        }

        // The sweep reaps while borrows and returns run: nothing is lost, double-freed or left outstanding
        @Test
        void sweepRacesBorrowsSafely() throws Exception {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 64L * 8192, 1 << 20, 1024);
            AtomicBoolean failed = new AtomicBoolean(false);
            AtomicBoolean done = new AtomicBoolean(false);
            long window = TimeUnit.MILLISECONDS.toNanos(1);
            Thread sweeper = new Thread(() -> {
                while (!done.get()) {
                    pool.reapIdle(System.nanoTime(), window);
                    Thread.onSpinWait();
                }
            });
            sweeper.start();
            try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
                for (int t = 0; t < 8; t++) {
                    executor.execute(() -> {
                        for (int i = 0; i < 5000; i++) {
                            try (SegmentPool.Pooled pooled = pool.borrow(256 + (i % 4) * 1024)) {
                                MemorySegment segment = pooled.segment();
                                segment.set(ValueLayout.JAVA_INT, 0, i);
                                if (segment.get(ValueLayout.JAVA_INT, 0) != i) {
                                    failed.set(true);
                                }
                            } catch (RuntimeException _) {
                                failed.set(true);
                            }
                        }
                    });
                }
                executor.shutdown();
                assertThat(executor.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
            } finally {
                done.set(true);
                sweeper.join(10_000);
            }
            assertThat(failed).isFalse();
            assertThat(pool.stats().outstandingBorrows()).isZero();
            pool.reapIdle(System.nanoTime() + window, window);
            assertThat(pool.stats().freeSegments()).isZero();
            assertThat(pool.stats().retainedBytes()).isZero();
        }

        @Test
        void reapFreesBackingsPastWindow() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(512);
            MemorySegment segment = pooled.segment();
            pooled.close();
            long retainedAtNanos = System.nanoTime();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);

            long window = TimeUnit.SECONDS.toNanos(60);
            pool.reapIdle(retainedAtNanos, window);
            assertThat(pool.stats().freeSegments())
                    .as("kept while still within the idle window")
                    .isEqualTo(1);

            pool.reapIdle(retainedAtNanos + window + 1, window);
            assertThat(pool.stats().freeSegments())
                    .as("freed once idle past the window")
                    .isZero();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("a reaped backing's arena is closed")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void zeroIdleThresholdKeepsBackings() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            pool.borrow(512).close();
            pool.reapIdle(System.nanoTime() + TimeUnit.SECONDS.toNanos(3600), 0);
            assertThat(pool.stats().freeSegments())
                    .as("a zero threshold disables reaping")
                    .isEqualTo(1);
        }

        @Test
        void abandonedPoolIsCollectible() {
            WeakReference<SegmentPool> ref = poolWithRunningReaper();
            assertThat(awaitCollected(ref))
                    .as(
                            "a pool dropped without close() stays collectible because the reaper holds only a weak reference")
                    .isTrue();
        }

        // A pool without idle decay has no timer to free its backings
        @Test
        void abandonedPoolFreesBackings() {
            MemorySegment retained = segmentRetainedByAbandonedPool();
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                System.gc();
                return !retained.scope().isAlive();
            });
        }

        private static MemorySegment segmentRetainedByAbandonedPool() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(512);
            MemorySegment segment = pooled.segment();
            pooled.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            return segment;
        }

        private static WeakReference<SegmentPool> poolWithRunningReaper() {
            SegmentPool pool = SegmentPool.create(
                    new SegmentPool.Options(8192, 1 << 20, 1 << 20, TimeUnit.SECONDS.toNanos(60), 1024));
            pool.borrow(512).close();
            return new WeakReference<>(pool);
        }

        private static boolean awaitCollected(WeakReference<?> ref) {
            for (int attempt = 0; attempt < 50 && ref.get() != null; attempt++) {
                System.gc();
            }
            return ref.get() == null;
        }
    }

    // the borrow shape kept by glibc in its arenas: threads, sizes below one freed earlier, a live buffer between two
    /**
     * Striping: the free lists are split over independent stripes, each thread works its own, and a borrow that finds
     * nothing in its stripe takes from another before allocating. The caps, the ceiling, the sweep and close keep their
     * meaning over all stripes together.
     */
    @Nested
    class Stripes {

        @Test
        void policyPoolsHaveOneStripe() {
            assertThat(new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024).stripes())
                    .as("the policy tests build a single stripe")
                    .isEqualTo(1);
        }

        @Test
        void optionsPickTheStripeCount() {
            long megabyte = 1L << 20;
            SegmentPool.Options options = new SegmentPool.Options(8192, 512 * megabyte, megabyte, 0, 8192);
            DefaultSegmentPool pool = DefaultSegmentPool.fromOptions(options);
            try {
                assertThat(pool.stripes())
                        .isEqualTo(StripedFreeLists.stripeCount(
                                Runtime.getRuntime().availableProcessors(), 512 * megabyte, 8192));
            } finally {
                pool.close();
            }
        }

        @Test
        void returnOnOtherThreadIsReused() throws Exception {
            DefaultSegmentPool pool = stripedPool(4);
            long[] address = new long[2];
            onPlatformThread(() -> {
                SegmentPool.Pooled first = pool.borrow(512);
                address[0] = first.segment().address();
                first.close();
            });
            onPlatformThread(() -> {
                try (SegmentPool.Pooled second = pool.borrow(512)) {
                    address[1] = second.segment().address();
                    assertThat(pool.stats().freeSegments()).isZero();
                }
            });
            assertThat(address[1]).as("taken from the other thread's stripe").isEqualTo(address[0]);
            assertThat(pool.stats().totalBorrows()).isEqualTo(2);
        }

        @Test
        void retentionCapHoldsAcrossThreads() throws Exception {
            long maxPooledBytes = 4096;
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, maxPooledBytes, 1 << 20, Long.MAX_VALUE, 0, 1024, 4);
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                onPlatformThread(() -> held.add(pool.borrow(512)));
            }
            for (SegmentPool.Pooled pooled : held) {
                onPlatformThread(pooled::close);
            }
            assertThat(pool.stats().retainedBytes()).isLessThanOrEqualTo(maxPooledBytes);
            assertThat(pool.stats().freeSegments())
                    .as("four 1024-byte backings under a 4096 cap")
                    .isEqualTo(4);
        }

        // Four buffers returned on four threads land in four stripes.
        @Test
        void reapIdleReachesEveryStripe() throws Exception {
            DefaultSegmentPool pool = stripedPool(4);
            for (SegmentPool.Pooled pooled : borrowFour(pool)) {
                onPlatformThread(pooled::close);
            }
            assertThat(pool.stats().freeSegments()).isEqualTo(4);
            long window = TimeUnit.SECONDS.toNanos(60);
            pool.reapIdle(System.nanoTime() + window + 1, window);
            assertThat(pool.stats().freeSegments()).isZero();
            assertThat(pool.stats().retainedBytes()).isZero();
        }

        @Test
        void closeDrainsEveryStripe() throws Exception {
            DefaultSegmentPool pool = stripedPool(4);
            List<MemorySegment> segments = new ArrayList<>();
            for (SegmentPool.Pooled pooled : borrowFour(pool)) {
                segments.add(pooled.segment());
                onPlatformThread(pooled::close);
            }
            assertThat(pool.stats().freeSegments()).isEqualTo(4);
            pool.close();
            assertThat(pool.stats().freeSegments()).isZero();
            for (MemorySegment segment : segments) {
                assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                        .isInstanceOf(IllegalStateException.class);
            }
        }

        @Test
        void virtualThreadReturnsAreReused() throws Exception {
            DefaultSegmentPool pool = stripedPool(4);
            long[] address = new long[2];
            Thread virtual = Thread.ofVirtual().start(() -> {
                SegmentPool.Pooled first = pool.borrow(512);
                address[0] = first.segment().address();
                first.close();
            });
            virtual.join();
            onPlatformThread(() -> {
                try (SegmentPool.Pooled second = pool.borrow(512)) {
                    address[1] = second.segment().address();
                }
            });
            assertThat(address[1]).isEqualTo(address[0]);
        }

        private List<SegmentPool.Pooled> borrowFour(SegmentPool pool) {
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                held.add(pool.borrow(512));
            }
            return held;
        }

        private DefaultSegmentPool stripedPool(int stripes) {
            return new DefaultSegmentPool(8192, 1 << 20, 1 << 20, Long.MAX_VALUE, 0, 1024, stripes);
        }

        private void onPlatformThread(Runnable body) throws InterruptedException {
            Throwable[] failure = new Throwable[1];
            Thread thread = Thread.ofPlatform().start(() -> {
                try {
                    body.run();
                } catch (Throwable t) {
                    failure[0] = t;
                }
            });
            thread.join();
            if (failure[0] != null) {
                throw new AssertionError(failure[0]);
            }
        }
    }

    @Test
    void freedBackingsLeaveTheProcess() {
        Path status = Path.of("/proc/self/status");
        assumeTrue(Files.isReadable(status), "needs the Linux /proc/self/status");
        assumeTrue(MappedMemory.mapsLargeBackings(), "needs mapped backings");
        DefaultSegmentPool pool = new DefaultSegmentPool(4L << 20, 0L, 0L, 8192);
        int threads = 8;
        long[] megabytes = {4, 5, 6, 7, 8, 9, 10, 11};
        List<SegmentPool.Pooled> pinned = Collections.synchronizedList(new ArrayList<>());
        long before = rssAnonKb(status);
        runOnPlatformThreads(threads, () -> {
            SegmentPool.Pooled first = pool.borrow(12L << 20);
            touchEveryPage(first.segment());
            first.close();
            for (long mb : megabytes) {
                SegmentPool.Pooled large = pool.borrow(mb << 20);
                touchEveryPage(large.segment());
                pinned.add(pool.borrow(8192));
                large.close();
            }
        });
        long after = rssAnonKb(status);
        pinned.forEach(SegmentPool.Pooled::close);
        long returnedMb = threads * (4 + 5 + 6 + 7 + 8 + 9 + 10 + 11);
        assertThat((after - before) >> 10)
                .as("anonymous RSS growth in MB after returning %d MB", returnedMb)
                .isLessThan(64);
    }

    private static long rssAnonKb(Path status) {
        try {
            for (String line : Files.readAllLines(status)) {
                if (line.startsWith("RssAnon:")) {
                    return Long.parseLong(line.replaceAll("[^0-9]", ""));
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        throw new IllegalStateException("no RssAnon line in " + status);
    }

    private static void touchEveryPage(MemorySegment segment) {
        for (long offset = 0; offset < segment.byteSize(); offset += 4096) {
            segment.set(ValueLayout.JAVA_BYTE, offset, (byte) 1);
        }
    }

    private static void runOnPlatformThreads(int count, Runnable body) {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    body.run();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }));
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        if (failure.get() != null) {
            throw new AssertionError("a worker failed", failure.get());
        }
    }
}
