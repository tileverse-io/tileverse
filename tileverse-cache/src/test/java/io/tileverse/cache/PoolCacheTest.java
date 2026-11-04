/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.github.benmanes.caffeine.cache.Policy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class PoolCacheTest {

    private final AtomicLong nanos = new AtomicLong();

    private final PoolCache pool = PoolCache.create(CachePool.DATA, 1_000L, nanos::get, Runnable::run);

    private final PartitionSpec<String, String> lengthWeigher =
            new PartitionSpec<>((key, value) -> value.length(), Duration.ofSeconds(10));

    private final PartitionSpec<String, String> tenEach =
            new PartitionSpec<>((key, value) -> 10, Duration.ofMinutes(1));

    @Test
    void samePartitionNameSameInstance() {
        Cache<String, String> first = pool.partition("a", lengthWeigher);
        Cache<String, String> second = pool.partition("a", lengthWeigher);

        assertThat(second).isSameAs(first);
    }

    @Test
    void secondSpecForANameFails() {
        pool.partition("a", lengthWeigher);

        assertThatThrownBy(() -> pool.partition("a", tenEach)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void partitionsDoNotShareKeys() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);
        Cache<String, String> b = pool.partition("b", tenEach);

        a.get("k", key -> "v");

        assertThat(b.getIfPresent("k")).isNull();
        assertThat(a.getIfPresent("k")).isEqualTo("v");
    }

    @Test
    void subPartitionsDoNotShareKeys() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);

        a.subCache("x").get("k", key -> "v");

        assertThat(a.subCache("y").getIfPresent("k")).isNull();
        assertThat(a.getIfPresent("k")).isNull();
        assertThat(a.subCache("x").getIfPresent("k")).isEqualTo("v");
    }

    @Test
    void subPartitionOfAViewFails() {
        Cache<String, String> view = pool.partition("a", lengthWeigher).subCache("x");

        assertThatThrownBy(() -> view.subCache("y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void poolRefusesSubCache() {
        assertThatThrownBy(() -> pool.subCache("x"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("partition(name, spec)");
    }

    @Test
    void weightedSizeFollowsWeighers() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);
        Cache<String, String> b = pool.partition("b", tenEach);

        a.get("k", key -> "abc");
        b.get("k", key -> "hello");
        pool.getNativeCache().cleanUp();

        assertThat(weightedSize(pool)).hasValue(13);
    }

    @Test
    void ttlOfPartitionApplies() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);
        Cache<String, String> b = pool.partition("b", tenEach);
        a.get("k", key -> "v");
        b.get("k", key -> "v");

        nanos.addAndGet(Duration.ofSeconds(30).toNanos());

        assertThat(a.getIfPresent("k")).isNull();
        assertThat(b.getIfPresent("k")).isEqualTo("v");
    }

    @Test
    void evictionCrossesPartitions() {
        PoolCache small = PoolCache.create(CachePool.DATA, 100L, nanos::get, Runnable::run);
        Cache<String, String> a = small.partition("a", lengthWeigher);
        Cache<String, String> b = small.partition("b", lengthWeigher);
        String sixtyChars = "x".repeat(60);

        a.get("k", key -> sixtyChars);
        b.get("k", key -> sixtyChars);
        small.getNativeCache().cleanUp();

        CacheStats statsOfA = a.stats();
        CacheStats statsOfB = b.stats();
        assertThat(statsOfA.evictionCount() + statsOfB.evictionCount()).isEqualTo(1);
        assertThat(statsOfA.evictionWeight() + statsOfB.evictionWeight()).isEqualTo(60);
        assertThat(statsOfA.entryCount() + statsOfB.entryCount()).isEqualTo(1);
    }

    @Test
    void statsCountLookups() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);

        CacheStats afterLoad = loadThenLookUp(a);

        CacheStats stats = a.stats();
        assertThat(afterLoad.loadCount()).isEqualTo(1);
        assertThat(afterLoad.missCount()).isEqualTo(1);
        assertThat(afterLoad.entryCount()).isEqualTo(1);
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(2);
        assertThat(stats.loadCount()).isEqualTo(1);
        assertThat(stats.hitRate()).isEqualTo(1.0 / 3);
    }

    @Test
    void loadTimeUsesTheTicker() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);

        a.get("k", key -> {
            nanos.addAndGet(Duration.ofMillis(5).toNanos());
            return "v";
        });

        assertThat(a.stats().averageLoadTime()).isEqualTo(5_000_000.0);
    }

    @Test
    void invalidateAllScopes() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);
        Cache<String, String> b = pool.partition("b", tenEach);
        a.get("k", key -> "a");
        a.subCache("x").get("k", key -> "x");
        a.subCache("y").get("k", key -> "y");
        b.get("k", key -> "b");

        a.subCache("x").invalidateAll();
        assertThat(a.subCache("x").getIfPresentQuietly("k")).isNull();
        assertThat(a.subCache("y").getIfPresentQuietly("k")).isEqualTo("y");
        assertThat(a.getIfPresentQuietly("k")).isEqualTo("a");

        a.invalidateAll();
        assertThat(a.getIfPresentQuietly("k")).isNull();
        assertThat(a.subCache("y").getIfPresentQuietly("k")).isNull();
        assertThat(b.getIfPresentQuietly("k")).isEqualTo("b");

        pool.invalidateAll();
        assertThat(pool.getNativeCache().asMap()).isEmpty();
    }

    // The scope of a partition includes the keys of its sub-partitions
    @Test
    void keysAndForEachStayInScope() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);
        Cache<String, String> b = pool.partition("b", tenEach);
        a.get("a1", key -> "v");
        a.get("a2", key -> "v");
        a.subCache("x").get("x1", key -> "in x");
        a.subCache("y").get("y1", key -> "in y");
        b.get("b1", key -> "v");
        Map<String, String> seenInX = new HashMap<>();

        a.subCache("x").forEach(seenInX::put);

        assertThat(a.keys()).containsExactlyInAnyOrder("a1", "a2", "x1", "y1");
        assertThat(a.subCache("y").keys()).containsExactly("y1");
        assertThat(seenInX).containsExactly(Map.entry("x1", "in x"));
    }

    @Test
    void nullFromLoaderStoresNothing() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);

        String loaded = a.get("k", key -> null);

        assertThat(loaded).isNull();
        assertThat(a.getIfPresent("k")).isNull();
        assertThat(a.stats().entryCount()).isZero();
        assertThat(a.stats().loadCount()).isEqualTo(1);
    }

    @Test
    void completedEntryWeighsItsValue() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);

        a.get("k", key -> "abcd");
        pool.getNativeCache().cleanUp();

        assertThat(weightedSize(pool)).hasValue(4);
    }

    @Test
    void partitionNamesAndStats() {
        Cache<String, String> a = pool.partition("a", lengthWeigher);
        pool.partition("b", tenEach);
        loadThenLookUp(a);

        Map<String, CacheStats> stats = pool.partitionStats();

        assertThat(pool.partitionNames()).containsExactlyInAnyOrder("a", "b");
        assertThat(stats).containsOnlyKeys("a", "b").containsEntry("a", a.stats());
        assertThat(stats.get("a").hitCount()).isEqualTo(1);
        assertThat(stats.get("a").missCount()).isEqualTo(2);
    }

    /** Loads one entry, then looks it up once, an absent key once, and both quietly. */
    private static CacheStats loadThenLookUp(Cache<String, String> partition) {
        partition.get("k", key -> "v");
        CacheStats afterLoad = partition.stats();
        partition.getIfPresent("k");
        partition.getIfPresent("absent");
        partition.getIfPresentQuietly("k");
        partition.getIfPresentQuietly("absent");
        return afterLoad;
    }

    private static OptionalLong weightedSize(PoolCache pool) {
        Policy<Object, Object> policy = pool.getNativeCache().policy();
        Policy.Eviction<Object, Object> eviction = policy.eviction().orElseThrow();
        return eviction.weightedSize();
    }

    @Nested
    class Loading {

        private static final Duration TIMEOUT = Duration.ofSeconds(10);

        private static final int THREADS = 8;

        private static final int WAITERS = 3;

        private final ExecutorService threads = Executors.newFixedThreadPool(THREADS);

        @AfterEach
        void shutDownThreads() throws InterruptedException {
            threads.shutdownNow();
            assertThat(threads.awaitTermination(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .isTrue();
        }

        @Test
        void concurrentGetsRunLoaderOnce() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            BlockedLoader loader = new BlockedLoader("v");
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                results.add(threads.submit(() -> a.get("k", loader)));
            }
            awaitHits(a, THREADS - 1);

            loader.release();

            for (Future<String> result : results) {
                assertThat(resultOf(result)).isEqualTo("v");
            }
            assertThat(loader.calls()).isEqualTo(1);
        }

        @Test
        void getIfPresentNullWhileLoading() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            BlockedLoader loader = new BlockedLoader("v");
            Future<String> loading = threads.submit(() -> a.get("k", loader));
            loader.awaitStart();
            long missesBefore = a.stats().missCount();

            String whileLoading = a.getIfPresent("k");

            long missesAfter = a.stats().missCount();
            loader.release();
            assertThat(whileLoading).isNull();
            assertThat(missesAfter).isEqualTo(missesBefore + 1);
            assertThat(resultOf(loading)).isEqualTo("v");
        }

        // The loader fails once the waiters wait on its claim. A waiter running its own loader would get no throwable.
        @ParameterizedTest
        @MethodSource("loaderFailures")
        void failingLoaderRethrowsToWaiters(Throwable failure) throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            CountDownLatch started = new CountDownLatch(1);
            Function<String, String> failingLoader = key -> {
                started.countDown();
                awaitHits(a, WAITERS);
                return throwFailure(failure);
            };
            Future<Throwable> loading = threads.submit(() -> catchThrowable(() -> a.get("k", failingLoader)));
            awaitLatch(started);
            List<Future<Throwable>> waiters = new ArrayList<>();
            for (int i = 0; i < WAITERS; i++) {
                waiters.add(threads.submit(() -> catchThrowable(() -> a.get("k", key -> "loaded by a waiter"))));
            }

            assertThat(resultOf(loading)).isSameAs(failure);
            for (Future<Throwable> waiter : waiters) {
                assertThat(resultOf(waiter)).isSameAs(failure);
            }
            assertThat(a.getIfPresent("k")).isNull();
            assertThat(pool.getNativeCache().asMap()).isEmpty();
            assertThat(a.stats().entryCount()).isZero();
        }

        static Stream<Throwable> loaderFailures() {
            return Stream.of(new IllegalStateException("boom"), new StackOverflowError());
        }

        @Test
        void errorFromLoaderPropagates() {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            StackOverflowError overflow = new StackOverflowError();

            Throwable thrown = catchThrowable(() -> a.get("k", key -> {
                throw overflow;
            }));

            assertThat(thrown).isSameAs(overflow);
            assertThat(pool.getNativeCache().asMap()).isEmpty();
        }

        @Test
        void nextGetAfterFailureLoadsAgain() {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            IllegalStateException boom = new IllegalStateException("boom");
            assertThatThrownBy(() -> a.get("k", key -> {
                        throw boom;
                    }))
                    .isSameAs(boom);

            String loaded = a.get("k", key -> "v");

            assertThat(loaded).isEqualTo("v");
            assertThat(a.stats().loadCount()).isEqualTo(2);
        }

        @Test
        void loaderMayStoreInAnotherPartition() {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            Cache<String, String> b = pool.partition("b", tenEach);

            String loaded = a.get("k", key -> b.get("x", x -> "y") + "!");

            assertThat(loaded).isEqualTo("y!");
            assertThat(b.getIfPresent("x")).isEqualTo("y");
        }

        @Test
        void loaderMayStoreAnotherKeyOfItsOwn() {
            Cache<String, String> a = pool.partition("a", lengthWeigher);

            String loaded = a.get("k", key -> {
                a.get("j", j -> "J");
                return "K";
            });

            assertThat(loaded).isEqualTo("K");
            assertThat(a.getIfPresent("k")).isEqualTo("K");
            assertThat(a.getIfPresent("j")).isEqualTo("J");
        }

        // The shared load waits until the three other loads wait on its claim
        @Test
        void concurrentNestedLoadsFinish() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            Cache<String, String> b = pool.partition("b", tenEach);
            Function<String, String> slowShared = shared -> {
                awaitHits(b, WAITERS);
                return "S";
            };
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < WAITERS + 1; i++) {
                String key = "k" + i;
                results.add(threads.submit(() -> a.get(key, k -> b.get("shared", slowShared))));
            }

            for (Future<String> result : results) {
                assertThat(resultOf(result)).isEqualTo("S");
            }
            assertThat(b.stats().loadCount()).isEqualTo(1);
        }

        @Test
        void interruptedWaiterGetsValue() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            BlockedLoader loader = new BlockedLoader("v");
            Future<String> loading = threads.submit(() -> a.get("k", loader));
            loader.awaitStart();
            AtomicReference<Thread> waiterThread = new AtomicReference<>();
            AtomicBoolean interruptedAfterGet = new AtomicBoolean();
            Future<String> waiting = threads.submit(() -> {
                waiterThread.set(Thread.currentThread());
                String value = a.get("k", key -> "loaded by the waiter");
                interruptedAfterGet.set(Thread.currentThread().isInterrupted());
                return value;
            });
            await().atMost(TIMEOUT).until(() -> isWaiting(waiterThread.get()));

            waiterThread.get().interrupt();
            loader.release();

            assertThat(resultOf(waiting)).isEqualTo("v");
            assertThat(interruptedAfterGet).isTrue();
            assertThat(resultOf(loading)).isEqualTo("v");
        }

        @Test
        void invalidateDuringLoad() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            BlockedLoader loader = new BlockedLoader("v");
            Future<String> loading = threads.submit(() -> a.get("k", loader));
            loader.awaitStart();

            pool.invalidateAll();
            loader.release();

            assertThat(resultOf(loading)).isEqualTo("v");
            assertThat(a.getIfPresent("k")).isNull();
            await().atMost(TIMEOUT).until(() -> a.stats().entryCount() == 0);
        }

        @Test
        void otherKeyLoadsDuringALoad() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);
            BlockedLoader loader = new BlockedLoader("v");
            Future<String> loading = threads.submit(() -> a.get("k", loader));
            loader.awaitStart();

            Future<String> other = threads.submit(() -> a.get("j", j -> "J"));

            assertThat(resultOf(other)).isEqualTo("J");
            assertThat(loading).isNotDone();
            loader.release();
            assertThat(resultOf(loading)).isEqualTo("v");
        }

        // Runs on another thread: a load waiting on its own claim would block the test forever
        @Test
        void recursiveLoadOfOwnKeyFails() throws Exception {
            Cache<String, String> a = pool.partition("a", lengthWeigher);

            Future<Throwable> recursive =
                    threads.submit(() -> catchThrowable(() -> a.get("k", k -> a.get("k", j -> "x"))));

            assertThat(resultOf(recursive)).isInstanceOf(IllegalStateException.class);
            assertThat(a.getIfPresent("k")).isNull();
            assertThat(a.get("k", k -> "v")).isEqualTo("v");
        }

        @Test
        void completedClaimForgetsOwner() {
            Partition<String, String> a = (Partition<String, String>) pool.partition("a", lengthWeigher);
            a.get("k", key -> "v");
            PoolKey poolKey = new PoolKey(a.partitionId(), PoolKey.NO_SUB_PARTITION, "k");

            CompletableFuture<Object> entry = pool.asyncCache().asMap().get(poolKey);

            assertThat(entry).isInstanceOf(Partition.Claim.class);
            assertThat(((Partition.Claim) entry).owner()).isNull();
        }

        private static <T> T resultOf(Future<T> future) throws Exception {
            return future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }

        private static void awaitHits(Cache<?, ?> cache, long hits) {
            await().atMost(TIMEOUT).until(() -> cache.stats().hitCount() >= hits);
        }

        private static boolean isWaiting(Thread thread) {
            return thread != null && thread.getState() == Thread.State.WAITING;
        }

        /** Throws {@code failure}, a runtime exception or an error, from a loader. */
        private static String throwFailure(Throwable failure) {
            if (failure instanceof Error error) {
                throw error;
            }
            throw (RuntimeException) failure;
        }

        private static void awaitLatch(CountDownLatch latch) {
            try {
                boolean released = latch.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                assertThat(released).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        /** A loader blocked until the test releases it. */
        private static final class BlockedLoader implements Function<String, String> {

            private final CountDownLatch started = new CountDownLatch(1);

            private final CountDownLatch released = new CountDownLatch(1);

            private final AtomicInteger calls = new AtomicInteger();

            private final String value;

            BlockedLoader(String value) {
                this.value = value;
            }

            @Override
            public String apply(String key) {
                calls.incrementAndGet();
                started.countDown();
                awaitLatch(released);
                return value;
            }

            void awaitStart() {
                awaitLatch(started);
            }

            void release() {
                released.countDown();
            }

            int calls() {
                return calls.get();
            }
        }
    }
}
