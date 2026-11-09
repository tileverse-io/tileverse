/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * A partition of a {@link PoolCache}, or a sub-partition view of one.
 *
 * <p>The root of a partition is registered with its pool under a name. A sub-partition is a view of the root with a
 * second id. It shares the spec and the statistics of its root. The pool does not keep views: each new version of a
 * source gets a new sub-partition id, and a map of views would grow for the life of the process.
 *
 * <p>The scope of a root includes the entries of its sub-partitions: {@link #keys()}, {@link #forEach(BiConsumer)} and
 * {@link #invalidateAll()} on the root reach them too.
 */
@NullMarked
final class Partition<K, V> implements Cache<K, V> {

    private final PoolCache pool;

    private final String name;

    private final long partitionId;

    private final long subPartitionId;

    private final PartitionSpec<K, V> spec;

    private final PartitionCounters counters;

    Partition(PoolCache pool, String name, PartitionSpec<K, V> spec) {
        this(pool, name, SubCache.computeCacheId(name), PoolKey.NO_SUB_PARTITION, spec, new PartitionCounters());
    }

    private Partition(
            PoolCache pool,
            String name,
            long partitionId,
            long subPartitionId,
            PartitionSpec<K, V> spec,
            PartitionCounters counters) {
        this.pool = pool;
        this.name = name;
        this.partitionId = partitionId;
        this.subPartitionId = subPartitionId;
        this.spec = spec;
        this.counters = counters;
    }

    String name() {
        return name;
    }

    long partitionId() {
        return partitionId;
    }

    PartitionSpec<K, V> spec() {
        return spec;
    }

    PoolCache pool() {
        return pool;
    }

    /**
     * Returns a new view of the sub-partition {@code subCacheId} of this partition.
     *
     * @throws UnsupportedOperationException if this partition is a sub-partition view
     */
    @Override
    public Cache<K, V> subCache(String subCacheId) {
        requireNonNull(subCacheId, "subCacheId");
        if (isView()) {
            throw new UnsupportedOperationException("A sub-partition of " + name + " has no sub-partitions");
        }
        long viewId = SubCache.computeCacheId(subCacheId);
        return new Partition<>(pool, name, partitionId, viewId, spec, counters);
    }

    private boolean isView() {
        return subPartitionId != PoolKey.NO_SUB_PARTITION;
    }

    @Override
    public @Nullable V getIfPresent(K key) {
        V value = presentValue(entries().getIfPresent(poolKey(key)));
        if (value == null) {
            counters.recordMiss();
        } else {
            counters.recordHit();
        }
        return value;
    }

    @Override
    public @Nullable V getIfPresentQuietly(K key) {
        Object value = entries().policy().getIfPresentQuietly(poolKey(key));
        return presentValue(value);
    }

    /**
     * Returns the value of {@code key}, loading it with {@code mappingFunction} on the calling thread if needed.
     *
     * <p>The caller installs a pending future for the key, called its claim. The pool holds a lock for that step only:
     * {@code mappingFunction} runs outside the locks of the pool and may read and store entries of the pool, except the
     * key being loaded. A caller finding the claim of another caller waits for its value. An interrupt does not end the
     * wait, and the thread keeps its interrupt status.
     *
     * <p>A {@code null} value completes the claim with {@code null}: the pool removes the entry and this method returns
     * {@code null}. If {@code mappingFunction} throws, the claim leaves the pool, and the loading caller and the
     * waiters get the same throwable.
     *
     * <p>A loader asking for its own key on the loading thread gets an {@link IllegalStateException}. A recursive load
     * through another thread is not detected, as in Caffeine, and never completes.
     */
    @Override
    public @Nullable V get(K key, Function<? super K, ? extends V> mappingFunction) {
        requireNonNull(mappingFunction, "mappingFunction");
        PoolKey poolKey = poolKey(key);
        Claim claim = new Claim();
        CompletableFuture<Object> installed = pool.asyncCache().get(poolKey, (k, executor) -> claim);
        if (installed != claim) {
            return awaitValue(installed);
        }
        counters.recordMiss();
        return loadInto(claim, poolKey, key, mappingFunction);
    }

    /** Waits for the claim of another caller. A claim owned by the calling thread fails: the wait would never end. */
    private @Nullable V awaitValue(CompletableFuture<Object> installed) {
        if (installed instanceof Claim claim && claim.isPendingOn(Thread.currentThread())) {
            throw new IllegalStateException("Recursive load of a key of partition " + name);
        }
        counters.recordHit();
        return valueOf(installed.join());
    }

    /** Returns the value of a completed claim, or throws the failure of its load. */
    private @Nullable V valueOf(@Nullable Object completed) {
        if (completed instanceof FailedLoad failedLoad) {
            failedLoad.rethrow();
        }
        return asValue(completed);
    }

    /**
     * Runs the loader and completes the claim with its value. If the loader throws, the claim leaves the pool and
     * completes with a {@link FailedLoad}.
     *
     * <p>The finally block completes a claim still pending if that cleanup fails too. A claim left pending would keep
     * its waiters waiting forever.
     */
    // The throwable is rethrown as it is once the claim is cleaned up
    @SuppressWarnings("java:S1181")
    private @Nullable V loadInto(
            Claim claim, PoolKey poolKey, K key, Function<? super K, ? extends V> mappingFunction) {
        try {
            V value = timedLoad(key, mappingFunction);
            complete(claim, value);
            return value;
        } catch (Throwable failure) {
            fail(claim, poolKey, failure);
            throw failure;
        } finally {
            abandonIfPending(claim, poolKey);
        }
    }

    private @Nullable V timedLoad(K key, Function<? super K, ? extends V> mappingFunction) {
        Ticker ticker = pool.ticker();
        long start = ticker.read();
        try {
            return mappingFunction.apply(key);
        } finally {
            counters.recordLoad(ticker.read() - start);
        }
    }

    /**
     * Counts the entry before completing the claim. A removal right after the completion then decrements an entry
     * already counted.
     */
    private void complete(Claim claim, @Nullable V value) {
        if (value != null) {
            counters.recordStore();
        }
        claim.completeWith(value);
    }

    /**
     * Removes the claim from the pool, then completes it with the failure for its waiters. A caller arriving after the
     * removal loads the key again instead of getting the failure.
     */
    private void fail(Claim claim, PoolKey poolKey, Throwable failure) {
        pool.asyncCache().asMap().remove(poolKey, claim);
        claim.completeWith(new FailedLoad(failure));
    }

    private void abandonIfPending(Claim claim, PoolKey poolKey) {
        if (!claim.isDone()) {
            fail(claim, poolKey, new IllegalStateException("load abandoned"));
        }
    }

    /** Returns the statistics of the partition, shared by its root and its sub-partition views. */
    @Override
    public CacheStats stats() {
        return counters.snapshot();
    }

    @Override
    public Cache<K, V> invalidateAll() {
        entries().invalidateAll(keysInScope());
        return this;
    }

    @Override
    public void invalidate(K key) {
        entries().invalidate(poolKey(key));
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        List<PoolKey> poolKeys = new ArrayList<>();
        for (K key : keys) {
            poolKeys.add(poolKey(key));
        }
        entries().invalidateAll(poolKeys);
    }

    @Override
    public Collection<K> keys() {
        List<K> keys = new ArrayList<>();
        forEach((key, value) -> keys.add(key));
        return Collections.unmodifiableList(keys);
    }

    @Override
    public void forEach(BiConsumer<K, V> consumer) {
        entries().asMap().forEach((poolKey, value) -> {
            if (inScope(poolKey)) {
                V present = presentValue(value);
                if (present != null) {
                    consumer.accept(outerKey(poolKey), present);
                }
            }
        });
    }

    private List<PoolKey> keysInScope() {
        Set<PoolKey> poolKeys = entries().asMap().keySet();
        return poolKeys.stream().filter(this::inScope).toList();
    }

    private boolean inScope(PoolKey poolKey) {
        boolean inPartition = poolKey.partitionId() == partitionId;
        boolean inView = poolKey.subPartitionId() == subPartitionId;
        return inPartition && (inView || !isView());
    }

    private com.github.benmanes.caffeine.cache.Cache<PoolKey, Object> entries() {
        return pool.asyncCache().synchronous();
    }

    private PoolKey poolKey(K key) {
        return new PoolKey(partitionId, subPartitionId, key);
    }

    @SuppressWarnings("unchecked")
    private K outerKey(PoolKey poolKey) {
        return (K) poolKey.key();
    }

    /** Returns {@code value} as a value of this partition, or {@code null} if it is absent or a failed load. */
    private @Nullable V presentValue(@Nullable Object value) {
        if (value instanceof FailedLoad) {
            return null;
        }
        return asValue(value);
    }

    @SuppressWarnings("unchecked")
    private @Nullable V asValue(@Nullable Object value) {
        return (V) value;
    }

    /** Weighs an entry of this partition with the weigher of its spec. */
    int weigh(PoolKey poolKey, Object value) {
        return spec.weigher().weigh(outerKey(poolKey), asValue(value));
    }

    /** Counts the removal of an entry of this partition, and its eviction when the pool evicted it. */
    void onRemoval(PoolKey poolKey, Object value, RemovalCause cause) {
        counters.recordRemoval();
        if (cause.wasEvicted()) {
            counters.recordEviction(weigh(poolKey, value));
        }
    }

    /**
     * The pending future of a load, installed in the pool by the caller running the loader. It knows the thread of that
     * caller until it completes: a loader asking for its own key on that thread would wait for itself.
     */
    static final class Claim extends CompletableFuture<Object> {

        // A completed claim stays in the pool as the entry's future: it must not keep the loading thread reachable
        // Only the reference changes, never the thread it points to: volatile is enough
        @SuppressWarnings("java:S3077")
        private volatile @Nullable Thread owner = Thread.currentThread();

        boolean isPendingOn(Thread thread) {
            return owner == thread && !isDone();
        }

        /** Forgets the owner, then completes the claim with {@code value}. */
        void completeWith(@Nullable Object value) {
            owner = null;
            super.complete(value);
        }

        /** Returns the thread running the loader, or {@code null} once the claim is complete. */
        @Nullable
        Thread owner() {
            return owner;
        }
    }

    /**
     * The statistics of a partition, shared by its root and its sub-partition views. The rates follow Caffeine: a
     * partition with no request has a hit rate of 1 and a miss rate of 0.
     */
    private static final class PartitionCounters {

        private final LongAdder hits = new LongAdder();

        private final LongAdder misses = new LongAdder();

        private final LongAdder loads = new LongAdder();

        private final LongAdder loadNanos = new LongAdder();

        private final LongAdder evictions = new LongAdder();

        private final LongAdder evictionWeight = new LongAdder();

        private final LongAdder entries = new LongAdder();

        void recordHit() {
            hits.increment();
        }

        void recordMiss() {
            misses.increment();
        }

        void recordLoad(long nanos) {
            loads.increment();
            loadNanos.add(nanos);
        }

        void recordStore() {
            entries.increment();
        }

        void recordRemoval() {
            entries.decrement();
        }

        void recordEviction(long weight) {
            evictions.increment();
            evictionWeight.add(weight);
        }

        CacheStats snapshot() {
            long hitCount = hits.sum();
            long missCount = misses.sum();
            long loadCount = loads.sum();
            long requestCount = hitCount + missCount;
            double hitRate = ratio(hitCount, requestCount, 1.0);
            double missRate = ratio(missCount, requestCount, 0.0);
            double averageLoadTime = ratio(loadNanos.sum(), loadCount, 0.0);
            return new CacheStats(
                    hitCount,
                    missCount,
                    loadCount,
                    evictions.sum(),
                    evictionWeight.sum(),
                    entries.sum(),
                    hitRate,
                    missRate,
                    averageLoadTime);
        }

        private static double ratio(long part, long whole, double whenEmpty) {
            if (whole == 0) {
                return whenEmpty;
            }
            return (double) part / whole;
        }
    }
}
