/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * One Caffeine cache with one budget of heap bytes, split into named partitions.
 *
 * <p>Each partition has its own weigher, expiry and statistics. The entries of the partitions compete for the same
 * budget, and eviction picks its victims across partitions. A partition can be split into sub-partitions, for example
 * one per source.
 *
 * <p>The Caffeine cache is asynchronous. A partition installs a pending future for a key and loads the value outside
 * the locks of the pool. The pool itself is a {@link CaffeineCache} over the synchronous view of that cache, and its
 * {@link #stats()} are the totals of the pool.
 */
@NullMarked
public final class PoolCache extends CaffeineCache<PoolKey, Object> {

    /** The average size of an entry, used to size the table of the pool once from its budget. */
    private static final int AVERAGE_ENTRY_BYTES = 64 * 1024;

    private final CachePool pool;

    private final AsyncCache<PoolKey, Object> asyncCache;

    private final PartitionRegistry partitions;

    private final Ticker ticker;

    private PoolCache(
            CachePool pool, AsyncCache<PoolKey, Object> asyncCache, PartitionRegistry partitions, Ticker ticker) {
        super(asyncCache.synchronous());
        this.pool = pool;
        this.asyncCache = asyncCache;
        this.partitions = partitions;
        this.ticker = ticker;
    }

    /**
     * Creates a pool with the system ticker. The pool runs its maintenance on the executor shared by the caches of this
     * package.
     *
     * @param maximumWeight the budget of the pool, in bytes
     */
    static PoolCache create(CachePool pool, long maximumWeight) {
        return create(pool, maximumWeight, Ticker.systemTicker(), maintenanceExecutor());
    }

    /**
     * Creates a pool.
     *
     * @param maximumWeight the budget of the pool, in bytes
     * @param ticker the clock of expiry and of load times
     * @param executor runs eviction and removal notifications
     */
    static PoolCache create(CachePool pool, long maximumWeight, Ticker ticker, Executor executor) {
        requireNonNull(pool, "pool");
        requireNonNull(ticker, "ticker");
        requireNonNull(executor, "executor");
        PartitionRegistry partitions = new PartitionRegistry();
        AsyncCache<PoolKey, Object> asyncCache = buildAsyncCache(partitions, maximumWeight, ticker, executor);
        return new PoolCache(pool, asyncCache, partitions, ticker);
    }

    /**
     * Builds the Caffeine cache of a pool. The pool does not exist yet: the weigher, the expiry and the removal
     * listener reach the partitions through the registry.
     */
    private static AsyncCache<PoolKey, Object> buildAsyncCache(
            PartitionRegistry partitions, long maximumWeight, Ticker ticker, Executor executor) {
        Weigher<PoolKey, Object> weigher = partitions::weigh;
        Expiry<PoolKey, Object> expiry = Expiry.accessing((key, value) -> partitions.expireAfterAccess(key));
        RemovalListener<PoolKey, Object> removalListener = partitions::onRemoval;
        int initialCapacity = CaffeineCache.Builder.computeInitialCapacity(maximumWeight, AVERAGE_ENTRY_BYTES);
        Caffeine<PoolKey, Object> entryPolicy = Caffeine.newBuilder()
                .maximumWeight(maximumWeight)
                .weigher(weigher)
                .expireAfter(expiry)
                .removalListener(removalListener);
        return entryPolicy
                .scheduler(Scheduler.systemScheduler())
                .ticker(ticker)
                .executor(executor)
                .initialCapacity(initialCapacity)
                .recordStats()
                .buildAsync();
    }

    /** Returns the pool of this cache. */
    public CachePool pool() {
        return pool;
    }

    /**
     * Returns the partition registered under {@code name}, registering it with {@code spec} the first time.
     *
     * @throws IllegalStateException if {@code name} is registered with another spec, or if another name has the same
     *     partition id
     */
    public <K, V> Cache<K, V> partition(String name, PartitionSpec<K, V> spec) {
        requireNonNull(name, "name");
        requireNonNull(spec, "spec");
        return partitions.register(this, name, spec);
    }

    /**
     * Refuses a sub-cache of the pool. Its entries would have no spec, and the weigher of the pool could not weigh
     * them.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Cache<PoolKey, Object> subCache(String subCacheId) {
        throw new UnsupportedOperationException("A pool has no sub-caches: use partition(name, spec)");
    }

    /** Returns the statistics of the partitions, by partition name. */
    public Map<String, CacheStats> partitionStats() {
        return partitions.stats();
    }

    /** Returns the names of the registered partitions. */
    public Collection<String> partitionNames() {
        return partitions.names();
    }

    AsyncCache<PoolKey, Object> asyncCache() {
        return asyncCache;
    }

    Ticker ticker() {
        return ticker;
    }

    /**
     * The partitions of a pool, by name and by id. The weigher, the expiry and the removal listener of the pool find
     * the partition of a key by its partition id.
     */
    private static final class PartitionRegistry {

        private final Map<String, Partition<?, ?>> byName = new ConcurrentHashMap<>();

        private final Map<Long, Partition<?, ?>> byId = new ConcurrentHashMap<>();

        /**
         * Registers a partition. Looking up a known name takes no lock: consumers register their partitions each time
         * they use them.
         */
        <K, V> Partition<K, V> register(PoolCache pool, String name, PartitionSpec<K, V> spec) {
            Partition<?, ?> registered = byName.get(name);
            if (registered == null) {
                registered = addIfAbsent(pool, name, spec);
            }
            return requireSameSpec(registered, spec);
        }

        private synchronized <K, V> Partition<?, ?> addIfAbsent(PoolCache pool, String name, PartitionSpec<K, V> spec) {
            Partition<?, ?> registered = byName.get(name);
            if (registered != null) {
                return registered;
            }
            return add(new Partition<>(pool, name, spec));
        }

        private <K, V> Partition<K, V> add(Partition<K, V> partition) {
            Partition<?, ?> sameId = byId.get(partition.partitionId());
            if (sameId != null) {
                throw new IllegalStateException(
                        "Partition names %s and %s have the same id".formatted(sameId.name(), partition.name()));
            }
            // A partition found by its name must already be found by its id, as required by the pool's callbacks
            byId.put(partition.partitionId(), partition);
            byName.put(partition.name(), partition);
            return partition;
        }

        @SuppressWarnings("unchecked")
        private static <K, V> Partition<K, V> requireSameSpec(Partition<?, ?> registered, PartitionSpec<K, V> spec) {
            if (!registered.spec().equals(spec)) {
                throw new IllegalStateException(
                        "Partition %s is registered with another spec".formatted(registered.name()));
            }
            return (Partition<K, V>) registered;
        }

        /** Weighs an entry with the weigher of its partition. A {@link FailedLoad} weighs zero. */
        int weigh(PoolKey key, Object value) {
            if (value instanceof FailedLoad) {
                return 0;
            }
            return partitionOf(key).weigh(key, value);
        }

        Duration expireAfterAccess(PoolKey key) {
            return partitionOf(key).spec().expireAfterAccess();
        }

        /**
         * Counts the removal of an entry in its partition. The removal of a {@link FailedLoad} is ignored: its
         * partition never counted it as an entry.
         */
        void onRemoval(@Nullable PoolKey key, @Nullable Object value, RemovalCause cause) {
            if (key != null && value != null && !(value instanceof FailedLoad)) {
                partitionOf(key).onRemoval(key, value, cause);
            }
        }

        private Partition<?, ?> partitionOf(PoolKey key) {
            Partition<?, ?> partition = byId.get(key.partitionId());
            if (partition == null) {
                throw new IllegalStateException("No partition with id " + key.partitionId());
            }
            return partition;
        }

        Map<String, CacheStats> stats() {
            Map<String, CacheStats> stats = new HashMap<>();
            byName.forEach((name, partition) -> stats.put(name, partition.stats()));
            return Map.copyOf(stats);
        }

        Collection<String> names() {
            return List.copyOf(byName.keySet());
        }
    }
}
