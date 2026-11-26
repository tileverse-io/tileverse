/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

/**
 * A {@link CacheManager} over Caffeine caches.
 *
 * <p>One map holds the caches by name: the private caches built through {@link #getCache(String, Supplier)}, the pools
 * under their reserved names, and the partitions registered through {@link #partition(CachePool, String,
 * PartitionSpec)} under their own names. A partition created directly on a pool is not in the map. A name listed by
 * {@link #getCacheNames()} resolves to its cache through {@code getCache}, and two caches never share a name.
 */
public class CaffeineCacheManager implements CacheManager {

    private final ConcurrentMap<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

    private final Supplier<CacheBudgets> budgets;

    private final Ticker ticker;

    private final Executor executor;

    /**
     * Creates a manager with the budgets of the settings of the process. The settings are read when a pool is first
     * created: an invalid setting fails that call, and not the creation of the default manager.
     */
    public CaffeineCacheManager() {
        this(CacheBudgets::fromSettings, Ticker.systemTicker(), CaffeineCache.maintenanceExecutor());
    }

    /** Creates a manager with the given budgets. */
    public CaffeineCacheManager(CacheBudgets budgets) {
        this(fixed(budgets), Ticker.systemTicker(), CaffeineCache.maintenanceExecutor());
    }

    /**
     * Creates a manager with the given budgets and clock, for tests and embedders with their own clock. The pools run
     * their maintenance on the calling threads. The system scheduler of Caffeine still triggers the cleanup of expired
     * entries, and that cleanup runs on the thread of the scheduler.
     */
    public CaffeineCacheManager(CacheBudgets budgets, Ticker ticker) {
        this(fixed(budgets), ticker, Runnable::run);
    }

    /**
     * Creates a manager.
     *
     * @param budgets read each time a pool is created
     * @param ticker the clock of the pools
     * @param executor runs the maintenance of the pools
     */
    CaffeineCacheManager(Supplier<CacheBudgets> budgets, Ticker ticker, Executor executor) {
        this.budgets = requireNonNull(budgets, "budgets");
        this.ticker = requireNonNull(ticker, "ticker");
        this.executor = requireNonNull(executor, "executor");
    }

    private static Supplier<CacheBudgets> fixed(CacheBudgets budgets) {
        requireNonNull(budgets, "budgets");
        return () -> budgets;
    }

    /**
     * Returns the cache registered under {@code cacheIdentifier}, registering the cache built by {@code builder} the
     * first time.
     *
     * <p>The reserved name of a {@link CachePool} returns the pool of this manager, built with the budgets of this
     * manager. The name of a partition registered through {@link #partition(CachePool, String, PartitionSpec)} returns
     * that partition: a {@link Cache} and not a {@link CaffeineCache}. A caller expecting a {@code CaffeineCache} under
     * that name gets a {@link ClassCastException}. The builder is not called for these two names.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V, C extends Cache<K, V>> C getCache(String cacheIdentifier, Supplier<C> builder) {
        Optional<CachePool> reserved = reservedPool(cacheIdentifier);
        if (reserved.isPresent()) {
            return (C) pool(reserved.get());
        }
        return (C) caches.computeIfAbsent(cacheIdentifier, name -> builder.get());
    }

    /**
     * Returns the partition {@code name} of {@code pool}, registering it with {@code spec} the first time. The pool has
     * the budget, the clock and the executor of this manager. The partition is a cache of this manager under its name.
     *
     * <p>Looking up a known name takes no lock: consumers register their partitions each time they use them.
     *
     * @throws IllegalStateException if {@code name} is registered with another spec, or if it is the name of a private
     *     cache, of a pool or of a partition of the other pool
     * @throws IllegalArgumentException if a setting of the budgets holds an invalid size
     */
    @Override
    public <K, V> Cache<K, V> partition(CachePool pool, String name, PartitionSpec<K, V> spec) {
        requireNonNull(pool, "pool");
        requireNonNull(name, "name");
        requireNonNull(spec, "spec");
        requireNotReserved(name);
        Cache<?, ?> registered = caches.get(name);
        if (registered == null) {
            registered = register(pool, name, spec);
        }
        PoolCache poolCache = poolOfPartition(pool, name, registered);
        return poolCache.partition(name, spec);
    }

    /** Registers a new partition in its pool, then under its name in this manager. */
    private <K, V> Cache<?, ?> register(CachePool pool, String name, PartitionSpec<K, V> spec) {
        // Obtained before computeIfAbsent: ConcurrentHashMap forbids a computeIfAbsent nested in another of the map
        PoolCache poolCache = pool(pool);
        return caches.computeIfAbsent(name, partitionName -> poolCache.partition(partitionName, spec));
    }

    private static void requireNotReserved(String name) {
        if (reservedPool(name).isPresent()) {
            throw new IllegalStateException("The name %s is reserved for a pool".formatted(name));
        }
    }

    /** Returns the pool of this manager for {@code pool}, creating it the first time. */
    private PoolCache pool(CachePool pool) {
        // A reserved name maps to its pool only: getCache routes it here, and partition refuses it
        return (PoolCache) caches.computeIfAbsent(pool.cacheName(), name -> newPool(pool));
    }

    /** Creates a pool. A failed read of the budgets leaves no pool in the map, and the next call reads them again. */
    private PoolCache newPool(CachePool pool) {
        CacheBudgets poolBudgets = budgets.get();
        return PoolCache.create(pool, poolBudgets.of(pool), ticker, executor);
    }

    /**
     * Returns the pool of the partition {@code registered}.
     *
     * @throws IllegalStateException if {@code registered} is not a partition of {@code pool}
     */
    private static PoolCache poolOfPartition(CachePool pool, String name, Cache<?, ?> registered) {
        if (!(registered instanceof Partition<?, ?> partition)) {
            throw new IllegalStateException(privateCacheUnderPartitionName(pool, name));
        }
        PoolCache poolCache = partition.pool();
        if (poolCache.pool() != pool) {
            String otherPool = poolCache.pool().name();
            throw new IllegalStateException(
                    "The name %s is taken by a partition of the %s pool".formatted(name, otherPool));
        }
        return poolCache;
    }

    private static String privateCacheUnderPartitionName(CachePool pool, String name) {
        return ("The name %s belongs to a partition of the %s pool, but a cache is registered under it. "
                        + "The pools are sized with the io.tileverse.cache.* settings instead of a cache registered "
                        + "under that name.")
                .formatted(name, pool.name());
    }

    private static Optional<CachePool> reservedPool(String name) {
        for (CachePool pool : CachePool.values()) {
            if (pool.cacheName().equals(name)) {
                return Optional.of(pool);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the Caffeine cache behind the cache registered under {@code name}, or {@code null} if the name is unknown
     * or maps to a partition. A partition has no Caffeine cache of its own: its figures are in {@link #stats()} under
     * its name, and in {@link PoolCache#partitionStats()}.
     */
    public com.github.benmanes.caffeine.cache.@Nullable Cache<Object, Object> getNativeCache(String name) {
        Cache<?, ?> cache = caches.get(name);
        if (cache instanceof CaffeineCache<?, ?> caffeineCache) {
            return caffeineCache.getNativeCache();
        }
        return null;
    }

    /**
     * Returns the names of the private caches, of the pools and of the partitions registered through
     * {@link #partition(CachePool, String, PartitionSpec)}.
     */
    @Override
    public Collection<String> getCacheNames() {
        return caches.keySet();
    }

    /**
     * Clears the private caches and the pools. Partitions are skipped: clearing a pool clears its partitions, and
     * clearing a partition alone scans the keys of the whole pool.
     */
    @Override
    public void invalidateAll() {
        for (Cache<?, ?> cache : caches.values()) {
            if (!(cache instanceof Partition)) {
                cache.invalidateAll();
            }
        }
    }

    /**
     * Returns the statistics of the private caches, of the pools and of the partitions registered through
     * {@link #partition(CachePool, String, PartitionSpec)}, by name.
     */
    @Override
    public Map<String, CacheStats> stats() {
        return caches.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stats()));
    }
}
