/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;

public interface CacheManager {

    @NonNull
    static CacheManager getDefault() {
        return DefaultCacheManagerHolder.get();
    }

    static void setDefault(@NonNull CacheManager newDefault) {
        DefaultCacheManagerHolder.set(newDefault);
    }

    static CacheManager newInstance() {
        return new CaffeineCacheManager();
    }

    /**
     * Get the cache associated with the given name.
     *
     * <p>Note that the cache may be lazily created at runtime if the native provider supports it.
     *
     * <p>A {@link CaffeineCacheManager} reserves the names of its two pools and the names given to
     * {@link #partition(CachePool, String, PartitionSpec)}.
     *
     * @param cacheIdentifier the cache identifier (must not be {@code null})
     * @return the associated cache, or {@code null} if such a cache does not exist or could be not created
     */
    @NonNull
    <K, V, C extends Cache<K, V>> C getCache(@NonNull String cacheIdentifier, @NonNull Supplier<C> builder);

    /**
     * Get a collection of the cache names known by this manager.
     *
     * @return the names of all caches known by the cache manager
     */
    Collection<String> getCacheNames();

    Map<String, CacheStats> stats();

    void invalidateAll();

    /**
     * Returns the partition {@code name} of the shared pool {@code pool}, registering it with {@code spec} the first
     * time.
     *
     * <p>The pool is a cache of this manager under its reserved name, {@link CachePool#cacheName()}. The
     * {@link #invalidateAll()}, {@link #stats()} and {@link #getCacheNames()} of any implementation reach it, and
     * {@link #getCache(String, Supplier)} with a reserved name returns the pool. The default implementation reads the
     * budget of the pool from the settings when the pool is first created. The {@code getCache} of the manager must
     * return the instance built by the supplier, or the instance registered under that name before.
     *
     * <p>The entry count in the statistics of a partition can lag behind removals until the next maintenance of the
     * pool.
     *
     * @throws IllegalStateException if {@code name} is registered with another spec
     * @throws IllegalArgumentException if a setting of the budgets holds an invalid size
     */
    @NonNull
    default <K, V> Cache<K, V> partition(
            @NonNull CachePool pool, @NonNull String name, @NonNull PartitionSpec<K, V> spec) {
        PoolCache poolCache = getCache(pool.cacheName(), () -> newPoolFromSettings(pool));
        return poolCache.partition(name, spec);
    }

    private static PoolCache newPoolFromSettings(CachePool pool) {
        CacheBudgets budgets = CacheBudgets.fromSettings();
        return PoolCache.create(pool, budgets.of(pool));
    }
}
