/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static io.tileverse.cache.CachePool.DATA;
import static io.tileverse.cache.CachePool.METADATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.benmanes.caffeine.cache.Policy;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

class CaffeineCacheManagerTest {

    private static final String DATA_POOL = "tileverse-cache-data";

    private static final String METADATA_POOL = "tileverse-cache-metadata";

    private static final CacheBudgets BUDGETS = new CacheBudgets(1_000, 1_000);

    private final AtomicLong nanos = new AtomicLong();

    private final PartitionSpec<String, String> spec =
            new PartitionSpec<>((key, value) -> value.length(), Duration.ofMinutes(1));

    private final CaffeineCacheManager manager = new CaffeineCacheManager(BUDGETS, nanos::get);

    @Test
    void onePoolPerCachePool() {
        manager.partition(DATA, "a", spec);
        Cache<Object, Object> dataPool = manager.getCache(DATA_POOL, () -> null);
        manager.partition(DATA, "b", spec);
        Collection<String> namesWithDataPool = List.copyOf(manager.getCacheNames());
        manager.partition(METADATA, "c", spec);

        Cache<Object, Object> dataPoolAgain = manager.getCache(DATA_POOL, () -> null);
        assertThat(namesWithDataPool).containsOnlyOnce(DATA_POOL).doesNotContain(METADATA_POOL);
        assertThat(manager.getCacheNames()).contains(METADATA_POOL);
        assertThat(dataPool).isInstanceOf(PoolCache.class).isSameAs(dataPoolAgain);
    }

    @Test
    void budgetsSizeThePools() {
        CaffeineCacheManager sized = new CaffeineCacheManager(new CacheBudgets(100, 50), nanos::get);

        sized.partition(DATA, "a", spec);
        sized.partition(METADATA, "b", spec);

        assertThat(maximumWeight(sized, DATA_POOL)).isEqualTo(100);
        assertThat(maximumWeight(sized, METADATA_POOL)).isEqualTo(50);
    }

    // A failed read of the budgets leaves no pool: the next call reads them again
    @Test
    void budgetsReadAtFirstUse() {
        AtomicInteger reads = new AtomicInteger();
        Supplier<CacheBudgets> failingOnce = () -> {
            if (reads.incrementAndGet() == 1) {
                throw new IllegalArgumentException("invalid size");
            }
            return BUDGETS;
        };
        CaffeineCacheManager lazy = new CaffeineCacheManager(failingOnce, nanos::get, Runnable::run);
        int readsAtCreation = reads.get();

        assertThatThrownBy(() -> lazy.partition(DATA, "a", spec)).isInstanceOf(IllegalArgumentException.class);
        Cache<String, String> a = lazy.partition(DATA, "a", spec);

        assertThat(readsAtCreation).isZero();
        assertThat(reads).hasValue(2);
        assertThat(a.get("k", key -> "v")).isEqualTo("v");
    }

    // A sub-partition has no name of its own in the manager
    @Test
    void statsListPoolsAndPartitions() {
        manager.getCache("private", CaffeineCacheManagerTest::privateCache);
        Cache<String, String> a = manager.partition(DATA, "a", spec);
        manager.partition(DATA, "b", spec);
        a.subCache("x").get("k", key -> "v");

        Map<String, CacheStats> stats = manager.stats();

        assertThat(stats).containsOnlyKeys(DATA_POOL, "a", "b", "private");
        assertThat(manager.getCacheNames()).containsExactlyInAnyOrderElementsOf(stats.keySet());
        assertThat(stats.get("a").entryCount()).isEqualTo(1);
    }

    @Test
    void getCacheResolvesPartitions() {
        Cache<String, String> a = manager.partition(DATA, "a", spec);
        Cache<String, String> b = manager.partition(DATA, "b", spec);
        a.get("in a", key -> "v");
        b.get("in b", key -> "v");

        Cache<String, String> found = manager.getCache("a", () -> {
            throw new AssertionError("the builder of a partition name ran");
        });

        assertThat(manager.getCacheNames()).contains("a");
        assertThat(found).isSameAs(a);
        assertThat(found.keys()).containsExactly("in a");
    }

    @Test
    void nativeCacheOfPartitionIsNull() {
        manager.partition(DATA, "a", spec);

        assertThat(manager.getNativeCache("a")).isNull();
        assertThat(manager.getNativeCache(DATA_POOL)).isNotNull();
    }

    @Test
    void partitionNameClashFails() {
        manager.getCache("p", CaffeineCacheManagerTest::privateCache);
        manager.partition(DATA, "a", spec);

        assertThatThrownBy(() -> manager.partition(DATA, "p", spec))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("partition of the DATA pool")
                .hasMessageContaining("io.tileverse.cache.* settings");
        assertThatThrownBy(() -> manager.partition(DATA, DATA_POOL, spec))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reserved for a pool");
        assertThatThrownBy(() -> manager.partition(DATA, METADATA_POOL, spec))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> manager.partition(METADATA, "a", spec)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void invalidateAllClearsPools() {
        Cache<String, String> a = manager.partition(DATA, "a", spec);
        Cache<String, String> c = manager.partition(METADATA, "c", spec);
        Cache<String, String> privateCache = manager.getCache("private", CaffeineCacheManagerTest::privateCache);
        a.get("k", key -> "v");
        c.get("k", key -> "v");
        privateCache.get("k", key -> "v");

        manager.invalidateAll();

        assertThat(a.getIfPresent("k")).isNull();
        assertThat(c.getIfPresent("k")).isNull();
        assertThat(privateCache.getIfPresent("k")).isNull();
    }

    @Test
    void zeroBudgetKeepsNothing() {
        CaffeineCacheManager empty = new CaffeineCacheManager(new CacheBudgets(0, 0), nanos::get);
        Cache<String, String> a = empty.partition(DATA, "a", spec);

        String loaded = a.get("k", key -> "v");

        assertThat(loaded).isEqualTo("v");
        assertThat(a.getIfPresent("k")).isNull();
        assertThat(a.stats().entryCount()).isZero();
    }

    @Test
    void foreignManagerGetsPoolsThroughGetCache() {
        MapCacheManager foreign = new MapCacheManager();
        Cache<String, String> a = foreign.partition(DATA, "a", spec);

        a.get("k", key -> "v");

        assertThat(foreign.getCacheNames()).containsExactly(DATA_POOL);
        assertThat(foreign.nativeCache(DATA_POOL).estimatedSize()).isEqualTo(1);
        assertThat(foreign.stats().get(DATA_POOL).entryCount()).isEqualTo(1);
        foreign.invalidateAll();
        assertThat(a.getIfPresent("k")).isNull();
    }

    private static long maximumWeight(CaffeineCacheManager manager, String poolName) {
        Policy<Object, Object> policy = manager.getNativeCache(poolName).policy();
        Policy.Eviction<Object, Object> eviction = policy.eviction().orElseThrow();
        return eviction.getMaximum();
    }

    private static CaffeineCache<String, String> privateCache() {
        return CaffeineCache.<String, String>newBuilder().recordStats().build();
    }

    @Nested
    @Isolated("swaps the default manager of the process")
    class DefaultManager {

        @Test
        void setDefaultInvalidatesOldPools() {
            CacheManager previous = CacheManager.getDefault();
            CacheManager old = new CaffeineCacheManager(BUDGETS, nanos::get);
            Cache<String, String> a = old.partition(DATA, "a", spec);
            CacheManager.setDefault(old);
            try {
                a.get("k", key -> "v");
            } finally {
                CacheManager.setDefault(previous);
            }

            assertThat(a.getIfPresent("k")).isNull();
        }
    }

    /** A manager that keeps its own map of caches and reads their native Caffeine caches. */
    private static final class MapCacheManager implements CacheManager {

        private final Map<String, Cache<?, ?>> caches = new ConcurrentHashMap<>();

        private final Map<String, com.github.benmanes.caffeine.cache.Cache<Object, Object>> natives =
                new ConcurrentHashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <K, V, C extends Cache<K, V>> C getCache(String cacheIdentifier, Supplier<C> builder) {
            return (C) caches.computeIfAbsent(cacheIdentifier, name -> {
                Cache<?, ?> built = builder.get();
                CaffeineCache<?, ?> caffeine = (CaffeineCache<?, ?>) built;
                natives.put(name, caffeine.getNativeCache());
                return built;
            });
        }

        com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache(String name) {
            return natives.get(name);
        }

        @Override
        public Collection<String> getCacheNames() {
            return List.copyOf(caches.keySet());
        }

        @Override
        public Map<String, CacheStats> stats() {
            Map<String, CacheStats> stats = new HashMap<>();
            caches.forEach((name, cache) -> stats.put(name, cache.stats()));
            return stats;
        }

        @Override
        public void invalidateAll() {
            caches.values().forEach(Cache::invalidateAll);
        }
    }
}
