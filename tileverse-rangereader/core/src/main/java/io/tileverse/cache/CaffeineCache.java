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
package io.tileverse.cache;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
public class CaffeineCache<K, V> implements Cache<K, V> {

    private static final ThreadFactory maintenanceThreadFactory = new ThreadFactory() {
        private static final AtomicInteger executorThreadId = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("tileverse-cache-maintenance-%d".formatted(executorThreadId.incrementAndGet()));
            return t;
        }
    };

    /**
     * Synchronous {@link com.github.benmanes.caffeine.cache.Cache caches} only use
     * the executor for background maintenance operations (like eviction and removal
     * notifications), while {@link AsyncCache} also uses it for loading values and
     * could hence more easily saturate or even dead-lock if the same executor is
     * used by multiple caches (especially if loading a value from one cache can
     * trigger loading a value from another cache).
     * <p>
     * We're only creating synchronous caches so it's safe for them to share the maintenance executor service.
     */
    private static final ExecutorService MAINTENANCE_EXECUTOR = Executors.newFixedThreadPool(
            Math.max(1, (Runtime.getRuntime().availableProcessors() / 2)), maintenanceThreadFactory);

    protected final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    protected final ConcurrentMap<String, Cache<K, V>> subCaches = new ConcurrentHashMap<>();

    public CaffeineCache(com.github.benmanes.caffeine.cache.Cache<K, V> cache) {
        this.cache = requireNonNull(cache);
    }

    @SuppressWarnings("unchecked")
    public com.github.benmanes.caffeine.cache.Cache<Object, Object> getNativeCache() {
        return (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cache;
    }

    @Override
    public Cache<K, V> subCache(String subCacheId) {
        return subCaches.computeIfAbsent(requireNonNull(subCacheId), id -> SubCache.of(id, CaffeineCache.this));
    }

    @Override
    public void forEach(BiConsumer<K, V> consumer) {
        this.cache.asMap().forEach(consumer);
    }

    @Override
    @Nullable
    public V getIfPresent(K key) {
        return cache.getIfPresent(key);
    }

    @Override
    @Nullable
    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        return cache.get(key, mappingFunction);
    }

    @Override
    public CacheStats stats() {
        return CaffeineCache.stats(cache);
    }

    public static CacheStats stats(com.github.benmanes.caffeine.cache.Cache<?, ?> c) {
        return CacheStats.fromCaffeine(c.stats(), c.estimatedSize());
    }

    @Override
    public Cache<K, V> invalidateAll() {
        cache.invalidateAll();
        return this;
    }

    @Override
    public Collection<K> keys() {
        return cache.asMap().keySet();
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        cache.invalidateAll(keys);
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {

        Caffeine<Object, Object> caffeine = Caffeine.newBuilder();

        private ExecutorService customExecutor;

        private Long maximumWeightBytes;

        private Integer averageWeightBytes;

        public CaffeineCache<K, V> build() {
            ExecutorService executor = customExecutor == null ? MAINTENANCE_EXECUTOR : customExecutor;
            caffeine.executor(executor);
            if (maximumWeightBytes != null && averageWeightBytes != null) {
                int initialCapacity =
                        computeInitialCapacity(maximumWeightBytes.longValue(), averageWeightBytes.intValue());
                //                System.out.printf(
                //                        "maximumWeight: %,d, averageWeight: %,d, initialCapacity: %,d%n",
                //                        maximumWeightBytes, averageWeightBytes, initialCapacity);
                caffeine.initialCapacity(initialCapacity);
            }
            com.github.benmanes.caffeine.cache.Cache<K, V> cache = caffeine.build();
            return new CaffeineCache<>(cache);
        }

        /**
         * Estimates the initial capacity to minimize internal hash table resizing.
         * <p>
         * While {@code initialCapacity} is a performance hint, setting it too low (default is 16)
         * causes frequent resizing and object allocation churn (garbage). Conversely,
         * over-estimating can lead to excessive memory pre-allocation.
         * <p>
         * This implementation uses a conservative 80% buffer of the theoretical max items
         * to ensure that the internal data structures do not outpace the actual heap
         * availability if the {@code averageWeight} estimate is slightly inaccurate.
         *
         * @param maxWeight the maximum allowed weight (e.g., total bytes) for the cache
         * @param avgWeight the estimated average weight of a single entry
         * @return a conservative initial capacity estimate, clamped to valid Integer ranges
         * @throws IllegalArgumentException if avgWeight is less than or equal to 0
         */
        private int computeInitialCapacity(long maxWeight, int avgWeight) {
            if (avgWeight <= 0) {
                throw new IllegalArgumentException("averageWeight must be greater than 0 to calculate capacity");
            }

            // Calculate theoretical maximum items that could fit
            long maxItems = maxWeight / avgWeight;

            // Apply a conservative 80% buffer. This balances the reduction of GC
            // overhead from resizing against the risk of over-allocating memory.
            long capacity = (long) (maxItems * 0.80);

            // Clamp the value to Caffeine's internal limits (approx. 1 billion)
            // and prevent Integer overflow.
            if (capacity > 1_000_000_000L) {
                return 1_000_000_000;
            }

            return Math.max(0, (int) capacity);
        }

        public Builder<K, V> maxHeapPercent(int maxHeapPercent, Weigher<K, V> weigher) {
            final long maxMemory = Runtime.getRuntime().maxMemory();
            final long maxHeapUsage = (long) (maxMemory * (maxHeapPercent / 100d));
            return maximumWeight(maxHeapUsage).weigher(weigher);
        }

        public Builder<K, V> maximumWeight(long maximumWeight) {
            this.maximumWeightBytes = maximumWeight;
            caffeine = caffeine.maximumWeight(maximumWeight);
            return this;
        }

        public Builder<K, V> averageWeight(int averageWeight) {
            this.averageWeightBytes = averageWeight;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<K, V> weigher(final Weigher<K, V> weigher) {
            caffeine = (Caffeine<Object, Object>) caffeine.weigher(SubCache.weigher(weigher));
            return this;
        }

        public Builder<K, V> expireAfterAccess(Duration duration) {
            caffeine = caffeine.expireAfterAccess(duration);
            return this;
        }

        public Builder<K, V> executor(ExecutorService executor) {
            this.customExecutor = executor;
            return this;
        }

        public Builder<K, V> scheduler(Scheduler scheduler) {
            caffeine = caffeine.scheduler(scheduler);
            return this;
        }

        public Builder<K, V> removalListener(RemovalListener<K, V> removalListener) {
            caffeine.removalListener(SubCache.removalListener(removalListener));
            return this;
        }

        public Builder<K, V> recordStats() {
            caffeine = caffeine.recordStats();
            return this;
        }
    }
}
