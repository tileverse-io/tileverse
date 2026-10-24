/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.CaffeineCache;
import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ByteRangeCache {

    static final String SHARED_CACHE_NAME = "tileverse-storage-cache";

    private static final int DEFAULT_MAX_HEAP_PERCENT = 20;

    private static final Duration DEFAULT_EXPIRE_AFTER_ACCESS = Duration.ofSeconds(60);

    private int maxHeapPercent = DEFAULT_MAX_HEAP_PERCENT;
    private static final Duration expireAfterAccess = DEFAULT_EXPIRE_AFTER_ACCESS;

    private Cache<ByteRange, ByteBuffer> cache;

    public ByteRangeCache(CacheManager cacheManager, String sourceIdentifier) {
        this.cache =
                cacheManager.getCache(SHARED_CACHE_NAME, this::buildSharedCache).subCache(sourceIdentifier);
    }

    private Cache<ByteRange, ByteBuffer> buildSharedCache() {
        return CaffeineCache.<ByteRange, ByteBuffer>newBuilder()
                .maxHeapPercent(maxHeapPercent, ByteRangeCache::weigh)
                .averageWeight(64 * 1024)
                .expireAfterAccess(expireAfterAccess)
                // run eviction tasks when idle to return memory without waiting for cache usage
                .scheduler(Scheduler.systemScheduler())
                .removalListener(this::onRemoval)
                .recordStats()
                .build();
    }

    /**
     * Returns the cached value of {@code range}, or null, recording the lookup: a hit and an access when the cache
     * holds the value, a miss otherwise. It stores nothing.
     */
    public ByteBuffer getIfPresent(ByteRange range) {
        return cache.getIfPresent(range);
    }

    /** Returns the cached value of {@code range}, or null, recording neither a hit nor a miss nor an access. */
    public ByteBuffer getIfPresentQuietly(ByteRange range) {
        return cache.getIfPresentQuietly(range);
    }

    /**
     * Stores {@code value} for {@code key} unless the cache holds one, returning the value held by the cache
     * afterwards. It records a hit when the cache held a value, a miss and a load when it stored this one.
     */
    public ByteBuffer getOrStore(ByteRange key, ByteBuffer value) {
        return cache.get(key, k -> value);
    }

    void onRemoval(ByteRange key, ByteBuffer value, RemovalCause cause) {
        if (log.isTraceEnabled()) {
            log.trace("Evicted {}, {}, cause: {}", value, key, cause);
        }
    }

    static int weigh(ByteRange key, ByteBuffer value) {
        // Approximate overhead of the key record (long + int + object header)
        int keyWeight = 24;

        // Value weight: object header + int field + the actual data in the ByteBuffer
        int valueWeight = 16 + value.capacity();

        return keyWeight + valueWeight;
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public CacheStats stats() {
        return cache.stats();
    }

    public long getEstimatedCacheSizeBytes() {
        final AtomicLong size = new AtomicLong();
        cache.forEach((k, buff) -> {
            if (buff != null) {
                size.addAndGet(buff.capacity());
            }
        });
        return size.get();
    }
}
