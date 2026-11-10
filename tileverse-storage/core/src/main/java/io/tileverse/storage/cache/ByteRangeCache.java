/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CachePool;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.PartitionSpec;
import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The byte ranges cached for one content identifier, in a partition of the {@link CachePool#DATA} pool. Caches built
 * from one {@link CacheManager} with equal content identifiers share their ranges.
 *
 * <p>The partition also counts the reads that were too large to cache, for the caches built from its manager, whatever
 * their content identifier.
 */
class ByteRangeCache {

    static final String SHARED_CACHE_NAME = "tileverse-storage-cache";

    static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(5);

    /**
     * A constant: registering the partition again requires an equal spec, and the pool compares weighers by identity.
     */
    private static final PartitionSpec<ByteRange, ByteBuffer> SPEC =
            new PartitionSpec<>(ByteRangeCache::weigh, EXPIRE_AFTER_ACCESS);

    /**
     * The bypass counters of each partition. A manager returns the same partition object each time the partition is
     * registered, hence the partition identifies its manager. The weak keys are compared by identity, and the counters
     * of a discarded manager are collected with its partition.
     */
    private static final LoadingCache<Cache<ByteRange, ByteBuffer>, BypassCounters> BYPASS_COUNTERS =
            Caffeine.newBuilder().weakKeys().build(partition -> new BypassCounters());

    private final Cache<ByteRange, ByteBuffer> cache;
    private final BypassCounters bypassCounters;

    public ByteRangeCache(CacheManager cacheManager, String contentIdentifier) {
        Cache<ByteRange, ByteBuffer> partition = cacheManager.partition(CachePool.DATA, SHARED_CACHE_NAME, SPEC);
        this.cache = partition.subCache(contentIdentifier);
        this.bypassCounters = BYPASS_COUNTERS.get(partition);
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

    /** Counts one read that was too large to cache, and its bytes. */
    public void recordBypass(long bytes) {
        bypassCounters.countCall(bytes);
    }

    /** Returns the reads that were too large to cache, and their bytes, for the caches sharing the partition. */
    public BypassStats bypassStats() {
        return bypassCounters.snapshot();
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

    /** Counts the reads of one partition that were too large to cache, and their bytes. */
    private static final class BypassCounters {
        private final LongAdder calls = new LongAdder();
        private final LongAdder bytes = new LongAdder();

        void countCall(long bytesRead) {
            calls.increment();
            bytes.add(bytesRead);
        }

        BypassStats snapshot() {
            return new BypassStats(calls.sum(), bytes.sum());
        }
    }
}
