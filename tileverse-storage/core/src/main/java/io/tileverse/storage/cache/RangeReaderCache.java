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
package io.tileverse.storage.cache;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.CaffeineCache;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RangeReaderCache {

    static final String SHARED_CACHE_NAME = "tileverse-storage-cache";

    private static final int DEFAULT_MAX_HEAP_PERCENT = 20;

    private static final Duration DEFAULT_EXPIRE_AFTER_ACCESS = Duration.ofSeconds(60);

    private int maxHeapPercent = DEFAULT_MAX_HEAP_PERCENT;
    private static final Duration expireAfterAccess = DEFAULT_EXPIRE_AFTER_ACCESS;

    private RangeReader delegate;
    private Cache<ByteRange, ByteBuffer> cache;

    public RangeReaderCache(CacheManager cacheManager, RangeReader delegate) {
        this.delegate = delegate;
        String sourceIdentifier = delegate.getSourceIdentifier();
        this.cache =
                cacheManager.getCache(SHARED_CACHE_NAME, this::buildSharedCache).subCache(sourceIdentifier);
    }

    private Cache<ByteRange, ByteBuffer> buildSharedCache() {
        return CaffeineCache.<ByteRange, ByteBuffer>newBuilder()
                .maxHeapPercent(maxHeapPercent, RangeReaderCache::weigh)
                .averageWeight(64 * 1024)
                .expireAfterAccess(expireAfterAccess)
                // run eviction tasks when idle to return memory without waiting for cache usage
                .scheduler(Scheduler.systemScheduler())
                .removalListener(this::onRemoval)
                .recordStats()
                .build();
    }

    public ByteBuffer get(ByteRange range) {
        return cache.get(range, this::loadRange);
    }

    public ByteBuffer getIfPresent(ByteRange range) {
        return cache.getIfPresent(range);
    }

    /**
     * Stores {@code value} for {@code key} unless another thread already stored one, returning whichever value the
     * cache holds afterwards.
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

    ByteBuffer loadRange(ByteRange key) {
        ByteBuffer blockData = ByteBuffer.allocate(key.length());
        int bytesRead = delegate.readRange(key, blockData);
        return sanitize(blockData, bytesRead);
    }

    /**
     * Trims a freshly filled buffer down to the bytes actually read, flips it for reading, and returns it read-only.
     * Relies on {@code filled}'s position being at {@code bytesRead}, exactly as left by a preceding read that advanced
     * the position by that count.
     *
     * @param filled the buffer written to by the read, positioned at {@code bytesRead}
     * @param bytesRead the number of bytes actually read, at most {@code filled.capacity()}
     * @return a read-only, right-sized, flipped buffer holding exactly the bytes read
     */
    static ByteBuffer sanitize(ByteBuffer filled, int bytesRead) {
        if (bytesRead < filled.capacity() && bytesRead > 0) {
            ByteBuffer actualData = ByteBuffer.allocate(bytesRead);
            filled.flip();
            actualData.put(filled);
            actualData.flip();
            return actualData.asReadOnlyBuffer();
        }
        filled.flip();
        return filled.asReadOnlyBuffer();
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
