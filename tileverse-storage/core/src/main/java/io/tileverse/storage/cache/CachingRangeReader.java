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

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A decorator for {@link RangeReader} that caches exact byte ranges in memory using a shared, Caffeine-backed cache.
 *
 * <p>Each cache key is the exact {@code (offset, length)} pair requested: a range is never split, merged, or padded to
 * a block boundary on its way into the cache. Block-grain caching is obtained by stacking a
 * {@link io.tileverse.storage.block.BlockAlignedRangeReader} above this reader: the aligner expands a request into
 * block-sized ranges before they reach this cache, which then stores those blocks under their own exact keys.
 *
 * <p>The underlying cache is shared across every {@code CachingRangeReader} built from the same {@link CacheManager},
 * partitioned by the delegate's {@link RangeReader#getSourceIdentifier() source identifier} so that ranges from
 * different sources never collide.
 *
 * <p><strong>Batch reads:</strong> {@link #readRanges(List)} serves cache hits directly, forwards the distinct misses
 * to the delegate as a single {@code readRanges} call, and stores each miss before copying its bytes out to the caller.
 * Two requests for the same range within one batch collapse into a single delegate fetch and cache store.
 */
public class CachingRangeReader extends AbstractRangeReader implements RangeReader {

    private final RangeReader delegate;
    private final RangeReaderCache cache;

    CachingRangeReader(RangeReader delegate, RangeReaderCache cache) {
        this.delegate = requireNonNull(delegate, "Delegate RangeReader cannot be null");
        this.cache = requireNonNull(cache, "Cache cannot be null");
    }

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
        ByteRange key = new ByteRange(offset, actualLength);
        ByteBuffer cachedBuffer = cache.get(key);
        if (cachedBuffer == null) {
            return 0;
        }
        ByteBuffer duplicate = cachedBuffer.duplicate();
        int bytesRead = duplicate.remaining();
        target.put(duplicate);
        return bytesRead;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Hits are served from the cache; the distinct misses are forwarded down as one {@code readRanges} call on the
     * delegate and stored before the results are copied out. Amplification: none, every forwarded range is exactly a
     * requested range. Concurrency note: unlike single reads, the batch path has no cross-thread single-flight; two
     * threads missing the same range concurrently both fetch it once and one store wins.
     */
    @Override
    public int[] readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        int[] read = new int[requests.size()];

        Map<ByteRange, ByteBuffer> misses = new LinkedHashMap<>();
        List<Integer> pending = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            ByteRange key = request.range();
            if (key.length() == 0) {
                continue;
            }
            ByteBuffer cached = cache.getIfPresent(key);
            if (cached != null) {
                read[i] = copyTo(cached, request.target());
            } else {
                misses.computeIfAbsent(key, k -> ByteBuffer.allocate(k.length()));
                pending.add(i);
            }
        }
        if (misses.isEmpty()) {
            return read;
        }

        Map<ByteRange, ByteBuffer> loaded = fetchAndStore(misses);
        for (int i : pending) {
            RangeRequest request = requests.get(i);
            read[i] = copyTo(loaded.get(request.range()), request.target());
        }
        return read;
    }

    /**
     * Fetches the given misses from the delegate as one batch and stores each sanitized value, returning the value the
     * cache holds per range (another thread's store wins over ours).
     */
    private Map<ByteRange, ByteBuffer> fetchAndStore(Map<ByteRange, ByteBuffer> misses) {
        List<RangeRequest> missRequests = new ArrayList<>(misses.size());
        misses.forEach((key, buffer) -> missRequests.add(new RangeRequest(key, buffer)));
        int[] missRead = delegate.readRanges(missRequests);

        Map<ByteRange, ByteBuffer> loaded = new HashMap<>();
        for (int j = 0; j < missRequests.size(); j++) {
            RangeRequest missRequest = missRequests.get(j);
            ByteBuffer value = RangeReaderCache.sanitize(missRequest.target(), missRead[j]);
            loaded.put(missRequest.range(), cache.getOrStore(missRequest.range(), value));
        }
        return loaded;
    }

    private static int copyTo(ByteBuffer cached, ByteBuffer target) {
        ByteBuffer duplicate = cached.duplicate();
        int bytesRead = duplicate.remaining();
        target.put(duplicate);
        return bytesRead;
    }

    @Override
    public OptionalLong size() {
        return delegate.size();
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        cache.invalidateAll();
    }

    /** Clears the cache, forcing subsequent reads to go to the underlying source. */
    public void clearCache() {
        cache.invalidateAll();
    }

    /**
     * Gets the current number of entries in the cache.
     *
     * @return The number of cached entries
     */
    long getCacheEntryCount() {
        return cache.stats().entryCount();
    }

    /**
     * Gets the estimated cache size in bytes.
     *
     * @return The estimated cache size in bytes
     */
    long getEstimatedCacheSizeBytes() {
        return cache.getEstimatedCacheSizeBytes();
    }

    /**
     * Gets the cache statistics.
     *
     * @return The cache statistics
     */
    public CacheStats getCacheStats() {
        return cache.stats();
    }

    /**
     * Creates a caching reader over {@code delegate} with the default cache manager, equivalent to
     * {@code builder(delegate).build()}.
     *
     * @param delegate the reader whose ranges are cached
     * @return a new CachingRangeReader instance
     */
    public static CachingRangeReader of(RangeReader delegate) {
        return builder(delegate).build();
    }

    /**
     * Creates a new builder for CachingRangeReader with the mandatory delegate parameter.
     *
     * @param delegate the delegate RangeReader to wrap with caching
     * @return a new builder instance with the delegate set
     */
    public static Builder builder(RangeReader delegate) {
        return new Builder(delegate);
    }

    /** Builder for CachingRangeReader. */
    public static class Builder {
        private final RangeReader delegate;
        private CacheManager cacheManager = CacheManager.getDefault();

        private Builder(RangeReader delegate) {
            this.delegate = requireNonNull(delegate, "Delegate cannot be null");
        }

        public Builder cacheManager(CacheManager cacheManager) {
            this.cacheManager = requireNonNull(cacheManager);
            return this;
        }

        /**
         * Builds the CachingRangeReader.
         *
         * @return a new CachingRangeReader instance
         */
        public CachingRangeReader build() {
            RangeReaderCache cache = new RangeReaderCache(cacheManager, delegate);
            return new CachingRangeReader(delegate, cache);
        }
    }
}
