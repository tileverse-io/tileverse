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
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

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
 * <p><strong>Concurrent reads:</strong> the reads going through one {@code CachingRangeReader} fetch a range once. A
 * read needing a range being fetched by another read, single or batch, waits for that fetch and takes the cached value.
 * A wait lasts 10 seconds at most; past it, the read fetches the range itself. When the awaited fetch fails, the read
 * tries the range once more before failing, and can wait a second time for a fetch started by another read in the
 * meantime: 20 seconds of waiting at most, besides the time of its own fetches. Readers built separately over the same
 * source share the cached ranges, not the fetches in progress.
 *
 * <p><strong>Batch reads:</strong> {@link #readRanges(List)} serves cache hits directly and forwards the distinct
 * ranges neither cached nor being fetched to the delegate as a single {@code readRanges} call. Two requests for the
 * same range within one batch collapse into a single delegate fetch and cache store.
 */
public class CachingRangeReader extends AbstractRangeReader implements RangeReader {

    private final RangeReader delegate;
    private final ByteRangeCache cache;
    private final BatchRangeLoader loader;

    CachingRangeReader(RangeReader delegate, ByteRangeCache cache, BatchRangeLoader loader) {
        this.delegate = requireNonNull(delegate, "Delegate RangeReader cannot be null");
        this.cache = requireNonNull(cache, "Cache cannot be null");
        this.loader = requireNonNull(loader, "Loader cannot be null");
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

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
        ByteRange range = ByteRange.of(offset, actualLength);
        BatchLoadOutcome loaded = loader.load(List.of(range), this::fetchSingleRange);
        ByteBuffer cached = loaded.cachedValue(range);
        return copyTo(cached, target);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Hits are served from the cache. The distinct ranges neither cached nor being fetched by another read are
     * forwarded down as one {@code readRanges} call on the delegate and stored; the ranges being fetched by another
     * read are awaited and taken from the cache. Amplification: none, the forwarded ranges are requested ranges. The
     * result counts the hits, the ranges taken from another read and the repeats of a range within the batch as bytes
     * from cache; it takes the fetches and bytes transferred from the delegate calls of this read.
     */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        Set<ByteRange> ranges = distinctNonEmptyRanges(requests);
        BatchLoadOutcome loaded = loader.load(ranges, delegate::readRanges);
        return copyOut(loaded, requests);
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
     * Gets the cache statistics.
     *
     * @return The cache statistics
     */
    public CacheStats getCacheStats() {
        return cache.stats();
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
            String sourceIdentifier = delegate.getSourceIdentifier();
            ByteRangeCache cache = new ByteRangeCache(cacheManager, sourceIdentifier);
            return new CachingRangeReader(delegate, cache, new BatchRangeLoader(cache));
        }
    }

    /** A single read stays a single read on the delegate. */
    private BatchReadResult fetchSingleRange(List<RangeRequest> singleRequest) {
        RangeRequest request = singleRequest.get(0);
        int bytesRead = delegate.readRange(request.range(), request.target());
        return BatchReadResult.perRange(singleRequest, new int[] {bytesRead});
    }

    private static Set<ByteRange> distinctNonEmptyRanges(List<RangeRequest> requests) {
        Set<ByteRange> ranges = new LinkedHashSet<>();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            ByteRange range = request.range();
            if (range.length() > 0) {
                ranges.add(range);
            }
        }
        return ranges;
    }

    private static BatchReadResult copyOut(BatchLoadOutcome loaded, List<RangeRequest> requests) {
        int[] bytesRead = new int[requests.size()];
        long bytesFromCache = 0;
        Set<ByteRange> countedAsFetched = new HashSet<>();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            ByteRange range = request.range();
            if (range.length() == 0) {
                continue;
            }
            ByteBuffer cached = loaded.cachedValue(range);
            bytesRead[i] = copyTo(cached, request.target());
            boolean firstRequestOfAFetchedRange = loaded.fetched(range) && countedAsFetched.add(range);
            if (!firstRequestOfAFetchedRange) {
                bytesFromCache += bytesRead[i];
            }
        }
        BatchReadResult ownView =
                BatchReadResult.of(requests, bytesRead, 0 /*fetches*/, 0 /*bytesTransferred*/, bytesFromCache);
        return ownView.merge(loaded.cost());
    }

    private static int copyTo(ByteBuffer cached, ByteBuffer target) {
        int bytesRead = cached.remaining();
        int at = target.position();
        // absolute put, doesn't move the position of either buffer
        target.put(at, cached, cached.position(), bytesRead);
        target.position(at + bytesRead);
        return bytesRead;
    }
}
