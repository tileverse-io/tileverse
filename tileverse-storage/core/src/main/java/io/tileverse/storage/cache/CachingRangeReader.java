/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CachePool;
import io.tileverse.cache.CacheSettings;
import io.tileverse.cache.CacheStats;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
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
 * <p>The cached ranges live in a partition of the {@link CachePool#DATA} pool of the {@link CacheManager}, shared by
 * the {@code CachingRangeReader}s built from that manager. Each reader uses the sub-partition of its delegate's
 * {@link RangeReader#contentIdentifier() content identifier}: readers of different sources, or of different versions of
 * one source, never read each other's ranges. Readers of one source with an unknown version share their ranges.
 *
 * <p><strong>Concurrent reads:</strong> the reads going through one {@code CachingRangeReader} fetch a range once. A
 * read needing a range being fetched by another read, single or batch, waits for that fetch and takes the cached value.
 * A wait lasts 10 seconds at most; past it, the read fetches the range itself. When the awaited fetch fails, the read
 * tries the range once more before failing, and can wait a second time for a fetch started by another read in the
 * meantime: 20 seconds of waiting at most, besides the time of its own fetches. Readers built separately with the same
 * content identifier share the cached ranges, not the fetches in progress.
 *
 * <p><strong>Batch reads:</strong> {@link #readRanges(List)} serves cache hits directly and forwards the distinct
 * ranges neither cached nor being fetched to the delegate as a single {@code readRanges} call. Two requests for the
 * same range within one batch collapse into a single delegate fetch and cache store.
 *
 * <p><strong>Reads too large to cache:</strong> the misses of a read are its ranges neither cached nor being fetched by
 * another read. If they add up to more than the read-size limit of the reader, the reader reads them from the delegate
 * directly into the caller's buffers and does not cache them. Otherwise one large read would evict the small ranges
 * read again and again. The ranges of such a read already in the cache are still served from it, and its ranges being
 * fetched by another read are still awaited. {@link Builder#maxReadSize(long)} sets the limit, and
 * {@link #getBypassStats()} counts these reads for the readers built from one cache manager.
 */
public class CachingRangeReader extends AbstractRangeReader implements RangeReader {

    /** The value of {@link #processMaxReadSize} until the setting is read: a byte count is never negative. */
    private static final long NOT_READ = -1;

    // read once per process, by the first build without an explicit limit
    private static volatile long processMaxReadSize = NOT_READ;

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
        if (actualLength > loader.maxReadBytes()) {
            return readOverTheBound(range, target);
        }
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
     *
     * <p>If the ranges to forward add up to more than the read-size limit, the delegate receives the caller's own
     * requests for them, each with its own target, and the cache does not keep them.
     */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        Set<ByteRange> ranges = distinctNonEmptyRanges(requests);
        BatchLoadOutcome loaded = loader.load(ranges, delegate::readRanges);
        BatchReadResult bypassed = readBypassedRequests(loaded, requests);
        return copyOut(loaded, requests, bypassed);
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
    public Optional<String> version() {
        return delegate.version();
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
     * Gets the statistics of the byte-range cache of this reader's cache manager. Like {@link #getBypassStats()}, they
     * cover the readers built from that manager, not this reader alone.
     *
     * @return the statistics of the byte-range cache
     */
    public CacheStats getCacheStats() {
        return cache.stats();
    }

    /**
     * Gets the counts of the reads that were too large to cache. Like {@link #getCacheStats()}, they cover the readers
     * built from this reader's cache manager, not this reader alone. Each range of such a read also counts as a miss,
     * with no load, in {@link #getCacheStats()}.
     *
     * @return the calls that bypassed the cache, and their bytes
     */
    public BypassStats getBypassStats() {
        return cache.bypassStats();
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
        private OptionalLong maxReadSize = OptionalLong.empty();

        private Builder(RangeReader delegate) {
            this.delegate = requireNonNull(delegate, "Delegate cannot be null");
        }

        public Builder cacheManager(CacheManager cacheManager) {
            this.cacheManager = requireNonNull(cacheManager);
            return this;
        }

        /**
         * Sets the read-size limit of the reader. If the misses of a call add up to more than {@code bytes}, the reader
         * reads them directly into the caller's buffers and does not cache them. With zero, the reader caches no miss,
         * and still serves hits from the cache. The default is the setting {@value CacheSettings#DATA_MAX_READ_SIZE},
         * 16 MiB unless set.
         *
         * @param bytes the limit, in bytes
         * @return this builder
         * @throws IllegalArgumentException if {@code bytes} is negative
         */
        public Builder maxReadSize(long bytes) {
            if (bytes < 0) {
                throw new IllegalArgumentException("maxReadSize cannot be negative: " + bytes);
            }
            this.maxReadSize = OptionalLong.of(bytes);
            return this;
        }

        /**
         * Builds the CachingRangeReader.
         *
         * @return a new CachingRangeReader instance
         * @throws IllegalArgumentException if no limit was set and the setting
         *     {@value CacheSettings#DATA_MAX_READ_SIZE} is not a valid byte count
         */
        public CachingRangeReader build() {
            long maxReadBytes = maxReadSize.orElseGet(CachingRangeReader::defaultMaxReadSize);
            String contentIdentifier = delegate.contentIdentifier();
            ByteRangeCache cache = new ByteRangeCache(cacheManager, contentIdentifier);
            BatchRangeLoader loader = new BatchRangeLoader(cache, maxReadBytes);
            return new CachingRangeReader(delegate, cache, loader);
        }
    }

    /**
     * The read-size limit of the readers built without {@link Builder#maxReadSize(long)}: the setting
     * {@value CacheSettings#DATA_MAX_READ_SIZE}, read once per process.
     *
     * @return the limit, in bytes
     * @throws IllegalArgumentException if the setting is not a valid byte count; the next call reads it again
     */
    static long defaultMaxReadSize() {
        long known = processMaxReadSize;
        if (known == NOT_READ) {
            CacheSettings settings = CacheSettings.ofProcess();
            known = settings.byteCount(CacheSettings.DATA_MAX_READ_SIZE, CacheSettings.DEFAULT_DATA_MAX_READ_SIZE);
            processMaxReadSize = known;
        }
        return known;
    }

    /**
     * Reads a single range that is too large to cache. If the range is cached, it is served from the cache. Otherwise
     * it is read directly into its target. The one lookup counts as a hit or as a miss.
     */
    private int readOverTheBound(ByteRange range, ByteBuffer target) {
        ByteBuffer cached = cache.getIfPresent(range);
        if (cached != null) {
            return copyTo(cached, target);
        }
        return readAroundTheCache(range, target);
    }

    /** Reads {@code range} from the delegate directly into its target, and counts the bypass. */
    private int readAroundTheCache(ByteRange range, ByteBuffer target) {
        int bytesRead = delegate.readRange(range, target);
        cache.recordBypass(bytesRead);
        return bytesRead;
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

    /**
     * Reads the requests of the bypassed ranges directly into their targets, in one delegate call. Each request keeps
     * its own target. Duplicate and overlapping requests share one range in the cache, and each of them still gets its
     * bytes.
     *
     * @return the result of the delegate for those requests, in request order; empty when the read bypassed nothing
     */
    private BatchReadResult readBypassedRequests(BatchLoadOutcome loaded, List<RangeRequest> requests) {
        if (loaded.bypassed().isEmpty()) {
            return BatchReadResult.EMPTY;
        }
        List<RangeRequest> bypassed = bypassedRequests(loaded, requests);
        BatchReadResult below = delegate.readRanges(bypassed);
        long bytes = totalBytesRead(below);
        cache.recordBypass(bytes);
        return below;
    }

    /** The bypassed set holds no zero-length range: the loader never receives one. */
    private static List<RangeRequest> bypassedRequests(BatchLoadOutcome loaded, List<RangeRequest> requests) {
        List<RangeRequest> bypassed = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            if (loaded.bypassed(request.range())) {
                bypassed.add(request);
            }
        }
        return bypassed;
    }

    private static long totalBytesRead(BatchReadResult result) {
        long bytes = 0;
        for (int i = 0; i < result.requests(); i++) {
            bytes += result.bytesRead(i);
        }
        return bytes;
    }

    /**
     * Copies the cached values into the targets of their requests and builds the result of the read. The counts of the
     * bypassed requests come from {@code bypassed}, the result of their delegate call.
     */
    private static BatchReadResult copyOut(
            BatchLoadOutcome loaded, List<RangeRequest> requests, BatchReadResult bypassed) {
        int[] bytesRead = new int[requests.size()];
        long bytesFromCache = 0;
        Set<ByteRange> countedAsFetched = new HashSet<>();
        // the bypassed requests reached the delegate in request order: their counts follow that order
        int nextBypassed = 0;
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            ByteRange range = request.range();
            if (loaded.bypassed(range)) {
                bytesRead[i] = bypassed.bytesRead(nextBypassed);
                nextBypassed++;
            } else if (range.length() > 0) {
                ByteBuffer cached = loaded.cachedValue(range);
                bytesRead[i] = copyTo(cached, request.target());
                boolean firstRequestOfAFetchedRange = loaded.fetched(range) && countedAsFetched.add(range);
                if (!firstRequestOfAFetchedRange) {
                    bytesFromCache += bytesRead[i];
                }
            }
        }
        BatchReadResult ownView =
                BatchReadResult.of(requests, bytesRead, 0 /*fetches*/, 0 /*bytesTransferred*/, bytesFromCache);
        BatchReadResult withLoads = ownView.merge(loaded.cost());
        return withLoads.merge(bypassed);
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
