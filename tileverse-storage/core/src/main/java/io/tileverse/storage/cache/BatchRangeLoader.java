/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.batch.CoalescingPolicy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Loads ranges into the cache of one {@link CachingRangeReader}, dedup'ing across concurrent reads: a read that
 * requests a range being fetched by another read waits for that fetch and takes the cached value.
 *
 * <p>{@link CachingRangeReader#readRanges(List)} cannot rely on the cache alone to share the fetches of concurrent
 * batches. A batch must reach the delegate as one {@code readRanges} call: the delegate plans the batch as a whole,
 * merging nearby ranges into shared fetches under its {@link CoalescingPolicy} and running them in parallel. Caffeine
 * would instead dedup into several individual fetches, one per range.
 *
 * <p>This loader keeps a table of the ranges being fetched beside the cache. A read claims the ranges found in neither,
 * fetches them in one delegate call, stores them, and only then waits for the ranges claimed by other reads, outside
 * the locks of the cache.
 *
 * <p>The sharing is between the reads going through one loader. A wait lasts {@link #DEFAULT_WAIT_DEADLINE} at most, to
 * keep a stalled fetch from holding the reads waiting on it; past it, a read fetches the range itself, hands its value
 * to the reads still waiting and frees the range for the next ones.
 */
final class BatchRangeLoader {

    static final Duration DEFAULT_WAIT_DEADLINE = Duration.ofSeconds(10);

    private final ByteRangeCache cache;
    private final PendingLoads pending = new PendingLoads();
    private final Duration waitDeadline;

    BatchRangeLoader(ByteRangeCache cache) {
        this(cache, DEFAULT_WAIT_DEADLINE);
    }

    BatchRangeLoader(ByteRangeCache cache, Duration waitDeadline) {
        this.cache = requireNonNull(cache, "cache cannot be null");
        this.waitDeadline = requireNonNull(waitDeadline, "waitDeadline cannot be null");
    }

    /**
     * Returns the cached value of each range, fetching the missing ones through {@code fetch}. When a fetch awaited by
     * this read fails, the read tries the range once more. The failure of its own fetch, or of a fetch awaited on that
     * second try, is rethrown as thrown.
     *
     * @param ranges the distinct, non-empty ranges of one read
     * @param fetch reads ranges from the delegate
     * @return the values, the ranges fetched by this read and the cost of its fetches
     * @throws TransientStorageException if the calling thread is interrupted while waiting for another read
     */
    public BatchLoadOutcome load(Collection<ByteRange> ranges, RangeFetch fetch) {
        BatchRangeLoadTask load = new BatchRangeLoadTask(cache, pending, waitDeadline, System::nanoTime, fetch);
        return load.run(ranges);
    }

    /** The number of ranges being fetched at this moment. */
    public int pendingLoads() {
        return pending.size();
    }

    /** Reads ranges from the delegate of a {@link CachingRangeReader}, each into the target of its request. */
    @FunctionalInterface
    interface RangeFetch {

        /**
         * @param requests the ranges to read, each with a target of its size at position zero
         * @return the bytes read per request and the cost of the read
         */
        BatchReadResult fetch(List<RangeRequest> requests);
    }

    /**
     * The ranges being fetched through one {@link CachingRangeReader}. A read claims a range before fetching it; a read
     * finding the range claimed waits for that fetch instead of starting one.
     */
    static final class PendingLoads {

        private final ConcurrentMap<ByteRange, PendingLoad> loads = new ConcurrentHashMap<>();

        /**
         * Enters {@code candidate} in the table as the load of {@code range}, unless another load is fetching the
         * range.
         *
         * @param range the range about to be fetched
         * @param candidate the load of the caller for the range
         * @return the load fetching the range: {@code candidate} when it entered the table, for the caller to complete
         *     or fail; the load already there otherwise
         */
        PendingLoad claim(ByteRange range, PendingLoad candidate) {
            PendingLoad earlier = loads.putIfAbsent(range, candidate);
            return earlier == null ? candidate : earlier;
        }

        /** Hands {@code cachedValue} to the reads waiting on {@code load}. The cache holds the value by then. */
        void complete(ByteRange range, PendingLoad load, ByteBuffer cachedValue) {
            loads.remove(range, load);
            load.complete(cachedValue);
        }

        /**
         * Fails the reads waiting on {@code load}. The range is free to claim before they wake up, unless another load
         * holds it: a candidate never entered in the table frees nothing.
         */
        void fail(ByteRange range, PendingLoad load, Throwable failure) {
            loads.remove(range, load);
            load.fail(failure);
        }

        int size() {
            return loads.size();
        }
    }

    /** The fetch of one range by one batch read, awaited by the other reads needing the same range. */
    static final class PendingLoad {

        private final CompletableFuture<Outcome> outcome = new CompletableFuture<>();

        /** The end of the fetch: the cached value of the range, or the failure of the fetching read. */
        private record Outcome(ByteBuffer value, Throwable failure) {}

        void complete(ByteBuffer cachedValue) {
            outcome.complete(new Outcome(cachedValue, null));
        }

        void fail(Throwable failure) {
            // a value, never the failure of the future: get() would rethrow a CancellationException unwrapped and
            // report
            // a CompletionException as its cause, and waiters need the failure as thrown
            outcome.complete(new Outcome(null, failure));
        }

        boolean isDone() {
            return outcome.isDone();
        }

        /** Returns the cached value of the range once the fetch ended with one, null otherwise: it never waits. */
        ByteBuffer valueNow() {
            Outcome ended = outcome.getNow(null /*valueIfAbsent*/);
            if (ended == null) {
                return null;
            }
            return ended.value();
        }

        /**
         * Waits for the fetch to end.
         *
         * @param nanos the longest wait, in nanoseconds; zero or less checks without waiting
         * @return the cached value of the range
         * @throws ExecutionException if the fetch failed, with its failure as the cause
         * @throws TimeoutException if the fetch is still running after {@code nanos}
         * @throws InterruptedException if the calling thread is interrupted while waiting
         */
        ByteBuffer await(long nanos) throws InterruptedException, ExecutionException, TimeoutException {
            Outcome ended = outcome.get(Math.max(0, nanos), TimeUnit.NANOSECONDS);
            Throwable failure = ended.failure();
            if (failure != null) {
                throw new ExecutionException(failure);
            }
            return ended.value();
        }
    }
}
