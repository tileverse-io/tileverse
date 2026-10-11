/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import io.tileverse.io.ByteRange;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.cache.BatchRangeLoader.PendingLoad;
import io.tileverse.storage.cache.BatchRangeLoader.PendingLoads;
import io.tileverse.storage.cache.BatchRangeLoader.RangeFetch;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/**
 * One batch read going through a {@link BatchRangeLoader}.
 *
 * <p>Fetches the ranges claimed by it before waiting for a range claimed by another read. A read holding claims never
 * waits and a waiting read holds none: two reads cannot wait on each other.
 */
final class BatchRangeLoadTask {

    private final ByteRangeCache cache;
    private final PendingLoads pending;
    private final Duration waitDeadline;
    private final LongSupplier nanoClock;
    private final RangeFetch fetch;

    private final Map<ByteRange, ByteBuffer> values = new HashMap<>();
    // created at the first fetch: a read served from the cache fetches nothing
    private Set<ByteRange> fetched = Set.of();
    private BatchReadResult cost = BatchReadResult.EMPTY;

    BatchRangeLoadTask(
            ByteRangeCache cache,
            PendingLoads pending,
            Duration waitDeadline,
            LongSupplier nanoClock,
            RangeFetch fetch) {
        this.cache = cache;
        this.pending = pending;
        this.waitDeadline = waitDeadline;
        this.nanoClock = nanoClock;
        this.fetch = fetch;
    }

    BatchLoadOutcome run(Collection<ByteRange> ranges) {
        Round first = load(ranges);
        if (first.hasRangesWithoutValue()) {
            Round retry = load(first.failed().keySet());
            rethrowIfFailed(retry);

            Map<ByteRange, PendingLoad> gaveUpOn = new LinkedHashMap<>();
            gaveUpOn.putAll(first.pastDeadline());
            gaveUpOn.putAll(retry.pastDeadline());

            fetchWithoutClaim(gaveUpOn);
        }
        return loaded();
    }

    private BatchLoadOutcome loaded() {
        return new BatchLoadOutcome(values, fetched, cost);
    }

    /**
     * One pass over {@code ranges}: loads the cached, claimed and awaited ranges and reports the failed and the expired
     * waits to the caller.
     */
    // catch Throwable, a claim left behind by an Error costs a read that miss its range a whole deadline
    @SuppressWarnings("java:S1181")
    private Round load(Collection<ByteRange> ranges) {
        Round round = new Round();
        try {
            for (ByteRange range : ranges) {
                classify(range, round);
            }
            Map<ByteRange, PendingLoad> claimed = round.claimed();
            fetchAndComplete(claimed);
        } catch (Throwable failure) {
            failUnfinishedClaims(round, failure);
            throw failure;
        }
        awaitOtherReads(round);
        return round;
    }

    /** Puts {@code range} in its pile of the round: cached, claimed by this read, or awaited from another read. */
    private void classify(ByteRange range, Round round) {
        ByteBuffer cached = cache.getIfPresentQuietly(range);
        if (cached != null) {
            useCached(range, cached);
            return;
        }
        PendingLoad mine = new PendingLoad();
        // in the round before it is in the table: a failure from here on has to reach the claim
        PendingLoad claimedEarlier = round.claim(range, mine);
        if (claimedEarlier != null) {
            // repeated in the read: the range is already claimed by it
            return;
        }
        PendingLoad fetching = pending.claim(range, mine);
        if (fetching != mine) {
            round.unclaim(range);
            round.await(range, fetching);
            return;
        }
        // a read finishing between the lookup above and the claim stored the value and left no load behind
        ByteBuffer storedMeanwhile = cache.getIfPresentQuietly(range);
        if (storedMeanwhile != null) {
            pending.complete(range, mine, storedMeanwhile);
            round.unclaim(range);
            useCached(range, storedMeanwhile);
        }
    }

    /**
     * Records the one lookup of this read for {@code range}, the lookups before it being quiet: a hit, or a miss when
     * the entry left the cache since {@code found} was read. Nothing is stored back: a cache cleared in between stays
     * cleared, and the read keeps the value in hand.
     */
    private void useCached(ByteRange range, ByteBuffer found) {
        ByteBuffer recorded = cache.getIfPresent(range);
        ByteBuffer value = recorded == null ? found : recorded;
        values.put(range, value);
    }

    /** Fetches the ranges of {@code loads} in one call, stores the values and completes the loads with them. */
    private void fetchAndComplete(Map<ByteRange, PendingLoad> loads) {
        if (loads.isEmpty()) {
            return;
        }
        List<RangeRequest> requests = requestsFor(loads.keySet());
        BatchReadResult result = fetch.fetch(requests);
        cost = cost.merge(result);
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            ByteRange range = request.range();
            int bytesRead = result.bytesRead(i);
            ByteBuffer stored = store(request, bytesRead);
            PendingLoad load = loads.get(range);
            pending.complete(range, load, stored);
        }
    }

    /** One heap buffer per range: the buffer becomes the cached value, a pooled one would be reused under it. */
    private static List<RangeRequest> requestsFor(Collection<ByteRange> ranges) {
        List<RangeRequest> requests = new ArrayList<>(ranges.size());
        for (ByteRange range : ranges) {
            ByteBuffer target = ByteBuffer.allocate(range.length());
            requests.add(new RangeRequest(range, target));
        }
        return requests;
    }

    private ByteBuffer store(RangeRequest filled, int bytesRead) {
        ByteRange range = filled.range();
        ByteBuffer value = sanitize(filled.target(), bytesRead);
        ByteBuffer stored = cache.getOrStore(range, value);
        values.put(range, stored);
        if (fetched.isEmpty()) {
            fetched = new HashSet<>();
        }
        fetched.add(range);
        return stored;
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

    private void failUnfinishedClaims(Round round, Throwable failure) {
        for (Map.Entry<ByteRange, PendingLoad> claim : round.claimed().entrySet()) {
            PendingLoad load = claim.getValue();
            if (!load.isDone()) {
                pending.fail(claim.getKey(), load, failure);
            }
        }
    }

    private void awaitOtherReads(Round round) {
        if (round.awaited().isEmpty()) {
            return;
        }
        long deadline = nanoClock.getAsLong() + waitDeadline.toNanos();
        for (Map.Entry<ByteRange, PendingLoad> awaited : round.awaited().entrySet()) {
            ByteRange range = awaited.getKey();
            PendingLoad load = awaited.getValue();
            await(range, load, deadline, round);
        }
    }

    private void await(ByteRange range, PendingLoad load, long deadline, Round round) {
        try {
            long left = deadline - nanoClock.getAsLong();
            ByteBuffer cached = load.await(left);
            useCached(range, cached);
        } catch (ExecutionException fetchFailed) {
            Throwable failure = fetchFailed.getCause();
            round.waitFailed(range, failure);
        } catch (TimeoutException stillRunning) {
            round.waitExpired(range, load);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TransientStorageException(
                    "Interrupted while waiting for a concurrent read of " + range, interrupted);
        }
    }

    /** The failure belongs to the fetching read: it is shared with its other waiters and rethrown untouched. */
    private static void rethrowIfFailed(Round retry) {
        if (!retry.failed().isEmpty()) {
            Collection<Throwable> failures = retry.failed().values();
            Throwable failure = failures.iterator().next();
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException unchecked) {
                throw unchecked;
            }
            throw new StorageException("A concurrent read of the same range failed", failure);
        }
    }

    /**
     * The read gave up on these loads at a deadline. It fetches their ranges without a claim and completes the loads
     * with the stored values: the reads still waiting on them wake up with the values, and the ranges are free for the
     * next reads missing them. A failing fetch leaves the loads to their owners.
     */
    private void fetchWithoutClaim(Map<ByteRange, PendingLoad> gaveUpOn) {
        Map<ByteRange, PendingLoad> stillWithoutValue = takeLoadsEndedSince(gaveUpOn);
        fetchAndComplete(stillWithoutValue);
    }

    /** Takes the value of the loads ended since the read stopped waiting for them, and returns the other loads. */
    private Map<ByteRange, PendingLoad> takeLoadsEndedSince(Map<ByteRange, PendingLoad> gaveUpOn) {
        Map<ByteRange, PendingLoad> stillWithoutValue = new LinkedHashMap<>();
        for (Map.Entry<ByteRange, PendingLoad> stalled : gaveUpOn.entrySet()) {
            ByteRange range = stalled.getKey();
            PendingLoad load = stalled.getValue();
            ByteBuffer endedSince = load.valueNow();
            if (endedSince == null) {
                stillWithoutValue.put(range, load);
            } else {
                useCached(range, endedSince);
            }
        }
        return stillWithoutValue;
    }

    /** One pass over a set of ranges: its claims, its waits, and the ranges left to the next step. */
    private static final class Round {
        private Map<ByteRange, PendingLoad> claimed = Map.of();
        private Map<ByteRange, PendingLoad> awaited = Map.of();
        private Map<ByteRange, Throwable> failed = Map.of();
        private Map<ByteRange, PendingLoad> pastDeadline = Map.of();

        /** Returns the load already claiming {@code range} in this round, or null once {@code load} claims it. */
        PendingLoad claim(ByteRange range, PendingLoad load) {
            if (claimed.isEmpty()) {
                claimed = new LinkedHashMap<>();
            }
            return claimed.putIfAbsent(range, load);
        }

        /** Takes a claim back: another read fetches the range, or its value was stored meanwhile. */
        void unclaim(ByteRange range) {
            claimed.remove(range);
        }

        void await(ByteRange range, PendingLoad load) {
            if (awaited.isEmpty()) {
                awaited = new LinkedHashMap<>();
            }
            awaited.put(range, load);
        }

        void waitFailed(ByteRange range, Throwable failure) {
            if (failed.isEmpty()) {
                failed = new LinkedHashMap<>();
            }
            failed.put(range, failure);
        }

        void waitExpired(ByteRange range, PendingLoad load) {
            if (pastDeadline.isEmpty()) {
                pastDeadline = new LinkedHashMap<>();
            }
            pastDeadline.put(range, load);
        }

        Map<ByteRange, PendingLoad> claimed() {
            return claimed;
        }

        Map<ByteRange, PendingLoad> awaited() {
            return awaited;
        }

        Map<ByteRange, Throwable> failed() {
            return failed;
        }

        Map<ByteRange, PendingLoad> pastDeadline() {
            return pastDeadline;
        }

        /** True when a wait of the pass ended without a value: failed, to retry, or expired, to fetch for. */
        boolean hasRangesWithoutValue() {
            return !failed.isEmpty() || !pastDeadline.isEmpty();
        }
    }
}
