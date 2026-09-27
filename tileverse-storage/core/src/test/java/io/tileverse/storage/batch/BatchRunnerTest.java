/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.batch;

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BatchRunnerTest {

    private static final int SOURCE_SIZE = 100_000;
    private final byte[] source = new byte[SOURCE_SIZE];
    private final List<ByteRange> fetchLog = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maxActive = new AtomicInteger();

    /** In-memory FetchReader with real EOF semantics, recording every fetch and the concurrency high-water mark. */
    private final FetchReader reader = (range, target) -> {
        int current = active.incrementAndGet();
        maxActive.accumulateAndGet(current, Math::max);
        try {
            fetchLog.add(range);
            if (range.offset() >= SOURCE_SIZE) {
                return 0;
            }
            int available = (int) Math.min(range.length(), SOURCE_SIZE - range.offset());
            target.put(source, (int) range.offset(), available);
            return available;
        } finally {
            active.decrementAndGet();
        }
    };

    private static final Supplier<Executor> FORBIDDEN_EXECUTOR = () -> {
        throw new AssertionError("the executor must not be resolved for sequential plans");
    };

    @BeforeEach
    void fillSource() {
        for (int i = 0; i < SOURCE_SIZE; i++) {
            source[i] = (byte) (i % 251);
        }
    }

    private static List<RangeRequest> requests(long[][] ranges) {
        List<RangeRequest> requests = new ArrayList<>();
        for (long[] range : ranges) {
            requests.add(RangeRequest.of(range[0], (int) range[1], ByteBuffer.allocate((int) range[1])));
        }
        return requests;
    }

    private void assertContents(List<RangeRequest> requests, int[] counts) {
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            long offset = request.range().offset();
            int expected = (int) Math.max(0, Math.min(request.range().length(), SOURCE_SIZE - offset));
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(expected);
            ByteBuffer target = request.target().duplicate().flip();
            assertThat(target.remaining()).isEqualTo(expected);
            for (int b = 0; b < expected; b++) {
                assertThat(target.get(b)).as("byte " + b + " of entry " + i).isEqualTo(source[(int) offset + b]);
            }
        }
    }

    @Test
    void capOfOneRunsSequentiallyWithoutTheExecutor() {
        List<RangeRequest> batch = requests(new long[][] {{0, 100}, {50_000, 200}, {99_990, 50}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);

        int[] counts = counts(BatchRunner.run(batch, fetches, reader, 1, FORBIDDEN_EXECUTOR));

        assertContents(batch, counts);
        assertThat(fetchLog).containsExactly(ByteRange.of(0, 100), ByteRange.of(50_000, 200), ByteRange.of(99_990, 50));
    }

    @Test
    void singleFetchPlanNeverResolvesTheExecutor() {
        List<RangeRequest> batch = requests(new long[][] {{10, 100}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);

        int[] counts = counts(BatchRunner.run(batch, fetches, reader, 8, FORBIDDEN_EXECUTOR));

        assertContents(batch, counts);
    }

    @Test
    void directFetchWritesTheCallerTarget() {
        List<RangeRequest> batch = requests(new long[][] {{0, 64}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);
        List<ByteBuffer> seenTargets = Collections.synchronizedList(new ArrayList<>());
        FetchReader spying = (range, target) -> {
            seenTargets.add(target);
            return reader.read(range, target);
        };

        BatchRunner.run(batch, fetches, spying, 1, FORBIDDEN_EXECUTOR);

        assertThat(seenTargets).containsExactly(batch.get(0).target());
    }

    @Test
    void mergedFetchScattersFromScratch() {
        List<RangeRequest> batch = requests(new long[][] {{0, 10}, {20, 10}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, new CoalescingPolicy(64, 1024));
        assertThat(fetches).hasSize(1);

        int[] counts = counts(BatchRunner.run(batch, fetches, reader, 1, FORBIDDEN_EXECUTOR));

        assertContents(batch, counts);
        assertThat(fetchLog).containsExactly(ByteRange.of(0, 30));
    }

    @Test
    void resultCountsTheFetchesAndTheBytesTheyReadGapsIncluded() {
        List<RangeRequest> batch = requests(new long[][] {{0, 10}, {20, 10}, {5_000, 100}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, new CoalescingPolicy(64, 1024));
        assertThat(fetches).hasSize(2);

        BatchReadResult result = BatchRunner.run(batch, fetches, reader, 1, FORBIDDEN_EXECUTOR);

        assertThat(result.bytesRequested()).isEqualTo(120);
        assertThat(result.fetches()).isEqualTo(2);
        assertThat(result.bytesTransferred())
                .as("requested bytes plus the 10-byte gap")
                .isEqualTo(130);
        assertThat(result.bytesFromCache()).isZero();
    }

    @Test
    void resultTransfersOnlyWhatAShortFetchRead() {
        List<RangeRequest> batch = requests(new long[][] {{99_990, 10}, {100_010, 10}, {200_000, 10}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, new CoalescingPolicy(200, 4096));
        assertThat(fetches).hasSize(2);

        BatchReadResult result = BatchRunner.run(batch, fetches, reader, 1, FORBIDDEN_EXECUTOR);

        assertThat(result.fetches()).isEqualTo(2);
        assertThat(result.bytesTransferred())
                .as("the merged fetch stops at EOF, the last one reads nothing")
                .isEqualTo(10);
    }

    @Test
    void shortFetchClampsItsSlices() {
        List<RangeRequest> batch = requests(new long[][] {{99_990, 10}, {100_010, 10}, {100_100, 5}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, new CoalescingPolicy(200, 4096));
        assertThat(fetches).hasSize(1);

        int[] counts = counts(BatchRunner.run(batch, fetches, reader, 1, FORBIDDEN_EXECUTOR));

        assertThat(counts).containsExactly(10, 0, 0);
        assertContents(batch, counts);
    }

    @Test
    void concurrentWorkersHonorTheCapAndProduceCorrectResults() {
        long[][] ranges = new long[12][];
        for (int i = 0; i < ranges.length; i++) {
            ranges[i] = new long[] {i * 8_000L, 500};
        }
        List<RangeRequest> batch = requests(ranges);
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            int[] counts = counts(BatchRunner.run(batch, fetches, reader, 3, () -> executor));

            assertContents(batch, counts);
            assertThat(maxActive.get()).isLessThanOrEqualTo(3);
            assertThat(fetchLog).hasSize(12);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void firstFailureIsRethrownAfterTheRestDrain() {
        List<RangeRequest> batch = requests(new long[][] {{0, 10}, {1_000, 10}, {2_000, 10}, {3_000, 10}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);
        StorageException boom = new StorageException("fetch 1 failed");
        FetchReader failing = (range, target) -> {
            if (range.offset() == 1_000) {
                throw boom;
            }
            return reader.read(range, target);
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            assertThatThrownBy(() -> BatchRunner.run(batch, fetches, failing, 2, () -> executor))
                    .isSameAs(boom);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * The target contract of {@link io.tileverse.storage.RangeReader#readRanges}: once the call returns or throws, no
     * thread is still writing a target. A failure is therefore reported only after the fetch still running on another
     * worker has completed.
     */
    @Test
    void aFailureIsThrownOnlyAfterTheFetchesInFlightCompleted() throws Exception {
        List<RangeRequest> batch = requests(new long[][] {{0, 10}, {1_000, 10}, {2_000, 10}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);
        CountDownLatch slowFetchStarted = new CountDownLatch(1);
        CountDownLatch failureThrown = new CountDownLatch(1);
        CountDownLatch releaseSlowFetch = new CountDownLatch(1);
        AtomicBoolean slowFetchWriting = new AtomicBoolean();
        FetchReader failingBesideASlowFetch = (range, target) -> {
            if (range.offset() == 0) {
                slowFetchWriting.set(true);
                slowFetchStarted.countDown();
                awaitQuietly(releaseSlowFetch);
                int read = reader.read(range, target);
                slowFetchWriting.set(false);
                return read;
            }
            awaitQuietly(slowFetchStarted);
            failureThrown.countDown();
            throw new StorageException("fetch at " + range.offset() + " failed");
        };
        ExecutorService workers = Executors.newFixedThreadPool(2);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> call =
                    caller.submit(() -> BatchRunner.run(batch, fetches, failingBesideASlowFetch, 2, () -> workers));
            assertThat(failureThrown.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(call.isDone())
                    .as("the call waits for the fetch in flight")
                    .isFalse();

            releaseSlowFetch.countDown();

            assertThatThrownBy(() -> call.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(StorageException.class);
            assertThat(slowFetchWriting)
                    .as("no fetch writes a target after the call threw")
                    .isFalse();
        } finally {
            caller.shutdownNow();
            workers.shutdownNow();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Test
    void sequentialFailurePropagates() {
        List<RangeRequest> batch = requests(new long[][] {{0, 10}});
        List<PlannedFetch> fetches = BatchPlanner.plan(batch, CoalescingPolicy.NONE);
        FetchReader failing = (range, target) -> {
            throw new StorageException("boom");
        };

        assertThatThrownBy(() -> BatchRunner.run(batch, fetches, failing, 1, FORBIDDEN_EXECUTOR))
                .isInstanceOf(StorageException.class);
    }

    @Test
    void invalidCapIsRejected() {
        List<RangeRequest> emptyRequests = List.of();
        List<PlannedFetch> emptyFetches = List.of();

        assertThatThrownBy(() -> BatchRunner.run(emptyRequests, emptyFetches, reader, 0, FORBIDDEN_EXECUTOR))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
