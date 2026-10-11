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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.tileverse.cache.CacheManager;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.ByteArrayRangeReader;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.cache.BatchRangeLoader.PendingLoad;
import io.tileverse.storage.cache.BatchRangeLoader.PendingLoads;
import io.tileverse.storage.cache.BatchRangeLoader.RangeFetch;
import io.tileverse.storage.cache.PausingRangeReaderCache.Pause;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

class BatchRangeLoaderTest {

    private static final byte[] DATA = randomBytes(10_000);
    private static final ByteRange FIRST = new ByteRange(100, 50);
    private static final ByteRange SECOND = new ByteRange(2_000, 300);

    private final ByteArrayRangeReader source = new ByteArrayRangeReader(DATA);
    private final CacheManager cacheManager = CacheManager.newInstance();

    @AfterEach
    void releaseTheCache() {
        cacheManager.invalidateAll();
    }

    @Test
    void missesFetchedInOneCall() {
        ByteRangeCache cache = new ByteRangeCache(cacheManager, source.getSourceIdentifier());
        BatchRangeLoader loader = new BatchRangeLoader(cache);

        BatchLoadOutcome loaded = loader.load(List.of(FIRST, SECOND), source::readRanges);

        assertThat(source.batchReads()).containsExactly(List.of(FIRST, SECOND));
        assertThat(loaded.fetched()).containsExactlyInAnyOrder(FIRST, SECOND);
        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST)).containsEntry(SECOND, bytesOf(SECOND));
        assertThat(loaded.cost().fetches()).isEqualTo(2);
        assertThat(loaded.cost().bytesTransferred()).isEqualTo(350);
        assertThat(cache.getIfPresentQuietly(FIRST)).isEqualTo(bytesOf(FIRST));
        assertThat(loader.pendingLoads()).isZero();
    }

    @Test
    void cachedRangesAreNotFetched() {
        BatchRangeLoader loader = new BatchRangeLoader(new ByteRangeCache(cacheManager, source.getSourceIdentifier()));
        loader.load(List.of(FIRST), source::readRanges);
        source.clearRecordings();

        BatchLoadOutcome loaded = loader.load(List.of(FIRST, SECOND), source::readRanges);

        assertThat(source.batchReads()).containsExactly(List.of(SECOND));
        assertThat(loaded.fetched()).containsExactly(SECOND);
        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST));
        assertThat(loaded.cost().fetches()).isEqualTo(1);
    }

    // cleared between the lookup finding the value and the recording of the hit
    @Test
    void hitStoresNothingAfterClear() {
        ByteRangeCache clearedUnderTheHit = new ByteRangeCache(cacheManager, source.getSourceIdentifier()) {
            private boolean cleared;

            @Override
            public ByteBuffer getIfPresentQuietly(ByteRange range) {
                ByteBuffer found = super.getIfPresentQuietly(range);
                if (found != null && !cleared) {
                    cleared = true;
                    invalidateAll();
                }
                return found;
            }
        };
        BatchRangeLoader loader = new BatchRangeLoader(clearedUnderTheHit);
        loader.load(List.of(FIRST), source::readRanges);
        source.clearRecordings();

        BatchLoadOutcome loaded = loader.load(List.of(FIRST), source::readRanges);

        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST));
        assertThat(loaded.fetched()).isEmpty();
        assertThat(source.batchReads()).isEmpty();
        assertThat(clearedUnderTheHit.getIfPresentQuietly(FIRST)).isNull();
    }

    @Test
    void storedBeforeClaimNotFetched() {
        ByteRangeCache storingAfterTheFirstLookup = new ByteRangeCache(cacheManager, source.getSourceIdentifier()) {
            private boolean stored;

            @Override
            public ByteBuffer getIfPresentQuietly(ByteRange range) {
                ByteBuffer found = super.getIfPresentQuietly(range);
                if (!stored) {
                    stored = true;
                    getOrStore(range, bytesOf(range));
                }
                return found;
            }
        };
        BatchRangeLoader loader = new BatchRangeLoader(storingAfterTheFirstLookup);

        BatchLoadOutcome loaded = loader.load(List.of(FIRST), source::readRanges);

        assertThat(source.batchReads()).isEmpty();
        assertThat(loaded.fetched()).isEmpty();
        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST));
        assertThat(loader.pendingLoads()).isZero();
    }

    @Test
    void claimReleasedOnLookupFailure() {
        IllegalStateException lookupFailure = new IllegalStateException("the lookup after the claim fails");
        ByteRangeCache failingOnTheSecondLookup = new ByteRangeCache(cacheManager, source.getSourceIdentifier()) {
            private int lookups;

            @Override
            public ByteBuffer getIfPresentQuietly(ByteRange range) {
                lookups++;
                if (lookups == 2) {
                    throw lookupFailure;
                }
                return super.getIfPresentQuietly(range);
            }
        };
        BatchRangeLoader loader = new BatchRangeLoader(failingOnTheSecondLookup);
        List<ByteRange> ranges = List.of(FIRST);
        RangeFetch fetch = source::readRanges;

        assertThatThrownBy(() -> loader.load(ranges, fetch)).isSameAs(lookupFailure);

        assertThat(loader.pendingLoads()).isZero();
    }

    /**
     * The table takes the load of the read, then fails before answering. The read has the load in its round by then. It
     * takes the load back out of the table and fails it for the reads already waiting on it.
     */
    @Test
    void claimReleasedOnTableFailure() {
        SimulatedError crash = new SimulatedError();
        AtomicReference<PendingLoad> taken = new AtomicReference<>();
        Answer<PendingLoad> takingTheLoadThenFailing = claiming -> {
            taken.set(claiming.getArgument(1));
            claiming.callRealMethod();
            throw crash;
        };
        PendingLoads table = spy(new PendingLoads());
        doAnswer(takingTheLoadThenFailing).when(table).claim(any(), any());
        ByteRangeCache cache = new ByteRangeCache(cacheManager, source.getSourceIdentifier());
        BatchRangeLoadTask read = new BatchRangeLoadTask(
                cache, table, BatchRangeLoader.DEFAULT_WAIT_DEADLINE, System::nanoTime, source::readRanges);
        List<ByteRange> ranges = List.of(FIRST);

        assertThatThrownBy(() -> read.run(ranges)).isSameAs(crash);

        assertThat(table.size()).isZero();
        PendingLoad released = taken.get();
        assertThatThrownBy(() -> released.await(0))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isSameAs(crash);
    }

    @Test
    void repeatedRangeClaimedOnce() {
        BatchRangeLoader loader = new BatchRangeLoader(new ByteRangeCache(cacheManager, source.getSourceIdentifier()));

        BatchLoadOutcome loaded = loader.load(List.of(FIRST, FIRST), source::readRanges);

        assertThat(source.batchReads()).containsExactly(List.of(FIRST));
        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST));
        assertThat(loader.pendingLoads()).isZero();
    }

    /**
     * Read one holds FIRST and needs SECOND. Read two holds SECOND and needs FIRST. Each fetches its own range before
     * waiting for the other's. Waiting first would hold both ranges for the whole deadline.
     */
    @Test
    void crossedClaimsBothComplete() throws Exception {
        PausingRangeReaderCache cache = new PausingRangeReaderCache(cacheManager, source.getSourceIdentifier());
        BatchRangeLoader loader = new BatchRangeLoader(cache, Duration.ofMinutes(1));
        CountDownLatch fetchOfSecondEntered = new CountDownLatch(1);
        CountDownLatch fetchOfSecondReleased = new CountDownLatch(1);
        RangeFetch fetch = requests -> {
            if (requests.get(0).range().equals(SECOND)) {
                fetchOfSecondEntered.countDown();
                awaitRelease(fetchOfSecondReleased);
            }
            return source.readRanges(requests);
        };
        Pause beforeReadOneLooksSecondUp = cache.pauseNextLookupOf(SECOND);

        RunningRead<BatchLoadOutcome> readOne = RunningRead.start(() -> loader.load(List.of(FIRST, SECOND), fetch));
        beforeReadOneLooksSecondUp.awaitReached(); // read one holds FIRST
        RunningRead<BatchLoadOutcome> readTwo = RunningRead.start(() -> loader.load(List.of(SECOND, FIRST), fetch));
        assertThat(fetchOfSecondEntered.await(10, TimeUnit.SECONDS)).isTrue(); // read two holds SECOND
        beforeReadOneLooksSecondUp.resume();
        readOne.awaitWaitingForAnotherRead(); // read one fetched FIRST and waits for SECOND
        fetchOfSecondReleased.countDown();

        BatchLoadOutcome one = readOne.result();
        BatchLoadOutcome two = readTwo.result();

        assertThat(one.fetched()).containsExactly(FIRST);
        assertThat(two.fetched()).containsExactly(SECOND);
        assertThat(one.values()).containsEntry(SECOND, bytesOf(SECOND));
        assertThat(two.values()).containsEntry(FIRST, bytesOf(FIRST));
        assertThat(source.batchReads()).containsExactlyInAnyOrder(List.of(FIRST), List.of(SECOND));
        assertThat(loader.pendingLoads()).isZero();
    }

    /**
     * The read waits for two ranges held in the table. The clock passes the deadline during the first wait. That wait
     * ends with the completion of its load. The read then looks at the second load without waiting and fetches that
     * range itself.
     */
    @Test
    void awaitedRangesShareDeadline() throws Exception {
        ByteRangeCache cache = new ByteRangeCache(cacheManager, source.getSourceIdentifier());
        PendingLoads table = new PendingLoads();
        PendingLoad fetchingFirst = new PendingLoad();
        table.claim(FIRST, fetchingFirst);
        table.claim(SECOND, new PendingLoad());
        Duration deadline = Duration.ofMinutes(1);
        AtomicLong nanos = new AtomicLong();
        BatchRangeLoadTask read = new BatchRangeLoadTask(cache, table, deadline, nanos::get, source::readRanges);

        RunningRead<BatchLoadOutcome> waiter = RunningRead.start(() -> read.run(List.of(FIRST, SECOND)));
        waiter.awaitWaitingForAnotherRead();
        nanos.addAndGet(deadline.toNanos());
        ByteBuffer first = cache.getOrStore(FIRST, bytesOf(FIRST));
        table.complete(FIRST, fetchingFirst, first);
        BatchLoadOutcome loaded = waiter.result();

        assertThat(loaded.fetched()).containsExactly(SECOND);
        assertThat(source.batchReads()).containsExactly(List.of(SECOND));
        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST)).containsEntry(SECOND, bytesOf(SECOND));
    }

    /**
     * Two reads wait for a stalled fetch, one for a minute and one for 50 ms. The second gives up, fetches the range
     * and completes the stalled load with its value. The first wakes up with that value.
     */
    @Test
    void givingUpWakesOtherWaiters() throws Exception {
        ByteRangeCache cache = new ByteRangeCache(cacheManager, source.getSourceIdentifier());
        PendingLoads table = new PendingLoads();
        table.claim(FIRST, new PendingLoad());
        BatchRangeLoadTask patient = readThrough(cache, table, Duration.ofMinutes(1));
        BatchRangeLoadTask impatient = readThrough(cache, table, Duration.ofMillis(50));

        RunningRead<BatchLoadOutcome> waiting = RunningRead.start(() -> patient.run(List.of(FIRST)));
        waiting.awaitWaitingForAnotherRead();
        BatchLoadOutcome givenUp = impatient.run(List.of(FIRST));
        BatchLoadOutcome woken = waiting.result();

        assertThat(givenUp.fetched()).containsExactly(FIRST);
        assertThat(woken.fetched()).isEmpty();
        assertThat(woken.values()).containsEntry(FIRST, bytesOf(FIRST));
        assertThat(source.batchReads()).containsExactly(List.of(FIRST));
        assertThat(table.size()).isZero();
    }

    // the entry is gone again after the give-up: the next read fetches at once
    @Test
    void givenUpRangeIsFreeAgain() throws Exception {
        ByteRangeCache cache = new ByteRangeCache(cacheManager, source.getSourceIdentifier());
        PendingLoads table = new PendingLoads();
        table.claim(FIRST, new PendingLoad());
        BatchRangeLoadTask impatient = readThrough(cache, table, Duration.ofMillis(50));
        BatchRangeLoadTask later = readThrough(cache, table, Duration.ofMinutes(1));
        impatient.run(List.of(FIRST));
        cache.invalidateAll();

        RunningRead<BatchLoadOutcome> missingAgain = RunningRead.start(() -> later.run(List.of(FIRST)));
        BatchLoadOutcome loaded = missingAgain.result();

        assertThat(loaded.fetched()).containsExactly(FIRST);
        assertThat(source.batchReads()).containsExactly(List.of(FIRST), List.of(FIRST));
        assertThat(table.size()).isZero();
    }

    /**
     * The wait for SECOND fails once the clock is past the deadline, and the wait for FIRST expires at once. While the
     * read tries SECOND again, the stalled fetch of FIRST ends. The read takes its value and fetches SECOND only.
     */
    @Test
    void endedLoadNotFetchedAgain() throws Exception {
        PausingRangeReaderCache cache = new PausingRangeReaderCache(cacheManager, source.getSourceIdentifier());
        PendingLoads table = new PendingLoads();
        PendingLoad failingOnSecond = new PendingLoad();
        PendingLoad stalledOnFirst = new PendingLoad();
        table.claim(SECOND, failingOnSecond);
        table.claim(FIRST, stalledOnFirst);
        Duration deadline = Duration.ofMinutes(1);
        AtomicLong nanos = new AtomicLong();
        BatchRangeLoadTask read = new BatchRangeLoadTask(cache, table, deadline, nanos::get, source::readRanges);

        RunningRead<BatchLoadOutcome> waiter = RunningRead.start(() -> read.run(List.of(SECOND, FIRST)));
        waiter.awaitWaitingForAnotherRead(); // for SECOND
        nanos.addAndGet(deadline.toNanos());
        Pause beforeTheSecondTry = cache.pauseNextLookupBy(waiter.thread());
        table.fail(SECOND, failingOnSecond, new StorageException("backend down"));
        beforeTheSecondTry.awaitReached(); // the wait for FIRST expired on the way
        ByteBuffer first = cache.getOrStore(FIRST, bytesOf(FIRST));
        table.complete(FIRST, stalledOnFirst, first);
        beforeTheSecondTry.resume();
        BatchLoadOutcome loaded = waiter.result();

        assertThat(loaded.fetched()).containsExactly(SECOND);
        assertThat(source.batchReads()).containsExactly(List.of(SECOND));
        assertThat(loaded.values()).containsEntry(FIRST, bytesOf(FIRST)).containsEntry(SECOND, bytesOf(SECOND));
    }

    private BatchRangeLoadTask readThrough(ByteRangeCache cache, PendingLoads table, Duration waitDeadline) {
        return new BatchRangeLoadTask(cache, table, waitDeadline, System::nanoTime, source::readRanges);
    }

    private static void awaitRelease(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static ByteBuffer bytesOf(ByteRange range) {
        ByteBuffer bytes = ByteBuffer.wrap(DATA, Math.toIntExact(range.offset()), range.length());
        return bytes.slice().asReadOnlyBuffer();
    }

    private static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        new Random(7).nextBytes(bytes);
        return bytes;
    }

    private static final class SimulatedError extends Error {
        private static final long serialVersionUID = 1L;
    }
}
