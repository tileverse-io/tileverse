/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.cache.BatchRangeLoader.PendingLoad;
import io.tileverse.storage.cache.BatchRangeLoader.PendingLoads;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

class PendingLoadsTest {

    private static final ByteRange RANGE = new ByteRange(100, 10);

    private final PendingLoads loads = new BatchRangeLoader.PendingLoads();

    @Test
    void firstClaimWins() {
        PendingLoad first = new PendingLoad();
        PendingLoad second = new PendingLoad();

        PendingLoad fetchingForTheFirst = loads.claim(RANGE, first);
        PendingLoad fetchingForTheSecond = loads.claim(RANGE, second);

        assertThat(fetchingForTheFirst).isSameAs(first);
        assertThat(fetchingForTheSecond).isSameAs(first);
        assertThat(loads.size()).isEqualTo(1);
    }

    @Test
    void rangesAreClaimedIndependently() {
        PendingLoad first = new PendingLoad();
        PendingLoad other = new PendingLoad();

        PendingLoad fetchingTheRange = loads.claim(RANGE, first);
        PendingLoad fetchingTheOtherRange = loads.claim(new ByteRange(200, 10), other);

        assertThat(fetchingTheRange).isSameAs(first);
        assertThat(fetchingTheOtherRange).isSameAs(other);
        assertThat(loads.size()).isEqualTo(2);
    }

    @Test
    void completedLoadHandsOverAndFrees() throws Exception {
        PendingLoad owner = claim(RANGE);
        PendingLoad awaited = claim(RANGE);
        ByteBuffer value = ByteBuffer.allocate(10).asReadOnlyBuffer();

        loads.complete(RANGE, owner, value);

        assertThat(awaited.await(0)).isSameAs(value);
        assertThat(loads.size()).isZero();
        assertFreeToClaim(RANGE);
    }

    @ParameterizedTest
    @MethodSource("failuresOfAFetch")
    void failedLoadFailsWaitersAndFrees(Throwable failure) {
        PendingLoad owner = claim(RANGE);
        PendingLoad awaited = claim(RANGE);

        loads.fail(RANGE, owner, failure);

        assertThatThrownBy(() -> awaited.await(0))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isSameAs(failure);
        assertThat(loads.size()).isZero();
        assertFreeToClaim(RANGE);
    }

    static Stream<Arguments> failuresOfAFetch() {
        IOException connectionReset = new IOException("connection reset");
        return Stream.of(
                Arguments.argumentSet("a storage exception", new StorageException("backend down")),
                Arguments.argumentSet("a cancellation", new CancellationException("fetch cancelled")),
                Arguments.argumentSet("a completion exception with a cause", new CompletionException(connectionReset)),
                Arguments.argumentSet("an error", new SimulatedError()));
    }

    // the spy claims the range from inside fail, as the retry of a woken waiter would
    @Test
    void rangeFreedBeforeWaitersFail() {
        PendingLoad failing = spy(new PendingLoad());
        PendingLoad retry = new PendingLoad();
        AtomicReference<PendingLoad> fetchingForTheRetry = new AtomicReference<>();
        Answer<Void> failingThenRetryingAtOnce = failingTheLoad -> {
            failingTheLoad.callRealMethod();
            PendingLoad fetching = loads.claim(RANGE, retry);
            fetchingForTheRetry.set(fetching);
            return null;
        };
        doAnswer(failingThenRetryingAtOnce).when(failing).fail(any());
        loads.claim(RANGE, failing);

        loads.fail(RANGE, failing, new StorageException("backend down"));

        assertThat(fetchingForTheRetry).hasValue(retry);
    }

    @Test
    void failingNonEnteredKeepsHolder() {
        PendingLoad holder = claim(RANGE);
        PendingLoad neverEntered = new PendingLoad();
        loads.claim(RANGE, neverEntered);
        StorageException failure = new StorageException("backend down");

        loads.fail(RANGE, neverEntered, failure);

        assertThatThrownBy(() -> neverEntered.await(0))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isSameAs(failure);
        assertThat(holder.isDone()).isFalse();
        assertThat(claim(RANGE)).isSameAs(holder);
    }

    @Test
    void valueNowAfterCompletion() {
        PendingLoad running = new PendingLoad();
        PendingLoad completed = new PendingLoad();
        PendingLoad failed = new PendingLoad();
        ByteBuffer value = ByteBuffer.allocate(10).asReadOnlyBuffer();

        completed.complete(value);
        failed.fail(new StorageException("backend down"));

        assertThat(running.valueNow()).isNull();
        assertThat(completed.valueNow()).isSameAs(value);
        assertThat(failed.valueNow()).isNull();
    }

    @Test
    void aRunningLoadTimesOutItsWaiter() {
        PendingLoad running = claim(RANGE);

        assertThat(running.isDone()).isFalse();
        assertThatThrownBy(() -> running.await(0)).isInstanceOf(TimeoutException.class);
    }

    /** Claims {@code range} with a new load and returns the load fetching it: the new one if the range was free. */
    private PendingLoad claim(ByteRange range) {
        PendingLoad candidate = new PendingLoad();
        return loads.claim(range, candidate);
    }

    private void assertFreeToClaim(ByteRange range) {
        PendingLoad candidate = new PendingLoad();
        PendingLoad fetching = loads.claim(range, candidate);
        assertThat(fetching).isSameAs(candidate);
    }

    private static final class SimulatedError extends Error {
        private static final long serialVersionUID = 1L;
    }
}
