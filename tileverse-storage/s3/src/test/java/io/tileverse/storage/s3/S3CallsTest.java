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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3CallsTest {

    private static final String KEY = "tiles/file.pmtiles";

    private final ExecutorService caller = Executors.newSingleThreadExecutor();

    @AfterEach
    void stopCaller() {
        caller.shutdownNow();
    }

    @Test
    void aCompletedCallReturnsItsValue() {
        String value = S3Calls.await(KEY, () -> CompletableFuture.completedFuture("value"));

        assertThat(value).isEqualTo("value");
    }

    @Test
    void aStorageExceptionPassesThroughUnchanged() {
        StorageException raised = new StorageException("Server returned more data than requested (100 bytes)");

        assertThatThrownBy(() -> S3Calls.await(KEY, () -> CompletableFuture.failedFuture(raised)))
                .isSameAs(raised);
    }

    @Test
    void aStorageExceptionWrappedByTheSdkPassesThroughUnchanged() {
        StorageException raised = new StorageException("Server returned more data than requested (100 bytes)");
        SdkClientException wrapped = SdkClientException.create("transform failed", raised);

        assertThatThrownBy(() -> S3Calls.await(KEY, () -> CompletableFuture.failedFuture(wrapped)))
                .isSameAs(raised);
    }

    @ParameterizedTest
    @MethodSource("serviceFailures")
    void aServiceFailureMapsByItsStatus(S3Exception failure, Class<? extends StorageException> expected) {
        assertThatThrownBy(() -> S3Calls.await(KEY, () -> CompletableFuture.failedFuture(failure)))
                .isExactlyInstanceOf(expected)
                .hasMessageContaining(KEY)
                .cause()
                .isSameAs(failure);
    }

    static Stream<Arguments> serviceFailures() {
        return Stream.of(
                Arguments.of(NoSuchKeyException.builder().message("missing").build(), NotFoundException.class),
                Arguments.of(serviceFailure(403), AccessDeniedException.class),
                Arguments.of(serviceFailure(412), PreconditionFailedException.class),
                Arguments.of(serviceFailure(503), TransientStorageException.class));
    }

    @Test
    void aClientSideFailureBecomesAStorageExceptionNamingTheKey() {
        SdkClientException refused = SdkClientException.create("connection refused");

        assertThatThrownBy(() -> S3Calls.await(KEY, () -> CompletableFuture.failedFuture(refused)))
                .isExactlyInstanceOf(StorageException.class)
                .hasMessageContaining(KEY)
                .cause()
                .isSameAs(refused);
    }

    @Test
    void aCallFailingToStartMapsTheSameWay() {
        SdkClientException invalid = SdkClientException.create("invalid bucket name");
        Supplier<CompletableFuture<String>> failingToStart = () -> {
            throw invalid;
        };

        assertThatThrownBy(() -> S3Calls.await(KEY, failingToStart))
                .isExactlyInstanceOf(StorageException.class)
                .cause()
                .isSameAs(invalid);
    }

    @Test
    void anErrorIsRethrownAsItself() {
        OutOfMemoryError heapExhausted = new OutOfMemoryError("Java heap space");

        assertThatThrownBy(() -> S3Calls.await(KEY, () -> CompletableFuture.failedFuture(heapExhausted)))
                .isSameAs(heapExhausted);
    }

    @Test
    void aCallCancelledElsewhereFails() {
        CompletableFuture<String> cancelled = new CompletableFuture<>();
        cancelled.cancel(true);

        assertThatThrownBy(() -> S3Calls.await(KEY, () -> cancelled)).isExactlyInstanceOf(StorageException.class);
    }

    @Test
    void aThreadAlreadyInterruptedSendsNothing() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        Future<Outcome> outcome = caller.submit(() -> {
            Thread.currentThread().interrupt();
            return Outcome.of(() -> S3Calls.await(KEY, () -> {
                calls.incrementAndGet();
                return CompletableFuture.completedFuture("sent");
            }));
        });

        Outcome result = outcome.get(5, TimeUnit.SECONDS);
        assertThat(calls).hasValue(0);
        assertThat(result.failure())
                .isInstanceOf(StorageException.class)
                .hasCauseInstanceOf(InterruptedIOException.class);
        assertThat(result.interruptedAfterwards()).isTrue();
    }

    @Test
    void anInterruptCancelsTheCallAndKeepsTheFlagSet() throws Exception {
        CompletableFuture<String> neverAnswered = new CompletableFuture<>();

        Outcome result = interruptWhileWaiting(neverAnswered, late -> {});

        assertThat(result.failure())
                .isInstanceOf(StorageException.class)
                .hasCauseInstanceOf(InterruptedIOException.class);
        assertThat(result.interruptedAfterwards()).isTrue();
        assertThat(neverAnswered).isCancelled();
    }

    @Test
    void aValueCompletingDespiteTheCancelIsDiscarded() throws Exception {
        CompletableFuture<String> answeredAtTheCancel = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                complete("late stream");
                return false;
            }
        };
        List<String> discarded = new CopyOnWriteArrayList<>();

        Outcome result = interruptWhileWaiting(answeredAtTheCancel, discarded::add);

        assertThat(result.failure()).hasCauseInstanceOf(InterruptedIOException.class);
        assertThat(discarded).containsExactly("late stream");
    }

    /** Waits on {@code pending} from the caller thread and interrupts that thread once the call went out. */
    private Outcome interruptWhileWaiting(CompletableFuture<String> pending, Consumer<String> discardLate)
            throws Exception {
        AtomicReference<Thread> waiting = new AtomicReference<>();
        CountDownLatch sent = new CountDownLatch(1);
        Future<Outcome> outcome = caller.submit(() -> {
            waiting.set(Thread.currentThread());
            return Outcome.of(() -> S3Calls.await(
                    KEY,
                    () -> {
                        sent.countDown();
                        return pending;
                    },
                    discardLate));
        });
        assertThat(sent.await(5, TimeUnit.SECONDS)).isTrue();
        waiting.get().interrupt();
        return outcome.get(5, TimeUnit.SECONDS);
    }

    private static S3Exception serviceFailure(int status) {
        // S3Exception.Builder inherits build() from AwsServiceException.Builder: the static type needs the cast
        return (S3Exception) S3Exception.builder()
                .statusCode(status)
                .message("status " + status)
                .build();
    }

    /** How a call made on the caller thread ended, and whether that thread was still interrupted afterwards. */
    private record Outcome(Throwable failure, boolean interruptedAfterwards) {

        static Outcome of(Runnable call) {
            Throwable failure = null;
            try {
                call.run();
            } catch (RuntimeException thrown) {
                failure = thrown;
            }
            boolean interrupted = Thread.currentThread().isInterrupted();
            return new Outcome(failure, interrupted);
        }
    }
}
