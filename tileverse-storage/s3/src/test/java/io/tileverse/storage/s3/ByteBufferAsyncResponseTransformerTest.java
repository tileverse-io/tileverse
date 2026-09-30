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
package io.tileverse.storage.s3;

import static io.tileverse.storage.s3.ByteBufferAsyncResponseTransformer.ABSOLUTE_PUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.tileverse.storage.StorageException;
import io.tileverse.storage.s3.ByteBufferAsyncResponseTransformer.AttemptFailer;
import io.tileverse.storage.s3.ByteBufferAsyncResponseTransformer.ChunkCopier;
import io.tileverse.storage.s3.ByteBufferAsyncResponseTransformer.Result;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Unit tests for {@link ByteBufferAsyncResponseTransformer}, driven the way the SDK drives one attempt at a time. */
class ByteBufferAsyncResponseTransformerTest {

    private static final GetObjectResponse RESPONSE =
            GetObjectResponse.builder().contentRange("bytes 0-15/100").build();

    private static final Set<Thread.State> BLOCKED_OR_FINISHED =
            EnumSet.of(Thread.State.BLOCKED, Thread.State.TERMINATED);

    /** Records cancellation and leaves emission to the test. */
    private static final class ManualSubscription implements Subscription {
        private boolean cancelled;

        @Override
        public void request(long demand) {
            // the test emits explicitly
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    private final ManualSubscription subscription = new ManualSubscription();
    private final AtomicReference<Subscriber<? super ByteBuffer>> subscriber = new AtomicReference<>();
    private final SdkPublisher<ByteBuffer> publisher = s -> {
        subscriber.set(s);
        s.onSubscribe(subscription);
    };

    private static byte[] bytes(int count) {
        byte[] out = new byte[count];
        for (int i = 0; i < count; i++) {
            out[i] = (byte) (i * 7 + 1);
        }
        return out;
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    private CompletableFuture<Result> startAttempt(ByteBufferAsyncResponseTransformer transformer) {
        CompletableFuture<Result> outcome = transformer.prepare();
        transformer.onResponse(RESPONSE);
        transformer.onStream(publisher);
        return outcome;
    }

    /** Emits {@code length} bytes of {@code body} from {@code from} as a chunk whose position is not 0. */
    private void emit(byte[] body, int from, int length) {
        subscriber.get().onNext(ByteBuffer.wrap(body, from, length));
    }

    private static Thread startThread(String name, Runnable action) {
        Thread thread = new Thread(action, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /** Waits until {@code thread} blocks on a lock or runs to its end. */
    private static void awaitBlockedOrFinished(Thread thread) {
        await().atMost(Duration.ofSeconds(5)).until(() -> BLOCKED_OR_FINISHED.contains(thread.getState()));
    }

    private static void joinAll(Thread... threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join(Duration.ofSeconds(5).toMillis());
            assertThat(thread.isAlive())
                    .as("%s still running", thread.getName())
                    .isFalse();
        }
    }

    /** Holds the first copy open until released; later copies go straight through. */
    private static final class HeldCopy implements ChunkCopier {
        private final Hold hold = new Hold();
        private final AtomicBoolean held = new AtomicBoolean();

        @Override
        public void copy(ByteBuffer destination, int index, ByteBuffer chunk, int length) {
            if (held.compareAndSet(false, true)) {
                hold.reach();
            }
            destination.put(index, chunk, chunk.position(), length);
        }

        void awaitStarted() throws InterruptedException {
            hold.awaitReached();
        }

        void release() {
            hold.release();
        }
    }

    /** Holds back the completion of an attempt marked failed until released. */
    private static final class HeldFailure implements AttemptFailer {
        private final Hold hold = new Hold();

        @Override
        public void fail(CompletableFuture<Result> attempt, Throwable error) {
            hold.reach();
            attempt.completeExceptionally(error);
        }

        void awaitMarked() throws InterruptedException {
            hold.awaitReached();
        }

        void release() {
            hold.release();
        }
    }

    /** Stops the thread reaching it until released. */
    private static final class Hold {
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void reach() {
            reached.countDown();
            try {
                if (!released.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the hold was never released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        }

        void awaitReached() throws InterruptedException {
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
        }

        void release() {
            released.countDown();
        }
    }

    @Test
    void chunksLandAtAbsolutePositionsWithoutMovingTheDestination() {
        ByteBuffer destination = ByteBuffer.allocateDirect(64);
        destination.position(8);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(16);

        CompletableFuture<Result> outcome = startAttempt(transformer);
        emit(body, 0, 5);
        emit(body, 5, 11);
        subscriber.get().onComplete();

        Result result = outcome.join();
        assertThat(result.response()).isSameAs(RESPONSE);
        assertThat(result.bytesWritten()).isEqualTo(16);
        assertThat(destination.position()).isEqualTo(8);
        assertThat(destination.limit()).isEqualTo(64);
        assertThat(contents(destination, 8, 16)).containsExactly(body);
    }

    @Test
    void aShortBodyCompletesWithTheShortCount() {
        ByteBuffer destination = ByteBuffer.allocate(64);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(10);

        CompletableFuture<Result> outcome = startAttempt(transformer);
        emit(body, 0, 10);
        subscriber.get().onComplete();

        assertThat(outcome.join().bytesWritten()).isEqualTo(10);
        assertThat(contents(destination, 0, 10)).containsExactly(body);
    }

    @Test
    void eachAttemptRestartsAtTheInitialPosition() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        destination.position(4);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(16);

        CompletableFuture<Result> first = startAttempt(transformer);
        emit(body, 0, 6);
        subscriber.get().onError(new IOException("connection reset"));
        assertThat(first).isCompletedExceptionally();

        CompletableFuture<Result> second = startAttempt(transformer);
        assertThat(second).isNotSameAs(first);
        emit(body, 0, 16);
        subscriber.get().onComplete();

        assertThat(second.join().bytesWritten()).isEqualTo(16);
        assertThat(contents(destination, 4, 16)).containsExactly(body);
        assertThat(destination.position()).isEqualTo(4);
    }

    @Test
    void aBodyLongerThanRequestedCancelsTheStreamAndFailsTheAttempt() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(20);

        CompletableFuture<Result> outcome = startAttempt(transformer);
        emit(body, 0, 10);
        emit(body, 10, 10);

        assertThat(subscription.cancelled).isTrue();
        assertThatThrownBy(outcome::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
    }

    @Test
    void aDestinationRefusingTheWriteCancelsTheStreamAndFailsTheAttempt() {
        ByteBuffer destination = ByteBuffer.allocate(32).asReadOnlyBuffer();
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);

        CompletableFuture<Result> outcome = startAttempt(transformer);
        emit(bytes(16), 0, 8);

        assertThat(subscription.cancelled).isTrue();
        assertThatThrownBy(outcome::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(StorageException.class)
                .hasMessageContaining("refused the write");
    }

    @Test
    void chunksAfterTheCancellingOneAreIgnored() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(23);

        CompletableFuture<Result> outcome = startAttempt(transformer);
        emit(body, 0, 10);
        emit(body, 10, 10);
        emit(body, 20, 3);

        assertThat(subscription.cancelled).isTrue();
        assertThatThrownBy(outcome::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(StorageException.class)
                .hasMessageContaining("more data than requested");
        assertThat(contents(destination, 10, 22)).containsOnly((byte) 0);
    }

    @Test
    void aRequestFailureFailsTheAttempt() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        IOException failure = new IOException("boom");

        CompletableFuture<Result> outcome = transformer.prepare();
        transformer.exceptionOccurred(failure);

        assertThatThrownBy(outcome::join).hasCause(failure);
    }

    @Test
    void aFailureDuringACopyCompletesTheAttemptOnlyAfterTheCopy() throws InterruptedException {
        ByteBuffer destination = ByteBuffer.allocate(32);
        HeldCopy heldCopy = new HeldCopy();
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(
                destination, 16, heldCopy, CompletableFuture::completeExceptionally);
        IOException timeout = new IOException("attempt timed out");

        CompletableFuture<Result> outcome = startAttempt(transformer);
        Thread copying = startThread("copying", () -> emit(bytes(16), 0, 8));
        heldCopy.awaitStarted();
        Thread failing;
        try {
            failing = startThread("failing", () -> transformer.exceptionOccurred(timeout));
            awaitBlockedOrFinished(failing);

            assertThat(outcome).isNotDone();
        } finally {
            heldCopy.release();
        }
        joinAll(copying, failing);
        assertThatThrownBy(outcome::join).hasCause(timeout);
    }

    /**
     * A failure marks its attempt before completing it. A body ending in between must not complete the attempt: the
     * chunks after the mark were dropped, and the count would read like a short body.
     */
    @Test
    void anAttemptMarkedFailedNeverCompletesWithAShortCount() throws InterruptedException {
        ByteBuffer destination = ByteBuffer.allocate(32);
        HeldFailure heldFailure = new HeldFailure();
        ByteBufferAsyncResponseTransformer transformer =
                new ByteBufferAsyncResponseTransformer(destination, 16, ABSOLUTE_PUT, heldFailure);
        byte[] body = bytes(16);
        IOException lateError = new IOException("error of a cancelled attempt");

        CompletableFuture<Result> outcome = startAttempt(transformer);
        emit(body, 0, 8);
        Thread failing = startThread("failing", () -> transformer.exceptionOccurred(lateError));
        try {
            heldFailure.awaitMarked();
            emit(body, 8, 8);
            subscriber.get().onComplete();

            assertThat(outcome).as("completed before its failure").isNotDone();
        } finally {
            heldFailure.release();
        }
        joinAll(failing);
        assertThatThrownBy(outcome::join).hasCause(lateError);
    }

    @Test
    void closeReturnsOnlyAfterTheCopyInProgressAndLaterChunksWriteNothing() throws InterruptedException {
        ByteBuffer destination = ByteBuffer.allocate(32);
        HeldCopy heldCopy = new HeldCopy();
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(
                destination, 16, heldCopy, CompletableFuture::completeExceptionally);
        byte[] body = bytes(16);

        startAttempt(transformer);
        Thread copying = startThread("copying", () -> emit(body, 0, 8));
        heldCopy.awaitStarted();
        Thread closing;
        try {
            closing = startThread("closing", transformer::close);
            awaitBlockedOrFinished(closing);

            assertThat(closing.isAlive())
                    .as("close() returned while a copy was in progress")
                    .isTrue();
        } finally {
            heldCopy.release();
        }
        joinAll(copying, closing);
        emit(body, 8, 8);
        assertThat(contents(destination, 0, 8)).containsExactly(Arrays.copyOf(body, 8));
        assertThat(contents(destination, 8, 8)).containsOnly((byte) 0);
    }

    @Test
    void chunksOfASupersededAttemptWriteNothing() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(16);
        byte[] late = new byte[16];
        Arrays.fill(late, (byte) -1);

        startAttempt(transformer);
        Subscriber<? super ByteBuffer> timedOut = subscriber.get();
        CompletableFuture<Result> retry = startAttempt(transformer);
        emit(body, 0, 16);
        subscriber.get().onComplete();
        timedOut.onNext(ByteBuffer.wrap(late));

        assertThat(retry.join().bytesWritten()).isEqualTo(16);
        assertThat(contents(destination, 0, 16)).containsExactly(body);
    }

    /**
     * The SDK forwards the late failure of an abandoned attempt to {@code exceptionOccurred} while its retry runs. The
     * failure reaches the abandoned attempt's writer first and must not fail the retry.
     */
    @Test
    void theLateFailureOfAnAbandonedAttemptLeavesTheRetryRunning() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(16);
        IOException timeout = new IOException("attempt timed out");
        IOException cancelled = new IOException("stream of the timed-out attempt cancelled");

        startAttempt(transformer);
        Subscriber<? super ByteBuffer> timedOut = subscriber.get();
        transformer.exceptionOccurred(timeout);
        CompletableFuture<Result> retry = startAttempt(transformer);
        emit(body, 0, 8);
        timedOut.onError(cancelled);
        transformer.exceptionOccurred(cancelled);
        emit(body, 8, 8);
        subscriber.get().onComplete();

        assertThat(retry.join().bytesWritten()).isEqualTo(16);
        assertThat(contents(destination, 0, 16)).containsExactly(body);
    }

    /**
     * A writer can see the failure of its attempt before the SDK marks that attempt failed and starts the retry. The
     * copy of that failure forwarded by the SDK afterwards must not fail the retry.
     */
    @Test
    void aFailureSeenByTheWriterBeforeTheRetryLeavesTheRetryRunning() {
        ByteBuffer destination = ByteBuffer.allocate(32);
        ByteBufferAsyncResponseTransformer transformer = new ByteBufferAsyncResponseTransformer(destination, 16);
        byte[] body = bytes(16);
        IOException timeout = new IOException("attempt timed out");
        IOException cancelled = new IOException("stream of the timed-out attempt cancelled");

        startAttempt(transformer);
        subscriber.get().onError(cancelled);
        transformer.exceptionOccurred(timeout);
        CompletableFuture<Result> retry = startAttempt(transformer);
        emit(body, 0, 16);
        transformer.exceptionOccurred(cancelled);
        subscriber.get().onComplete();

        assertThat(retry.join().bytesWritten()).isEqualTo(16);
        assertThat(contents(destination, 0, 16)).containsExactly(body);
    }

    @Test
    void rejectsACountOutsideTheDestinationCapacity() {
        ByteBuffer destination = ByteBuffer.allocate(8);

        assertThatThrownBy(() -> new ByteBufferAsyncResponseTransformer(destination, 9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ByteBufferAsyncResponseTransformer(destination, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
