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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.StorageException;
import io.tileverse.storage.s3.ByteBufferAsyncResponseTransformer.Result;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    void rejectsACountOutsideTheDestinationCapacity() {
        ByteBuffer destination = ByteBuffer.allocate(8);

        assertThatThrownBy(() -> new ByteBufferAsyncResponseTransformer(destination, 9))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ByteBufferAsyncResponseTransformer(destination, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
