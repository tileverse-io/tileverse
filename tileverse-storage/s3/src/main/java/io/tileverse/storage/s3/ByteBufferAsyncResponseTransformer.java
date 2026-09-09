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

import io.tileverse.storage.StorageException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Streams an async GET body into a destination buffer as its chunks arrive, inside the SDK's retry loop.
 *
 * <p>Chunks land at absolute positions from the destination position captured at construction; the destination's
 * position and limit never move, and the caller advances the position from the completed {@link Result}. The SDK calls
 * {@link #prepare()} once per request attempt and subscribes a fresh chunk writer for each, which keeps a retried body
 * from landing after a partial attempt. A body longer than the requested count cancels the stream and fails the attempt
 * with a {@link StorageException}.
 *
 * <p>The SDK may call {@code prepare}, {@code onResponse}, {@code onStream}, and {@code exceptionOccurred} from
 * different threads; the volatile fields publish {@code attempt} and {@code response} across that handoff. Within one
 * attempt, the chunk writer's own state relies on the SDK's guarantee that it serializes every callback, the
 * subscriber's included.
 */
final class ByteBufferAsyncResponseTransformer
        implements AsyncResponseTransformer<GetObjectResponse, ByteBufferAsyncResponseTransformer.Result> {

    /**
     * What a completed attempt delivered.
     *
     * @param response the GET response metadata
     * @param bytesWritten how many body bytes landed in the destination
     */
    record Result(GetObjectResponse response, int bytesWritten) {}

    private final ByteBuffer destination;
    private final int start;
    private final int maxBytes;

    @SuppressWarnings("java:S3077") // CompletableFuture is thread-safe; volatile publishes each fresh reference
    private volatile CompletableFuture<Result> attempt = new CompletableFuture<>();

    @Nullable
    @SuppressWarnings("java:S3077") // GetObjectResponse is immutable; volatile publishes each fresh reference
    private volatile GetObjectResponse response;

    /**
     * Creates a transformer writing into {@code destination} from its current position.
     *
     * @param destination the buffer where the body lands, heap or direct
     * @param maxBytes the requested range length; must not exceed the destination's remaining capacity
     * @throws IllegalArgumentException if {@code maxBytes} is negative or exceeds the remaining capacity
     */
    ByteBufferAsyncResponseTransformer(ByteBuffer destination, int maxBytes) {
        if (maxBytes < 0 || maxBytes > destination.remaining()) {
            throw new IllegalArgumentException(
                    "maxBytes " + maxBytes + " outside the destination's remaining " + destination.remaining());
        }
        this.destination = destination;
        this.start = destination.position();
        this.maxBytes = maxBytes;
    }

    @Override
    public CompletableFuture<Result> prepare() {
        attempt = new CompletableFuture<>();
        return attempt;
    }

    @Override
    public void onResponse(GetObjectResponse response) {
        this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(new ChunkWriter(attempt));
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        attempt.completeExceptionally(error);
    }

    /** Writes one attempt's chunks at the running offset and completes that attempt's future. */
    private final class ChunkWriter implements Subscriber<ByteBuffer> {

        private final CompletableFuture<Result> outcome;

        @Nullable
        private Subscription subscription;

        private int written;

        ChunkWriter(CompletableFuture<Result> outcome) {
            this.outcome = outcome;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (this.subscription != null) {
                subscription.cancel();
                return;
            }
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer chunk) {
            // A chunk arriving after this attempt finished could write into a destination already
            // back in the pool.
            if (outcome.isDone()) {
                return;
            }
            int length = chunk.remaining();
            if (length > maxBytes - written) {
                subscription.cancel();
                outcome.completeExceptionally(
                        new StorageException("Server returned more data than requested (" + maxBytes + " bytes)"));
                return;
            }
            destination.put(start + written, chunk, chunk.position(), length);
            written += length;
        }

        @Override
        public void onError(Throwable error) {
            outcome.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            outcome.complete(new Result(response, written));
        }
    }
}
