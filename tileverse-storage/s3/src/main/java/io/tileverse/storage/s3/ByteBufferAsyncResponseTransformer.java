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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
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
 * from landing after a partial attempt. A body longer than the requested count, or a destination refusing a chunk,
 * cancels the stream and fails the attempt with a {@link StorageException}.
 *
 * <p>The SDK may call {@code prepare}, {@code onResponse}, {@code onStream}, and {@code exceptionOccurred} from
 * different threads, its timeout scheduler included; the volatile fields publish {@code attempt} and {@code response}
 * across that handoff. One lock serializes the chunk copies with {@code exceptionOccurred} and {@link #close()}. A
 * failure never ends an attempt during a copy, and {@code close()} never returns during one. No copy starts after its
 * attempt ended, a retry replaced it, or the transformer closed. An attempt failed through {@code exceptionOccurred}
 * never completes normally. The SDK's copy of an error already delivered to a chunk writer never marks another attempt
 * failed.
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

    /** Copies one chunk into the destination; tests replace it to hold a copy open. */
    @FunctionalInterface
    interface ChunkCopier {
        void copy(ByteBuffer destination, int index, ByteBuffer chunk, int length);
    }

    /** Completes an attempt already marked failed; tests replace it to hold the completion back. */
    @FunctionalInterface
    interface AttemptFailer {
        void fail(CompletableFuture<Result> attempt, Throwable error);
    }

    static final ChunkCopier ABSOLUTE_PUT =
            (destination, index, chunk, length) -> destination.put(index, chunk, chunk.position(), length);

    private static final AttemptFailer COMPLETE_EXCEPTIONALLY = CompletableFuture::completeExceptionally;

    private final ByteBuffer destination;
    private final int start;
    private final int maxBytes;
    private final ChunkCopier copier;
    private final AttemptFailer failer;

    private final Object lock = new Object();

    private boolean closed; // guarded by lock

    private final Set<Throwable> errorsDeliveredToWriters =
            Collections.newSetFromMap(new IdentityHashMap<>()); // guarded by lock

    @Nullable
    private CompletableFuture<Result> failedAttempt; // guarded by lock

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
        this(destination, maxBytes, ABSOLUTE_PUT, COMPLETE_EXCEPTIONALLY);
    }

    /** Creates a transformer copying chunks through {@code copier} and failing attempts through {@code failer}. */
    ByteBufferAsyncResponseTransformer(ByteBuffer destination, int maxBytes, ChunkCopier copier, AttemptFailer failer) {
        if (maxBytes < 0 || maxBytes > destination.remaining()) {
            throw new IllegalArgumentException(
                    "maxBytes " + maxBytes + " outside the destination's remaining " + destination.remaining());
        }
        this.destination = destination;
        this.start = destination.position();
        this.maxBytes = maxBytes;
        this.copier = copier;
        this.failer = failer;
    }

    /**
     * Ignores every later chunk and returns once a copy in progress has finished. Callers close before handing the
     * destination on: the writer of an abandoned attempt can outlive the request.
     */
    void close() {
        synchronized (lock) {
            closed = true;
        }
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

    /**
     * Marks the current attempt failed, then fails it outside the lock: its dependents must not run under it. Ignores
     * an error already delivered to a chunk writer: that writer failed its own attempt with it, and the SDK forwards
     * the same error here, possibly after a retry replaced that attempt.
     */
    @Override
    public void exceptionOccurred(Throwable error) {
        CompletableFuture<Result> failing;
        synchronized (lock) {
            if (errorsDeliveredToWriters.remove(error)) {
                return;
            }
            failing = attempt;
            failedAttempt = failing;
        }
        failer.fail(failing, error);
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
            StorageException rejection = copyWhileAttemptIsLive(chunk);
            if (rejection != null) {
                subscription.cancel();
                outcome.completeExceptionally(rejection);
            }
        }

        /**
         * Copies the chunk only while the transformer is open and the attempt is live. After that, the destination may
         * belong to someone else.
         *
         * @return why the chunk was rejected, or {@code null} when it was copied or ignored
         */
        @Nullable
        private StorageException copyWhileAttemptIsLive(ByteBuffer chunk) {
            synchronized (lock) {
                if (closed || attemptIsOver()) {
                    return null;
                }
                int length = chunk.remaining();
                if (length > maxBytes - written) {
                    return new StorageException("Server returned more data than requested (" + maxBytes + " bytes)");
                }
                try {
                    copier.copy(destination, start + written, chunk, length);
                } catch (RuntimeException refused) {
                    return new StorageException("Target buffer refused the write: " + refused, refused);
                }
                written += length;
                return null;
            }
        }

        private boolean attemptIsOver() {
            // must be called under the lock
            return outcome != attempt || outcome == failedAttempt || outcome.isDone();
        }

        /** Records the error before failing the attempt: failing it can start a retry on another thread. */
        @Override
        public void onError(Throwable error) {
            recordDelivery(error);
            outcome.completeExceptionally(error);
        }

        private void recordDelivery(Throwable error) {
            synchronized (lock) {
                errorsDeliveredToWriters.add(error);
            }
        }

        /**
         * Leaves an attempt marked failed to {@code exceptionOccurred}: the chunks after the mark were dropped, and the
         * count would read like a short body.
         */
        @Override
        public void onComplete() {
            if (!markedFailed()) {
                outcome.complete(new Result(response, written));
            }
        }

        private boolean markedFailed() {
            synchronized (lock) {
                return outcome == failedAttempt;
            }
        }
    }
}
