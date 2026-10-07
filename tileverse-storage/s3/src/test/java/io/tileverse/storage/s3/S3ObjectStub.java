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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Serves a fixed byte array through a mocked async S3 client as the SDK does: it drives the caller's
 * {@link AsyncResponseTransformer} through {@code prepare}, {@code onResponse}, and {@code onStream} with a publisher
 * emitting the body in chunks of a configured size, each a view of the body array at its own offset. A body failing
 * mid-stream fails its attempt with a retryable error, and the stub starts a fresh attempt with a new
 * {@code prepare()}, like the SDK's retry stage, up to its attempt limit. A request starting past the end fails with a
 * 416; a range running past the end is truncated as a real object store answers it; a request without a range gets the
 * whole object. In {@link #holdingAsyncResponses holding} mode the stub keeps each response back until the test
 * {@link #releaseOne releases} or {@link #failOne fails} it, which is how a test observes how many fetches a reader
 * keeps in flight.
 */
final class S3ObjectStub {

    /** The number of attempts allowed per request by the SDK's default retry mode. */
    private static final int MAX_ATTEMPTS = 3;

    private final byte[] data;
    private final int chunkSize;
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicInteger cancellations = new AtomicInteger();
    private final AtomicInteger asyncCalls = new AtomicInteger();
    private final AtomicInteger asyncInFlight = new AtomicInteger();
    private final AtomicInteger asyncPeakInFlight = new AtomicInteger();
    private final Deque<HeldResponse> held = new ArrayDeque<>();
    private int extraBytes;
    private int bodyFailuresLeft; // guarded by this
    private boolean holdingAsyncResponses;
    private int throwingAsyncCall;
    private Error asyncCallError;

    /** An attempt whose response the stub keeps back until the test releases or fails it. */
    private record HeldResponse(
            GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, Object> transformer) {}

    S3ObjectStub(byte[] data, int chunkSize) {
        this.data = data;
        this.chunkSize = chunkSize;
    }

    /** Lengthens every body by {@code count} bytes past the requested range, like a server ignoring the range. */
    S3ObjectStub respondingWithExtraBytes(int count) {
        this.extraBytes = count;
        return this;
    }

    /** Makes the next {@code count} bodies fail after their first chunk with a retryable error, like a dropped link. */
    synchronized S3ObjectStub failingBodyReads(int count) {
        this.bodyFailuresLeft = count;
        return this;
    }

    /** Keeps every async response back until {@link #releaseOne()} or {@link #failOne} lets it through. */
    S3ObjectStub holdingAsyncResponses() {
        this.holdingAsyncResponses = true;
        return this;
    }

    /** Makes async {@code getObject} call number {@code call} throw {@code error} instead of answering. */
    S3ObjectStub throwingOnAsyncCall(int call, Error error) {
        this.throwingAsyncCall = call;
        this.asyncCallError = error;
        return this;
    }

    /** The number of attempts driven by the stub, retries included. */
    int attempts() {
        return attempts.get();
    }

    /** The number of bodies cancelled by their consumer before their end. */
    int cancellations() {
        return cancellations.get();
    }

    /** The largest number of async requests outstanding at once, from {@code getObject} to its future's completion. */
    int asyncPeakInFlight() {
        return asyncPeakInFlight.get();
    }

    /** How many async responses the stub is holding back right now. */
    synchronized int heldResponses() {
        return held.size();
    }

    /** Lets the oldest held response through, serving its body. */
    void releaseOne() {
        HeldResponse next = takeHeld();
        serveHeld(next.request(), next.transformer());
    }

    /** Lets every held response through, in order. */
    void releaseAll() {
        while (heldResponses() > 0) {
            releaseOne();
        }
    }

    /** Fails the oldest held response with the given exception. */
    void failOne(Throwable failure) {
        HeldResponse next = takeHeld();
        next.transformer().exceptionOccurred(failure);
    }

    private synchronized HeldResponse takeHeld() {
        HeldResponse next = held.pollFirst();
        if (next == null) {
            throw new IllegalStateException("no async response is being held");
        }
        return next;
    }

    /** Stubs {@code getObject(request, transformer)} for every range of the object. */
    @SuppressWarnings("unchecked")
    void installAsync(S3AsyncClient client) {
        lenient()
                .when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenAnswer(invocation -> serveAsync(invocation.getArgument(0), invocation.getArgument(1)));
    }

    private CompletableFuture<Object> serveAsync(
            GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, Object> transformer) {
        if (asyncCalls.incrementAndGet() == throwingAsyncCall) {
            throw asyncCallError;
        }
        int outstanding = asyncInFlight.incrementAndGet();
        asyncPeakInFlight.accumulateAndGet(outstanding, Math::max);
        CompletableFuture<Object> call = new CompletableFuture<>();
        // The caller chains on the returned future; completing it after the decrement keeps the count exact
        // whatever the caller does on completion.
        CompletableFuture<Object> outcome = call.whenComplete((ignored, failure) -> asyncInFlight.decrementAndGet());
        startAttempt(request, transformer, call, 1);
        return outcome;
    }

    /** Runs one attempt as the SDK's retry stage does: a fresh {@code prepare()}, then the response or a retry. */
    private void startAttempt(
            GetObjectRequest request,
            AsyncResponseTransformer<GetObjectResponse, Object> transformer,
            CompletableFuture<Object> call,
            int attempt) {
        attempts.incrementAndGet();
        transformer.prepare().whenComplete((value, failure) -> {
            if (failure == null) {
                call.complete(value);
            } else if (retryable(failure) && attempt < MAX_ATTEMPTS) {
                startAttempt(request, transformer, call, attempt + 1);
            } else {
                call.completeExceptionally(failure);
            }
        });
        if (holdingAsyncResponses) {
            synchronized (this) {
                held.addLast(new HeldResponse(request, transformer));
            }
            return;
        }
        serveHeld(request, transformer);
    }

    private static boolean retryable(Throwable failure) {
        Throwable cause =
                failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
        return cause instanceof RetryableException;
    }

    private void serveHeld(GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, Object> transformer) {
        long[] bounds = requestedRange(request);
        if (bounds[0] >= data.length) {
            transformer.exceptionOccurred(rangeNotSatisfiable());
            return;
        }
        byte[] body = bodyFor(bounds);
        transformer.onResponse(responseFor(bounds[0], body.length - extraBytes));
        transformer.onStream(chunkedPublisher(body, takeBodyFailure()));
    }

    private synchronized boolean takeBodyFailure() {
        if (bodyFailuresLeft == 0) {
            return false;
        }
        bodyFailuresLeft--;
        return true;
    }

    private SdkPublisher<ByteBuffer> chunkedPublisher(byte[] body, boolean failAfterFirstChunk) {
        return subscriber -> subscriber.onSubscribe(new ChunkedSubscription(subscriber, body, failAfterFirstChunk));
    }

    /**
     * Parses the request's {@code bytes=first-last} header into {@code {first, last}}. An open end, or no header at
     * all, reads to the end of the object.
     */
    static long[] requestedRange(GetObjectRequest request) {
        String range = request.range();
        if (range == null) {
            return new long[] {0, Long.MAX_VALUE - 1};
        }
        String[] bounds = range.replace("bytes=", "").split("-", -1);
        long first = Long.parseLong(bounds[0]);
        long last = bounds[1].isEmpty() ? Long.MAX_VALUE - 1 : Long.parseLong(bounds[1]);
        return new long[] {first, last};
    }

    /** The bytes answered by a real store for the range: truncated at the object's end, plus any configured excess. */
    byte[] bodyFor(long[] bounds) {
        int from = (int) bounds[0];
        int to = (int) Math.min(bounds[1] + 1, data.length);
        return Arrays.copyOfRange(data, from, to + extraBytes);
    }

    GetObjectResponse responseFor(long offset, int length) {
        return GetObjectResponse.builder()
                .contentLength((long) length)
                .contentRange("bytes " + offset + "-" + (offset + length - 1) + "/" + data.length)
                .eTag("\"" + Integer.toHexString(data.length) + "\"")
                .build();
    }

    static S3Exception rangeNotSatisfiable() {
        // S3Exception.Builder inherits build() from AwsServiceException.Builder without a
        // covariant override. That leaves the chain's static type as AwsServiceException here,
        // even though the runtime type built is S3Exception.
        return (S3Exception) S3Exception.builder()
                .statusCode(416)
                .message("Requested Range Not Satisfiable")
                .build();
    }

    /**
     * Emits a body in chunks, never more than requested, and never from inside a subscriber's own request call: a
     * request made from {@code onNext} raises the demand served by the loop already running. A failing body stops after
     * its first chunk with a retryable error.
     */
    private final class ChunkedSubscription implements Subscription {

        private final Subscriber<? super ByteBuffer> subscriber;
        private final byte[] body;
        private final boolean failAfterFirstChunk;
        private long demand;
        private int position;
        private boolean emitting;
        private boolean done;

        ChunkedSubscription(Subscriber<? super ByteBuffer> subscriber, byte[] body, boolean failAfterFirstChunk) {
            this.subscriber = subscriber;
            this.body = body;
            this.failAfterFirstChunk = failAfterFirstChunk;
        }

        @Override
        public synchronized void request(long count) {
            boolean unbounded = count == Long.MAX_VALUE || demand + count < 0;
            demand = unbounded ? Long.MAX_VALUE : demand + count;
            if (emitting) {
                return;
            }
            emitting = true;
            try {
                emitWhileDemanded();
            } finally {
                emitting = false;
            }
        }

        private void emitWhileDemanded() {
            while (!done && demand > 0) {
                if (failAfterFirstChunk && position > 0) {
                    done = true;
                    subscriber.onError(RetryableException.builder()
                            .message("connection reset")
                            .build());
                    return;
                }
                if (position >= body.length) {
                    done = true;
                    subscriber.onComplete();
                    return;
                }
                int start = position;
                int length = Math.min(chunkSize, body.length - start);
                position += length;
                demand--;
                subscriber.onNext(ByteBuffer.wrap(body, start, length));
            }
        }

        @Override
        public synchronized void cancel() {
            if (!done) {
                cancellations.incrementAndGet();
            }
            done = true;
        }
    }
}
