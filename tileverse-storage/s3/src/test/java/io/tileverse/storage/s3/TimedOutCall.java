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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.tileverse.storage.s3.ByteBufferAsyncResponseTransformer.Result;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Answers async GETs as the SDK does for a call hitting its {@code apiCallTimeout}: it fails the current attempt
 * through the transformer, then fails the returned future. A retry scheduled earlier can still start a fresh attempt
 * afterwards: a test then checks that no body reaches a buffer once the read has thrown.
 */
final class TimedOutCall {

    private final AtomicReference<ByteBufferAsyncResponseTransformer> transformer = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Result>> retry = new AtomicReference<>();

    @SuppressWarnings("unchecked")
    TimedOutCall(S3AsyncClient client) {
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenAnswer(invocation -> timeOut(invocation.getArgument(1)));
    }

    private CompletableFuture<Result> timeOut(ByteBufferAsyncResponseTransformer body) {
        transformer.set(body);
        ApiCallTimeoutException timeout = ApiCallTimeoutException.create(1_000);
        body.prepare();
        body.exceptionOccurred(timeout);
        return CompletableFuture.failedFuture(timeout);
    }

    /** Starts a fresh attempt, delivers {@code length} non-zero bytes as its body, and ends it. */
    void startTheScheduledRetry(int length) {
        ByteBufferAsyncResponseTransformer body = transformer.get();
        retry.set(body.prepare());
        body.onResponse(GetObjectResponse.builder().build());
        body.onStream(subscriber -> deliver(subscriber, length));
    }

    private static void deliver(Subscriber<? super ByteBuffer> subscriber, int length) {
        byte[] late = new byte[length];
        Arrays.fill(late, (byte) -1);
        subscriber.onSubscribe(new IdleSubscription());
        subscriber.onNext(ByteBuffer.wrap(late));
        subscriber.onComplete();
    }

    int bytesWrittenByTheRetry() {
        return retry.get().join().bytesWritten();
    }

    /** Leaves emission to the test. */
    private static final class IdleSubscription implements Subscription {
        @Override
        public void request(long demand) {
            // the test emits explicitly
        }

        @Override
        public void cancel() {
            // nothing to stop
        }
    }
}
