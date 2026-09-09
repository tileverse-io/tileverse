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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Serves a fixed byte array through mocked S3 clients as the SDK does. The sync stub hands the caller's
 * {@link ResponseTransformer} the body as an {@link AbortableInputStream} delivered in chunks of a configured size and
 * re-invokes the transformer on a retryable exception, like the SDK's retry loop. A request starting past the end fails
 * with a 416; a range running past the end is truncated as a real object store answers it. The async stub drives the
 * caller's {@link AsyncResponseTransformer} through {@code prepare}, {@code onResponse}, and {@code onStream} with a
 * publisher emitting the same chunks, each a view of the body array at its own offset.
 */
final class S3ObjectStub {

    /** The number of attempts allowed per request by the SDK's default retry mode. */
    private static final int MAX_ATTEMPTS = 3;

    private final byte[] data;
    private final int chunkSize;
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicInteger aborts = new AtomicInteger();
    private int extraBytes;
    private int bodyFailuresLeft;

    S3ObjectStub(byte[] data, int chunkSize) {
        this.data = data;
        this.chunkSize = chunkSize;
    }

    /** Lengthens every body by {@code count} bytes past the requested range, like a server ignoring the range. */
    S3ObjectStub respondingWithExtraBytes(int count) {
        this.extraBytes = count;
        return this;
    }

    /** Makes the next {@code count} body reads fail after their first chunk, like a dropped connection. */
    S3ObjectStub failingBodyReads(int count) {
        this.bodyFailuresLeft = count;
        return this;
    }

    /** The number of transform attempts driven by the stub, retries included. */
    int attempts() {
        return attempts.get();
    }

    /** The number of bodies aborted by the caller instead of drained. */
    int aborts() {
        return aborts.get();
    }

    /** Stubs {@code getObject(request, transformer)} for every range of the object. */
    @SuppressWarnings("unchecked")
    void installSync(S3Client client) {
        lenient()
                .when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenAnswer(invocation -> serveSync(invocation.getArgument(0), invocation.getArgument(1)));
    }

    private Object serveSync(GetObjectRequest request, ResponseTransformer<GetObjectResponse, Object> transformer)
            throws Exception {
        long[] bounds = requestedRange(request);
        if (bounds[0] >= data.length) {
            throw rangeNotSatisfiable();
        }
        byte[] body = bodyFor(bounds);
        GetObjectResponse response = responseFor(bounds[0], body.length - extraBytes);
        for (int attempt = 1; ; attempt++) {
            attempts.incrementAndGet();
            AbortableInputStream stream = AbortableInputStream.create(nextBody(body), aborts::incrementAndGet);
            try {
                return transformer.transform(response, stream);
            } catch (RetryableException retry) {
                if (attempt == MAX_ATTEMPTS) {
                    throw retry;
                }
            } catch (RuntimeException other) {
                // Mirrors BaseSyncClientHandler's response-handler adapter: a non-retryable
                // failure out of the transformer reaches the caller wrapped in an SdkException,
                // never raw.
                throw NonRetryableException.builder()
                        .message("transform failed")
                        .cause(other)
                        .build();
            }
        }
    }

    /** Stubs {@code getObject(request, asyncTransformer)} for every range of the object. */
    @SuppressWarnings("unchecked")
    void installAsync(S3AsyncClient client) {
        lenient()
                .when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenAnswer(invocation -> serveAsync(invocation.getArgument(0), invocation.getArgument(1)));
    }

    private CompletableFuture<Object> serveAsync(
            GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, Object> transformer) {
        attempts.incrementAndGet();
        CompletableFuture<Object> outcome = transformer.prepare();
        long[] bounds = requestedRange(request);
        if (bounds[0] >= data.length) {
            transformer.exceptionOccurred(rangeNotSatisfiable());
            return outcome;
        }
        byte[] body = bodyFor(bounds);
        transformer.onResponse(responseFor(bounds[0], body.length - extraBytes));
        transformer.onStream(chunkedPublisher(body));
        return outcome;
    }

    /** Emits the body in chunks of the configured size, each a view of the body array at its own offset. */
    private SdkPublisher<ByteBuffer> chunkedPublisher(byte[] body) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            private int position;
            private boolean done;

            @Override
            public void request(long demand) {
                long remaining = demand;
                while (!done && remaining > 0) {
                    if (position >= body.length) {
                        done = true;
                        subscriber.onComplete();
                        return;
                    }
                    int length = Math.min(chunkSize, body.length - position);
                    subscriber.onNext(ByteBuffer.wrap(body, position, length));
                    position += length;
                    remaining--;
                }
            }

            @Override
            public void cancel() {
                done = true;
            }
        });
    }

    private InputStream nextBody(byte[] body) {
        if (bodyFailuresLeft > 0) {
            bodyFailuresLeft--;
            return new ChunkedInputStream(body, chunkSize, chunkSize);
        }
        return new ChunkedInputStream(body, chunkSize, -1);
    }

    /** Parses the request's {@code bytes=first-last} header into {@code {first, last}}. */
    static long[] requestedRange(GetObjectRequest request) {
        String[] bounds = request.range().replace("bytes=", "").split("-");
        return new long[] {Long.parseLong(bounds[0]), Long.parseLong(bounds[1])};
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

    /** Serves a body at most {@code chunkSize} bytes per read, failing once {@code failAt} bytes have been served. */
    private static final class ChunkedInputStream extends InputStream {

        private final byte[] body;
        private final int chunkSize;
        private final int failAt;
        private int position;

        ChunkedInputStream(byte[] body, int chunkSize, int failAt) {
            this.body = body;
            this.chunkSize = chunkSize;
            this.failAt = failAt;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] into, int offset, int length) throws IOException {
            if (failAt >= 0 && position >= failAt) {
                throw new IOException("connection reset");
            }
            if (position >= body.length) {
                return -1;
            }
            int count = Math.min(length, Math.min(chunkSize, body.length - position));
            System.arraycopy(body, position, into, offset, count);
            position += count;
            return count;
        }
    }
}
