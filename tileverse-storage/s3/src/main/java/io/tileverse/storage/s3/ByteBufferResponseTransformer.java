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
import io.tileverse.storage.adapters.ByteBufferOutputStream;
import io.tileverse.storage.adapters.ByteBufferSinkException;
import java.io.IOException;
import java.nio.ByteBuffer;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Streams a ranged GET body straight into the caller's buffer inside the SDK's retry loop.
 *
 * <p>The SDK invokes {@link #transform} once per request attempt; every attempt restarts at the buffer position
 * captured at construction, which keeps a retried body from landing after a partial first attempt. A failure while
 * reading the body is rethrown as a retryable SDK exception, exactly like the SDK's own byte-array transformer, which
 * keeps the retry behavior that reads had before streaming. A sink failure, a body longer than requested or a target
 * refusing the write, aborts the connection and fails the read without a retry; the SDK drains a shorter body's
 * remainder itself.
 *
 * <p>On success the target's position has advanced by {@link #bytesWritten()} and its limit is untouched. Transient
 * heap per read is the chunk buffer of {@link java.io.InputStream#transferTo}, never the response size.
 */
final class ByteBufferResponseTransformer implements ResponseTransformer<GetObjectResponse, GetObjectResponse> {

    private final ByteBuffer target;
    private final int start;
    private final int maxBytes;
    private int written;

    /**
     * Creates a transformer writing into {@code target} from its current position.
     *
     * @param target the buffer where the body lands, heap or direct
     * @param maxBytes the requested range length; must not exceed the target's remaining capacity
     * @throws IllegalArgumentException if {@code maxBytes} is negative or exceeds the remaining capacity
     */
    ByteBufferResponseTransformer(ByteBuffer target, int maxBytes) {
        if (maxBytes < 0 || maxBytes > target.remaining()) {
            throw new IllegalArgumentException(
                    "maxBytes " + maxBytes + " outside the target's remaining " + target.remaining());
        }
        this.target = target;
        this.start = target.position();
        this.maxBytes = maxBytes;
    }

    /**
     * Returns how many body bytes were written into the target by the last completed attempt.
     *
     * @return the byte count, at most {@code maxBytes}
     */
    int bytesWritten() {
        return written;
    }

    @Override
    public GetObjectResponse transform(GetObjectResponse response, AbortableInputStream body) {
        target.position(start);
        ByteBufferOutputStream sink = new ByteBufferOutputStream(target, maxBytes);
        try {
            body.transferTo(sink);
        } catch (ByteBufferSinkException sinkFailure) {
            body.abort();
            throw new StorageException(sinkFailure.getMessage(), sinkFailure);
        } catch (IOException dropped) {
            throw RetryableException.builder()
                    .message("Failed to read S3 response body")
                    .cause(dropped)
                    .build();
        }
        written = sink.bytesWritten();
        return response;
    }
}
