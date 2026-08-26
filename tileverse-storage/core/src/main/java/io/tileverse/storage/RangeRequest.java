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
package io.tileverse.storage;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.List;

/**
 * A single read of one byte range into a caller-provided buffer, the unit of batch read operations.
 *
 * <p>This is a parameter object, never a value: record equality goes through {@link ByteBuffer#equals(Object)}, which
 * compares remaining content and changes as the buffer fills. Do not use instances as map or set keys.
 *
 * <p>The constructor rejects a request whose target cannot hold the range. Buffer positions can move between
 * construction and the read; implementations re-check remaining capacity at call time through {@link #validate(List)}.
 *
 * @param range the byte range to read
 * @param target the writable buffer the bytes land in, starting at its current position
 */
public record RangeRequest(ByteRange range, ByteBuffer target) {

    public RangeRequest {
        requireNonNull(range, "range cannot be null");
        requireNonNull(target, "target cannot be null");
        if (target.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        checkCapacity(range, target);
    }

    /**
     * Creates a request for {@code length} bytes at {@code offset} into {@code target}.
     *
     * @param offset the offset to read from
     * @param length the number of bytes to read
     * @param target the buffer the bytes land in
     * @return a validated request
     */
    public static RangeRequest of(long offset, int length, ByteBuffer target) {
        return new RangeRequest(new ByteRange(offset, length), target);
    }

    /**
     * Creates a request for {@code range} into {@code target}.
     *
     * @param range the byte range to read
     * @param target the buffer the bytes land in
     * @return a validated request
     */
    public static RangeRequest of(ByteRange range, ByteBuffer target) {
        return new RangeRequest(range, target);
    }

    /**
     * Validates a batch before any I/O: the list and every element are non-null, and every target still has room for
     * its range at the time of the call.
     *
     * @param requests the batch to validate
     * @throws NullPointerException if the list itself is null
     * @throws IllegalArgumentException on a null element or a target with insufficient remaining capacity, naming the
     *     offending index
     */
    public static void validate(List<RangeRequest> requests) {
        requireNonNull(requests, "requests cannot be null");
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            if (request == null) {
                throw new IllegalArgumentException("null request at index " + i);
            }
            try {
                checkCapacity(request.range(), request.target());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid request at index " + i + ": " + e.getMessage());
            }
        }
    }

    private static void checkCapacity(ByteRange range, ByteBuffer target) {
        if (target.remaining() < range.length()) {
            throw new IllegalArgumentException(
                    "target has insufficient remaining capacity: " + target.remaining() + " < " + range.length());
        }
    }
}
