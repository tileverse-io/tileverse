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

import java.util.Arrays;
import java.util.List;

/**
 * What one {@link RangeReader#readRanges} call did: the bytes landed per request, and what the call cost in backend
 * requests and bytes moved.
 *
 * <p>The per-request counts describe the caller's own requests. The transport numbers describe the whole stack below
 * the caller: {@link #fetches()} backend requests were issued, {@link #bytesTransferred()} bytes crossed the wire, gaps
 * bridged between merged ranges included, and {@link #bytesFromCache()} bytes were served without reaching the backend.
 * A decorator reports its own per-request view and folds the transport numbers of the reader below it in with
 * {@link #merge(BatchReadResult)}. A call that throws reports nothing.
 *
 * <p>Instances are immutable; the counts are reached one at a time through {@link #bytesRead(int)}.
 */
public final class BatchReadResult {

    /** The result of a batch with no requests: nothing read, nothing fetched. */
    public static final BatchReadResult EMPTY = new BatchReadResult(new int[0], 0, 0, 0, 0);

    private final int[] bytesRead;
    private final long bytesRequested;
    private final long fetches;
    private final long bytesTransferred;
    private final long bytesFromCache;

    private BatchReadResult(
            int[] bytesRead, long bytesRequested, long fetches, long bytesTransferred, long bytesFromCache) {
        requireNonNegative(bytesRequested, "bytesRequested");
        requireNonNegative(fetches, "fetches");
        requireNonNegative(bytesTransferred, "bytesTransferred");
        requireNonNegative(bytesFromCache, "bytesFromCache");
        for (int count : bytesRead) {
            requireNonNegative(count, "bytesRead");
        }
        this.bytesRead = bytesRead;
        this.bytesRequested = bytesRequested;
        this.fetches = fetches;
        this.bytesTransferred = bytesTransferred;
        this.bytesFromCache = bytesFromCache;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " cannot be negative: " + value);
        }
    }

    /**
     * Builds the result of a call: the bytes landed per request, in request order, and what the call cost.
     *
     * @param requests the batch the call served; its ranges' lengths are the bytes requested
     * @param bytesRead the bytes landed per request, one entry per request; copied
     * @param fetches backend requests issued
     * @param bytesTransferred bytes that crossed the wire, gaps included
     * @param bytesFromCache bytes served without reaching the backend
     * @return the result
     * @throws IllegalArgumentException if {@code bytesRead} has one entry per request or any number is negative
     */
    public static BatchReadResult of(
            List<RangeRequest> requests, int[] bytesRead, long fetches, long bytesTransferred, long bytesFromCache) {
        requireNonNull(requests, "requests cannot be null");
        requireNonNull(bytesRead, "bytesRead cannot be null");
        if (bytesRead.length != requests.size()) {
            throw new IllegalArgumentException(
                    "bytesRead has " + bytesRead.length + " entries for " + requests.size() + " requests");
        }
        return new BatchReadResult(
                bytesRead.clone(), bytesRequested(requests), fetches, bytesTransferred, bytesFromCache);
    }

    /**
     * Builds the result of a call that read every non-empty range with its own fetch, nothing merged and nothing served
     * from a cache: the cost model of the interface default. Bytes transferred equal the bytes read.
     *
     * @param requests the batch the call served
     * @param bytesRead the bytes landed per request, one entry per request; copied
     * @return the result
     */
    public static BatchReadResult perRange(List<RangeRequest> requests, int[] bytesRead) {
        requireNonNull(requests, "requests cannot be null");
        long fetches = requests.stream()
                .filter(request -> request.range().length() > 0)
                .count();
        long transferred = 0;
        for (int count : bytesRead) {
            transferred += count;
        }
        return of(requests, bytesRead, fetches, transferred, 0);
    }

    private static long bytesRequested(List<RangeRequest> requests) {
        long total = 0;
        for (RangeRequest request : requests) {
            total += request.range().length();
        }
        return total;
    }

    /**
     * How many requests the call served.
     *
     * @return the number of requests, the valid indexes of {@link #bytesRead(int)}
     */
    public int requests() {
        return bytesRead.length;
    }

    /**
     * The bytes landed for one request, with the exact {@link RangeReader#readRange(long, int, java.nio.ByteBuffer)}
     * semantics: 0 at or past EOF, a short count when the range straddles EOF.
     *
     * @param requestIndex the request's index in the batch
     * @return the bytes read for that request
     * @throws IndexOutOfBoundsException if the index is not one of the batch
     */
    public int bytesRead(int requestIndex) {
        return bytesRead[requestIndex];
    }

    /**
     * The sum of the requested ranges' lengths.
     *
     * @return bytes requested by the caller
     */
    public long bytesRequested() {
        return bytesRequested;
    }

    /**
     * Backend requests issued to serve the call, by the whole stack below the caller.
     *
     * @return the number of fetches
     */
    public long fetches() {
        return fetches;
    }

    /**
     * Bytes that crossed the wire to serve the call, the gaps bridged between merged ranges included.
     *
     * @return bytes transferred
     */
    public long bytesTransferred() {
        return bytesTransferred;
    }

    /**
     * Bytes served without reaching the backend, by a cache somewhere in the stack.
     *
     * @return bytes from cache
     */
    public long bytesFromCache() {
        return bytesFromCache;
    }

    /**
     * Folds in the transport numbers of the reader below this one, keeping this result's per-request view. A decorator
     * builds its own result and merges the result of its delegate call into it.
     *
     * @param below the result of the delegate call
     * @return a result with this per-request view and the summed transport numbers
     */
    public BatchReadResult merge(BatchReadResult below) {
        requireNonNull(below, "below cannot be null");
        return new BatchReadResult(
                bytesRead,
                bytesRequested,
                fetches + below.fetches,
                bytesTransferred + below.bytesTransferred,
                bytesFromCache + below.bytesFromCache);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BatchReadResult that)) {
            return false;
        }
        return Arrays.equals(bytesRead, that.bytesRead)
                && bytesRequested == that.bytesRequested
                && fetches == that.fetches
                && bytesTransferred == that.bytesTransferred
                && bytesFromCache == that.bytesFromCache;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(bytesRead);
        result = 31 * result + Long.hashCode(bytesRequested);
        result = 31 * result + Long.hashCode(fetches);
        result = 31 * result + Long.hashCode(bytesTransferred);
        result = 31 * result + Long.hashCode(bytesFromCache);
        return result;
    }

    @Override
    public String toString() {
        return "BatchReadResult[bytesRead=" + Arrays.toString(bytesRead)
                + ", bytesRequested=" + bytesRequested
                + ", fetches=" + fetches
                + ", bytesTransferred=" + bytesTransferred
                + ", bytesFromCache=" + bytesFromCache + "]";
    }
}
