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
package io.tileverse.storage.batch;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeRequest;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * One contiguous downstream fetch produced by {@link BatchPlanner}, together with the request slices it satisfies.
 *
 * <p>A fetch whose single slice covers its whole range is {@link #isDirect() direct}: the executing side reads it
 * straight into the caller's target, zero copy. Every other fetch reads into scratch and {@link #scatter(ByteBuffer,
 * int, List, int[]) scatters} the bytes into its targets.
 *
 * @param range the contiguous byte range to fetch downstream
 * @param slices where the fetched bytes land, one entry per satisfied request
 */
public record PlannedFetch(ByteRange range, List<Slice> slices) {

    /**
     * Maps one request onto its position inside a fetch.
     *
     * @param requestIndex index of the satisfied request in the original batch
     * @param offsetInFetch offset of the request's first byte relative to the fetch start
     * @param length the request's length in bytes
     */
    public record Slice(int requestIndex, int offsetInFetch, int length) {}

    /**
     * Validates and defensively copies the slice list.
     *
     * @throws IllegalArgumentException if the fetch satisfies no request
     */
    public PlannedFetch {
        requireNonNull(range, "range cannot be null");
        slices = List.copyOf(slices);
        if (slices.isEmpty()) {
            throw new IllegalArgumentException("a fetch must satisfy at least one request");
        }
    }

    /**
     * Returns whether this fetch can read straight into its single requester's target.
     *
     * @return true when the only slice covers the whole fetch range
     */
    public boolean isDirect() {
        if (slices.size() != 1) {
            return false;
        }
        Slice only = slices.get(0);
        return only.offsetInFetch() == 0 && only.length() == range.length();
    }

    /**
     * Copies each slice's available bytes from the fetched data into its request target and records the byte count.
     *
     * <p>{@code fetched} holds the fetch result in positions {@code [0, bytesRead)} and is read with absolute slices;
     * its position never moves and a read-only buffer works. A short fetch (EOF inside the fetch range) truncates or
     * zeroes the slices past the read end, preserving the per-entry EOF contract of
     * {@link io.tileverse.storage.RangeReader#readRanges}. Targets are written at their current position, advancing it
     * by the copied count.
     *
     * @param fetched the fetch result, data in positions 0 to bytesRead
     * @param bytesRead the number of bytes actually fetched
     * @param requests the original batch the slice indices point into
     * @param counts per-request byte counts, written at each slice's request index
     */
    public void scatter(ByteBuffer fetched, int bytesRead, List<RangeRequest> requests, int[] counts) {
        for (Slice slice : slices) {
            int available = Math.min(slice.length(), Math.max(0, bytesRead - slice.offsetInFetch()));
            if (available > 0) {
                ByteBuffer source = fetched.slice(slice.offsetInFetch(), available);
                requests.get(slice.requestIndex()).target().put(source);
            }
            counts[slice.requestIndex()] = available;
        }
    }
}
