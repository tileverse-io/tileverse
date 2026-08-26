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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.cache.CachingRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for the batched {@link AbstractRangeReader#readRanges} template: planning honors the protected hooks,
 * every fetch goes through the public {@code readRange} (keeping the 416-to-0 translation on one path), and the default
 * hooks reproduce the interface-default behavior exactly.
 */
class AbstractRangeReaderBatchTest {

    private static final int SOURCE_SIZE = 100_000;

    /**
     * Backend-style fake with configurable hooks: throws RangeNotSatisfiableException for a past-EOF offset and
     * truncates reads straddling EOF, like the cloud backends do. Records every backend read and the concurrency
     * high-water mark.
     */
    static class RecordingRangeReader extends AbstractRangeReader {
        final byte[] data = new byte[SOURCE_SIZE];
        final List<ByteRange> backendReads = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger maxActive = new AtomicInteger();
        CoalescingPolicy policy = CoalescingPolicy.NONE;
        int maxConcurrent = 1;

        RecordingRangeReader() {
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i % 251);
            }
        }

        @Override
        protected int readRangeNoFlip(long offset, int length, ByteBuffer target) {
            int current = active.incrementAndGet();
            maxActive.accumulateAndGet(current, Math::max);
            try {
                backendReads.add(new ByteRange(offset, length));
                if (offset >= data.length) {
                    throw new RangeNotSatisfiableException("offset " + offset + " is past EOF");
                }
                int available = (int) Math.min(length, data.length - offset);
                target.put(data, (int) offset, available);
                return available;
            } finally {
                active.decrementAndGet();
            }
        }

        @Override
        protected CoalescingPolicy coalescingPolicy() {
            return policy;
        }

        @Override
        protected int maxConcurrentFetches() {
            return maxConcurrent;
        }

        @Override
        public OptionalLong size() {
            return OptionalLong.of(data.length);
        }

        @Override
        public String getSourceIdentifier() {
            return "recording";
        }

        @Override
        public void close() {
            // nothing to release
        }
    }

    private final RecordingRangeReader reader = new RecordingRangeReader();

    private static List<RangeRequest> batchOf(long[][] ranges) {
        List<RangeRequest> requests = new ArrayList<>();
        for (long[] range : ranges) {
            requests.add(RangeRequest.of(range[0], (int) range[1], ByteBuffer.allocate((int) range[1])));
        }
        return requests;
    }

    private void assertMatchesSingleReads(List<RangeRequest> requests, int[] counts) {
        for (int i = 0; i < requests.size(); i++) {
            ByteBuffer expected = reader.readRange(requests.get(i).range()).flip();
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(expected.remaining());
            assertThat(requests.get(i).target().duplicate().flip())
                    .as("content for entry " + i)
                    .isEqualTo(expected);
        }
    }

    @Test
    void defaultHooksReadEachRangeInRequestOrder() {
        List<RangeRequest> requests = batchOf(new long[][] {{500, 100}, {0, 100}, {500, 100}});

        int[] counts = reader.readRanges(requests);

        assertThat(reader.backendReads)
                .containsExactly(ByteRange.of(500, 100), ByteRange.of(0, 100), ByteRange.of(500, 100));
        assertMatchesSingleReads(requests, counts);
    }

    @Test
    void pastEofEntriesTranslateToZeroThroughReadRange() {
        List<RangeRequest> requests = batchOf(new long[][] {{SOURCE_SIZE + 10, 10}, {SOURCE_SIZE - 25, 100}, {0, 10}});

        int[] counts = reader.readRanges(requests);

        assertThat(counts).containsExactly(0, 25, 10);
    }

    @Test
    void zeroLengthEntriesAndEmptyBatches() {
        assertThat(reader.readRanges(List.of())).isEmpty();

        List<RangeRequest> requests = batchOf(new long[][] {{100, 0}, {200, 10}});
        int[] counts = reader.readRanges(requests);

        assertThat(counts).containsExactly(0, 10);
        assertThat(reader.backendReads).containsExactly(ByteRange.of(200, 10));
    }

    @Test
    void malformedBatchesFailBeforeAnyBackendRead() {
        List<RangeRequest> withNull = new ArrayList<>();
        withNull.add(null);

        assertThatThrownBy(() -> reader.readRanges(withNull)).isInstanceOf(IllegalArgumentException.class);
        assertThat(reader.backendReads).isEmpty();
    }

    @Test
    void mergingPolicyCoalescesNearbyRangesIntoOneBackendRead() {
        reader.policy = new CoalescingPolicy(64, 4096);
        List<RangeRequest> requests = batchOf(new long[][] {{100, 20}, {150, 20}});

        int[] counts = reader.readRanges(requests);

        assertThat(reader.backendReads).containsExactly(ByteRange.of(100, 70));
        assertMatchesSingleReads(requests, counts);
    }

    @Test
    void concurrencyCapIsHonored() {
        reader.policy = CoalescingPolicy.NONE;
        reader.maxConcurrent = 4;
        long[][] ranges = new long[10][];
        for (int i = 0; i < ranges.length; i++) {
            ranges[i] = new long[] {i * 9_000L, 400};
        }
        List<RangeRequest> requests = batchOf(ranges);

        int[] counts = reader.readRanges(requests);

        assertThat(reader.maxActive.get()).isLessThanOrEqualTo(4);
        assertThat(reader.backendReads).hasSize(10);
        assertMatchesSingleReads(requests, counts);
    }

    @Test
    void decoratorBatchOverridesAreUnaffected() throws IOException {
        ByteArrayRangeReader delegate = new ByteArrayRangeReader(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        try (CachingRangeReader caching = CachingRangeReader.of(delegate)) {
            List<RangeRequest> requests = List.of(
                    RangeRequest.of(0, 4, ByteBuffer.allocate(4)), RangeRequest.of(4, 4, ByteBuffer.allocate(4)));

            int[] counts = caching.readRanges(requests);

            assertThat(counts).containsExactly(4, 4);
            // The pure cache forwards its misses as one delegate batch. The in-memory delegate
            // additionally records every entry as a single read; only the batch log is asserted.
            assertThat(delegate.batchReads()).hasSize(1);
        }
    }
}
