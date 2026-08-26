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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RangeReaderReadRangesTest {

    private static final int SIZE = 10_000;
    private byte[] data;
    private RangeReader reader;

    @BeforeEach
    void setUp() {
        data = new byte[SIZE];
        new Random(42).nextBytes(data);
        // plain interface implementation: exercises the default readRanges loop, not the
        // ByteArrayRangeReader override
        ByteArrayRangeReader recorder = new ByteArrayRangeReader(data);
        reader = new RangeReader() {
            @Override
            public int readRange(long offset, int length, ByteBuffer target) {
                return recorder.readRange(offset, length, target);
            }

            @Override
            public OptionalLong size() {
                return recorder.size();
            }

            @Override
            public String getSourceIdentifier() {
                return recorder.getSourceIdentifier();
            }

            @Override
            public void close() {
                // nothing to release
            }
        };
    }

    @Test
    void resultsMatchRequestOrderAndSingleReadSemantics() {
        ByteBuffer first = ByteBuffer.allocate(100);
        ByteBuffer second = ByteBuffer.allocate(50);
        ByteBuffer third = ByteBuffer.allocate(200);
        List<RangeRequest> requests = List.of(
                RangeRequest.of(5000, 100, first), RangeRequest.of(0, 50, second), RangeRequest.of(1234, 200, third));

        int[] read = reader.readRanges(requests);

        assertThat(read).containsExactly(100, 50, 200);
        assertBufferContent(first.flip(), 5000, 100);
        assertBufferContent(second.flip(), 0, 50);
        assertBufferContent(third.flip(), 1234, 200);
    }

    @Test
    void overlappingAndDuplicateRequestsAreEachSatisfied() {
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(100);
        ByteBuffer c = ByteBuffer.allocate(60);
        int[] read = reader.readRanges(
                List.of(RangeRequest.of(1000, 100, a), RangeRequest.of(1000, 100, b), RangeRequest.of(1050, 60, c)));
        assertThat(read).containsExactly(100, 100, 60);
        assertBufferContent(a.flip(), 1000, 100);
        assertBufferContent(b.flip(), 1000, 100);
        assertBufferContent(c.flip(), 1050, 60);
    }

    @Test
    void eofMatrixPerEntry() {
        ByteBuffer atEof = ByteBuffer.allocate(10);
        ByteBuffer pastEof = ByteBuffer.allocate(10);
        ByteBuffer straddling = ByteBuffer.allocate(100);
        ByteBuffer exactEnd = ByteBuffer.allocate(10);
        ByteBuffer zeroLength = ByteBuffer.allocate(10);
        int[] read = reader.readRanges(List.of(
                RangeRequest.of(SIZE, 10, atEof),
                RangeRequest.of(SIZE + 5, 10, pastEof),
                RangeRequest.of(SIZE - 40, 100, straddling),
                RangeRequest.of(SIZE - 10, 10, exactEnd),
                RangeRequest.of(100, 0, zeroLength)));
        assertThat(read).containsExactly(0, 0, 40, 10, 0);
        assertThat(straddling.position()).isEqualTo(40);
        assertThat(zeroLength.position()).isZero();
    }

    @Test
    void emptyBatchDoesNoIO() {
        assertThat(reader.readRanges(List.of())).isEmpty();
    }

    @Test
    void targetPositionAdvancesFromCurrentPosition() {
        ByteBuffer target = ByteBuffer.allocate(120);
        target.position(20);
        int[] read = reader.readRanges(List.of(RangeRequest.of(3000, 100, target)));
        assertThat(read).containsExactly(100);
        assertThat(target.position()).isEqualTo(120);
        target.flip().position(20);
        assertBufferContent(target, 3000, 100);
    }

    @Test
    void malformedBatchThrowsBeforeAnyIO() {
        ByteBuffer moved = ByteBuffer.allocate(100);
        RangeRequest shrunk = RangeRequest.of(0, 100, moved);
        moved.position(50);
        ByteBuffer untouched = ByteBuffer.allocate(10);
        List<RangeRequest> malformedBatch = List.of(RangeRequest.of(0, 10, untouched), shrunk);

        assertThatThrownBy(() -> reader.readRanges(malformedBatch)).isInstanceOf(IllegalArgumentException.class);
        assertThat(untouched.position()).isZero();
    }

    private void assertBufferContent(ByteBuffer buffer, int offset, int length) {
        assertThat(buffer.remaining()).isEqualTo(length);
        byte[] actual = new byte[length];
        buffer.get(actual);
        assertThat(actual).isEqualTo(Arrays.copyOfRange(data, offset, offset + length));
    }
}
