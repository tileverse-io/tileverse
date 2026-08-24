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
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for {@link AbstractRangeReader#readRange(long, int, ByteBuffer)}: EOF handling is delegated to the
 * backend and {@link RangeReader#size()} is never consulted on the read path.
 */
class AbstractRangeReaderTest {

    private static final int SOURCE_SIZE = 100;

    /**
     * Backend-style fake over an in-memory source: throws RangeNotSatisfiableException for a past-EOF offset and
     * truncates reads that straddle EOF, like the cloud backends do.
     */
    static class FakeRangeReader extends AbstractRangeReader {
        final AtomicInteger sizeCalls = new AtomicInteger();
        final byte[] data;

        FakeRangeReader() {
            this.data = new byte[SOURCE_SIZE];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
        }

        @Override
        protected int readRangeNoFlip(long offset, int length, ByteBuffer target) {
            if (offset >= data.length) {
                throw new RangeNotSatisfiableException("offset " + offset + " past EOF");
            }
            int available = (int) Math.min(length, data.length - offset);
            target.put(data, (int) offset, available);
            return available;
        }

        @Override
        public OptionalLong size() {
            sizeCalls.incrementAndGet();
            return OptionalLong.of(data.length);
        }

        @Override
        public String getSourceIdentifier() {
            return "fake";
        }

        @Override
        public void close() {
            // nothing to release
        }
    }

    @Test
    void readRangeDoesNotConsultSize() {
        FakeRangeReader reader = new FakeRangeReader();
        ByteBuffer target = ByteBuffer.allocate(10);
        int read = reader.readRange(0, 10, target);
        assertThat(read).isEqualTo(10);
        assertThat(reader.sizeCalls).hasValue(0);
    }

    @Test
    void readRangePastEofReturnsZeroWithPositionUnchanged() {
        FakeRangeReader reader = new FakeRangeReader();
        ByteBuffer target = ByteBuffer.allocate(13);
        target.position(3);
        int read = reader.readRange(SOURCE_SIZE + 5, 10, target);
        assertThat(read).isZero();
        assertThat(target.position()).isEqualTo(3);
        assertThat(reader.sizeCalls).hasValue(0);
    }

    @Test
    void readRangeAtExactEofReturnsZero() {
        FakeRangeReader reader = new FakeRangeReader();
        ByteBuffer target = ByteBuffer.allocate(10);
        assertThat(reader.readRange(SOURCE_SIZE, 10, target)).isZero();
    }

    @Test
    void readRangeStraddlingEofReturnsShortCount() {
        FakeRangeReader reader = new FakeRangeReader();
        ByteBuffer target = ByteBuffer.allocate(50);
        int read = reader.readRange(SOURCE_SIZE - 10, 50, target);
        assertThat(read).isEqualTo(10);
        assertThat(target.position()).isEqualTo(10);
    }

    @Test
    void validationStillRejectsBadArguments() {
        FakeRangeReader reader = new FakeRangeReader();
        ByteBuffer target = ByteBuffer.allocate(10);
        assertThatThrownBy(() -> reader.readRange(-1, 10, target)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.readRange(0, -1, target)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.readRange(0, 10, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.readRange(0, 20, target)).isInstanceOf(IllegalArgumentException.class);
        assertThat(reader.readRange(5, 0, target)).isZero();
        assertThat(reader.sizeCalls).hasValue(0);
    }
}
