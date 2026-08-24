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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KnownSizeRangeReaderTest {

    static class CountingReader extends AbstractRangeReader {
        final AtomicInteger sizeCalls = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();

        @Override
        protected int readRangeNoFlip(long offset, int length, ByteBuffer target) {
            if (offset >= 64) {
                throw new RangeNotSatisfiableException("past EOF");
            }
            int available = (int) Math.min(length, 64 - offset);
            target.put(new byte[available]);
            return available;
        }

        @Override
        public OptionalLong size() {
            sizeCalls.incrementAndGet();
            return OptionalLong.of(64);
        }

        @Override
        public String getSourceIdentifier() {
            return "counting";
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    @Test
    void sizeAnswersSeededValueWithoutDelegateCall() throws IOException {
        CountingReader delegate = new CountingReader();
        try (RangeReader seeded = KnownSizeRangeReader.of(delegate, 64)) {
            assertThat(seeded.size()).hasValue(64L);
            assertThat(delegate.sizeCalls).hasValue(0);
        }
    }

    @Test
    void identityAndReadsForwardToDelegate() throws IOException {
        CountingReader delegate = new CountingReader();
        RangeReader seeded = KnownSizeRangeReader.of(delegate, 64);

        assertThat(seeded.getSourceIdentifier()).isEqualTo(delegate.getSourceIdentifier());
        ByteBuffer target = ByteBuffer.allocate(10);
        assertThat(seeded.readRange(0, 10, target)).isEqualTo(10);

        seeded.close();
        assertThat(delegate.closed).isTrue();
    }
}
