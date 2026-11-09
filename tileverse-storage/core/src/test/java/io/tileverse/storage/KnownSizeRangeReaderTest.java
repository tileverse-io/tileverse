/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.io.ByteRange;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
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
        StorageEntry.File entry = entry(64, Instant.now(), Optional.of("e"), Optional.empty());
        try (RangeReader seeded = KnownSizeRangeReader.of(delegate, entry)) {
            assertThat(seeded.size()).hasValue(64L);
            assertThat(delegate.sizeCalls).hasValue(0);
        }
    }

    @Test
    void identityAndReadsForwardToDelegate() throws IOException {
        CountingReader delegate = new CountingReader();
        StorageEntry.File entry = entry(64, Instant.now(), Optional.of("e"), Optional.empty());
        RangeReader seeded = KnownSizeRangeReader.of(delegate, entry);

        assertThat(seeded.getSourceIdentifier()).isEqualTo(delegate.getSourceIdentifier());
        ByteBuffer target = ByteBuffer.allocate(10);
        assertThat(seeded.readRange(0, 10, target)).isEqualTo(10);

        seeded.close();
        assertThat(delegate.closed).isTrue();
    }

    @Test
    void readRangesForwardsTheBatchToTheDelegate() throws IOException {
        byte[] data = new byte[1000];
        new Random(7).nextBytes(data);
        ByteArrayRangeReader recorder = new ByteArrayRangeReader(data);
        StorageEntry.File entry = entry(data.length, Instant.now(), Optional.of("e"), Optional.empty());
        try (RangeReader reader = KnownSizeRangeReader.of(recorder, entry)) {
            ByteBuffer first = ByteBuffer.allocate(10);
            ByteBuffer second = ByteBuffer.allocate(20);
            List<RangeRequest> requests = List.of(RangeRequest.of(0, 10, first), RangeRequest.of(100, 20, second));

            BatchReadResult result = reader.readRanges(requests);

            assertThat(counts(result)).containsExactly(10, 20);
            assertThat(recorder.batchReads()).containsExactly(List.of(new ByteRange(0, 10), new ByteRange(100, 20)));
            assertThat(result)
                    .as("the delegate's accounting passes through untouched")
                    .isEqualTo(BatchReadResult.perRange(requests, new int[] {10, 20}));
        }
    }

    @Test
    void versionIsTheEtag() throws IOException {
        StorageEntry.File entry = entry(64, Instant.now(), Optional.of("\"abc@1\""), Optional.of("v7"));
        try (RangeReader reader = KnownSizeRangeReader.of(new CountingReader(), entry)) {
            assertThat(reader.version()).hasValue("\"abc@1\"");
            assertThat(reader.contentIdentifier()).isEqualTo("counting@\"abc@1\"");
        }
    }

    @Test
    void versionFallsBackToVersionId() throws IOException {
        StorageEntry.File entry = entry(64, Instant.now(), Optional.empty(), Optional.of("v7"));
        try (RangeReader reader = KnownSizeRangeReader.of(new CountingReader(), entry)) {
            assertThat(reader.version()).hasValue("v7");
        }
    }

    @Test
    void versionFromSizeAndLastModified() throws IOException {
        Instant lastModified = Instant.ofEpochMilli(1_700_000_000_000L);
        StorageEntry.File entry = entry(64, lastModified, Optional.empty(), Optional.empty());
        try (RangeReader reader = KnownSizeRangeReader.of(new CountingReader(), entry)) {
            assertThat(reader.version()).hasValue("64-1700000000000");
        }
    }

    // Backends report the epoch when they do not know the last-modified time.
    @Test
    void noVersionAtEpoch() throws IOException {
        StorageEntry.File entry = entry(64, Instant.EPOCH, Optional.empty(), Optional.empty());
        try (RangeReader reader = KnownSizeRangeReader.of(new CountingReader(), entry)) {
            assertThat(reader.version()).isEmpty();
            assertThat(reader.contentIdentifier()).isEqualTo(reader.getSourceIdentifier());
        }
    }

    @Test
    void versionlessEntryAsksDelegate() throws IOException {
        CountingReader versionedDelegate = new CountingReader() {
            @Override
            public Optional<String> version() {
                return Optional.of("from-delegate");
            }
        };
        StorageEntry.File entry = entry(64, Instant.EPOCH, Optional.empty(), Optional.empty());
        try (RangeReader reader = KnownSizeRangeReader.of(versionedDelegate, entry)) {
            assertThat(reader.version()).hasValue("from-delegate");
        }
    }

    @Test
    void unversionedReaderByDefault() throws IOException {
        try (RangeReader reader = new CountingReader()) {
            assertThat(reader.version()).isEmpty();
            assertThat(reader.contentIdentifier()).isEqualTo("counting");
        }
    }

    static StorageEntry.File entry(long size, Instant lastModified, Optional<String> etag, Optional<String> versionId) {
        return new StorageEntry.File("key", size, lastModified, etag, versionId, Optional.empty(), Map.of());
    }
}
