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

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OptionsRecordsTest {

    @Test
    void listOptionsDefaultsAreEmpty() {
        ListOptions o = ListOptions.defaults();
        assertThat(o.pageSize()).isEmpty();
        assertThat(o.includeUserMetadata()).isFalse();
    }

    @Test
    void listOptionsBuilderHonorsPageSizeAndMetadata() {
        ListOptions o =
                ListOptions.builder().pageSize(500).includeUserMetadata(true).build();
        assertThat(o.pageSize()).hasValue(500);
        assertThat(o.includeUserMetadata()).isTrue();
    }

    @Test
    void readOptionsRangeRejectsNegativeOffset() {
        assertThatThrownBy(() -> ReadOptions.range(-1L, 16L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readOptionsFromOffsetReadsToEof() {
        ReadOptions o = ReadOptions.fromOffset(1024L);
        assertThat(o.offset()).isEqualTo(1024L);
        assertThat(o.length()).isEmpty();
    }

    @Test
    void writeOptionsDefaultsAreEmpty() {
        WriteOptions o = WriteOptions.defaults();
        assertThat(o.contentType()).isEmpty();
        assertThat(o.userMetadata()).isEmpty();
        assertThat(o.ifNotExists()).isFalse();
        assertThat(o.disableMultipart()).isFalse();
    }

    @Test
    void writeOptionsBuilderHonorsAllFields() {
        WriteOptions o = WriteOptions.builder()
                .contentType("application/octet-stream")
                .userMetadata(Map.of("source", "test"))
                .ifNotExists(true)
                .ifMatchEtag("etag-1")
                .contentLength(1024L)
                .disableMultipart(true)
                .timeout(Duration.ofSeconds(30))
                .build();
        assertThat(o.contentType()).contains("application/octet-stream");
        assertThat(o.userMetadata()).containsEntry("source", "test");
        assertThat(o.ifNotExists()).isTrue();
        assertThat(o.ifMatchEtag()).contains("etag-1");
        assertThat(o.contentLength()).hasValue(1024L);
        assertThat(o.disableMultipart()).isTrue();
        assertThat(o.timeout()).contains(Duration.ofSeconds(30));
    }

    @Test
    void copyOptionsDefaultsArePassthrough() {
        CopyOptions o = CopyOptions.defaults();
        assertThat(o.ifMatchSourceEtag()).isEmpty();
        assertThat(o.ifNotExistsAtDestination()).isFalse();
        assertThat(o.overrideUserMetadata()).isEmpty();
        assertThat(o.preserveLastModified()).isFalse();
    }

    @Test
    void readOptionsVersionIdDefaultsToEmpty() {
        ReadOptions opts = ReadOptions.defaults();
        assertThat(opts.versionId()).isEmpty();
    }

    @Test
    void deleteResultReportsCompletion() {
        DeleteResult ok = new DeleteResult(Set.of("a", "b"), Set.of(), Map.of());
        assertThat(ok.isComplete()).isTrue();
        DeleteResult partial = new DeleteResult(Set.of("a"), Set.of(), Map.of("b", new StorageException("boom")));
        assertThat(partial.isComplete()).isFalse();
        assertThat(partial.failed()).containsKey("b");
    }

    @Test
    void presignWriteOptionsDefaultsAreEmpty() {
        PresignWriteOptions opts = PresignWriteOptions.defaults();
        assertThat(opts.contentType()).isEmpty();
        assertThat(opts.userMetadata()).isEmpty();
        assertThat(opts.ifNotExists()).isFalse();
    }

    @Test
    void presignWriteOptionsBuilderRoundTrips() {
        PresignWriteOptions opts = PresignWriteOptions.builder()
                .contentType("application/octet-stream")
                .addMetadata("source", "test")
                .ifNotExists(true)
                .build();
        assertThat(opts.contentType()).hasValue("application/octet-stream");
        assertThat(opts.userMetadata()).containsEntry("source", "test");
        assertThat(opts.ifNotExists()).isTrue();
    }

    @Test
    void readHandleClosesUnderlyingStream() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        java.io.InputStream in = new java.io.ByteArrayInputStream(new byte[] {1, 2, 3}) {
            @Override
            public void close() throws java.io.IOException {
                closed.set(true);
                super.close();
            }
        };
        StorageEntry.File meta = new StorageEntry.File(
                "k",
                3L,
                java.time.Instant.now(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Map.of());
        try (ReadHandle h = new ReadHandle(in, meta)) {
            assertThat(h.metadata()).isEqualTo(meta);
        }
        assertThat(closed).isTrue();
    }
}
