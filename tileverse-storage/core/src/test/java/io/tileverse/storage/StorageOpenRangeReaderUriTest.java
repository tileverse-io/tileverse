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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Validates the {@link Storage#openRangeReader(URI)} default and its {@link Storage#relativizeToKey(URI)} helper.
 *
 * <p>Uses a stub {@code Storage} that records the relative key passed to its {@link Storage#openRangeReader(String)}
 * implementation so the tests focus on the URI-to-key derivation logic without touching any backend.
 */
@SuppressWarnings("resource")
class StorageOpenRangeReaderUriTest {

    @Test
    void delegatesRelativizedKeyToStringOverload() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/folder/"));
        storage.openRangeReader(URI.create("s3://bucket/folder/file.parquet"));
        assertThat(storage.lastKey).isEqualTo("file.parquet");
    }

    @Test
    void preservesNestedKeyStructure() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/release/2026-04-01/"));
        storage.openRangeReader(URI.create("s3://bucket/release/2026-04-01/theme=addresses/part-00001.parquet"));
        assertThat(storage.lastKey).isEqualTo("theme=addresses/part-00001.parquet");
    }

    @Test
    void normalizesBaseUriWithoutTrailingSlash() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/folder"));
        storage.openRangeReader(URI.create("s3://bucket/folder/file.parquet"));
        assertThat(storage.lastKey).isEqualTo("file.parquet");
    }

    @Test
    void schemeMismatchThrows() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/folder/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("https://bucket/folder/file.parquet")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme");
    }

    @Test
    void authorityMismatchThrows() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket-a/folder/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("s3://bucket-b/folder/file.parquet")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authority");
    }

    @Test
    void pathOutsideBaseThrows() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/release/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("s3://bucket/other/file.parquet")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not within");
    }

    @Test
    void uriEqualToBaseRejectsEmptyKey() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/folder/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("s3://bucket/folder/")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaf object");
    }

    @Test
    void queryStringIsPreservedInDerivedKey() {
        // Query strings can be load-bearing (HTTP signed URLs, SAS tokens, GCS ?alt=media). The default preserves them
        // into the key; backends whose key grammar can't represent a query string surface a NotFoundException at
        // openRangeReader(String) time, which is the right ownership of the error.
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/path/"));
        storage.openRangeReader(URI.create("https://server/path/file.bin?X-Amz-Signature=abc&X-Amz-Date=20260430"));
        assertThat(storage.lastKey).isEqualTo("file.bin?X-Amz-Signature=abc&X-Amz-Date=20260430");
    }

    @Test
    void fragmentIsDroppedSilently() {
        // RFC 3986 fragments are client-only and never reach the server, so they have no role in object identity. The
        // default drops them silently rather than rejecting.
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/folder/"));
        storage.openRangeReader(URI.create("s3://bucket/folder/file.parquet#footer"));
        assertThat(storage.lastKey).isEqualTo("file.parquet");
    }

    @Test
    void queryAndFragmentTogether_queryKeptFragmentDropped() {
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/path/"));
        storage.openRangeReader(URI.create("https://server/path/file.bin?signature=abc#footer"));
        assertThat(storage.lastKey).isEqualTo("file.bin?signature=abc");
    }

    @Test
    void literalDotDotEscapingNamespaceIsRejected() {
        // After URI.normalize(), 'https://server/data/../etc/passwd' becomes 'https://server/etc/passwd', which is no
        // longer under '/data/'. The prefix check fires before any reader is constructed.
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("https://server/data/../etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not within");
        assertThat(storage.lastKey).isNull();
    }

    @Test
    void deeplyNestedDotDotEscapingNamespaceIsRejected() {
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/sub/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("https://server/data/sub/../../etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not within");
        assertThat(storage.lastKey).isNull();
    }

    @Test
    void dotDotThatNormalizesBackInsideNamespaceIsAllowed() {
        // The user's URI is 'https://server/data/sub/../file.bin'; after normalize that's
        // 'https://server/data/file.bin',
        // which IS within /data/. There's no security issue - the resolved URL is canonical.
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        storage.openRangeReader(URI.create("https://server/data/sub/../file.bin"));
        assertThat(storage.lastKey).isEqualTo("file.bin");
    }

    @Test
    void singleDotSegmentIsCollapsedByNormalize() {
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        storage.openRangeReader(URI.create("https://server/data/./file.bin"));
        assertThat(storage.lastKey).isEqualTo("file.bin");
    }

    @Test
    void percentEncodedDotDotIsRejected() {
        // URI.normalize() does NOT decode percent-encoding, so '%2E%2E' survives normalization. Some HTTP servers
        // decode mid-path and would traverse, so we reject explicitly.
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("https://server/data/%2E%2E/etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traversal");
        assertThat(storage.lastKey).isNull();
    }

    @Test
    void percentEncodedDotDotIsRejectedRegardlessOfCase() {
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("https://server/data/%2e%2e/etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traversal");
    }

    @Test
    void mixedPercentEncodedDotDotIsRejected() {
        // One '.' encoded, the other literal: still decodes to '..' as a segment.
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        assertThatThrownBy(() -> storage.openRangeReader(URI.create("https://server/data/%2E./etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traversal");
    }

    @Test
    void deeplyNestedPercentEncodedTraversalIsRejected() {
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        assertThatThrownBy(
                        () -> storage.openRangeReader(URI.create("https://server/data/foo/%2E%2E/%2E%2E/etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traversal");
    }

    @Test
    void percentEncodedNonTraversalIsAllowed() {
        // Percent-encoded characters that aren't '..' must still pass through (e.g. encoded spaces, special chars in
        // legitimate filenames).
        RecordingStorage storage = new RecordingStorage(URI.create("https://server/data/"));
        storage.openRangeReader(URI.create("https://server/data/file%20with%20spaces.bin"));
        assertThat(storage.lastKey).isEqualTo("file%20with%20spaces.bin");
    }

    @Test
    void schemeComparisonIsCaseInsensitive() {
        RecordingStorage storage = new RecordingStorage(URI.create("HTTPS://server.example/data/"));
        // Java's URI parser already lowercases a uppercase scheme to "https" for the parsed URI; the comparison must
        // still tolerate either form to be RFC-compliant.
        storage.openRangeReader(URI.create("https://server.example/data/file.bin"));
        assertThat(storage.lastKey).isEqualTo("file.bin");
    }

    @Test
    void nullUriIsRejected() {
        RecordingStorage storage = new RecordingStorage(URI.create("s3://bucket/folder/"));
        assertThatThrownBy(() -> storage.openRangeReader((URI) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("uri");
    }

    /** Minimal Storage stub: records the key passed to the String overload, ignores all other operations. */
    private static final class RecordingStorage implements Storage {

        private final URI base;
        String lastKey;

        RecordingStorage(URI base) {
            this.base = base;
        }

        @Override
        public URI baseUri() {
            return base;
        }

        @Override
        public StorageCapabilities capabilities() {
            return StorageCapabilities.builder().rangeReads(true).build();
        }

        @Override
        public Optional<StorageEntry.File> stat(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Stream<StorageEntry> list(String pattern, ListOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RangeReader openRangeReader(String key) {
            lastKey = key;
            return new NoopRangeReader();
        }

        @Override
        public ReadHandle read(String key, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File put(String key, java.nio.file.Path source, WriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.io.OutputStream openOutputStream(String key, WriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeleteResult deleteAll(Collection<String> keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI presignGet(String key, java.time.Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI presignPut(String key, java.time.Duration ttl, PresignWriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class NoopRangeReader implements RangeReader {
        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            return 0;
        }

        @Override
        public OptionalLong size() {
            return OptionalLong.empty();
        }

        @Override
        public String getSourceIdentifier() {
            return "noop";
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
