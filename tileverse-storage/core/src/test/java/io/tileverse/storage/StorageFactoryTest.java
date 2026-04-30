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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageFactoryTest {

    @Test
    void openFileUriReturnsFileStorage(@TempDir Path tmp) throws IOException {
        try (Storage s = StorageFactory.open(tmp.toUri())) {
            assertThat(s).isNotNull();
            assertThat(s.capabilities().writes()).isTrue();
            assertThat(s.capabilities().list()).isTrue();
        }
    }

    @Test
    void openHttpUriReturnsHttpStorage() throws IOException {
        try (Storage s = StorageFactory.open(java.net.URI.create("http://example.com/"))) {
            assertThat(s).isNotNull();
            assertThat(s.capabilities().writes()).isFalse();
            assertThat(s.capabilities().rangeReads()).isTrue();
        }
    }

    @Test
    void openUnknownSchemeThrows() {
        assertThatThrownBy(() -> StorageFactory.open(java.net.URI.create("unknown://x/y")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void openWithPropertiesAcceptsBackendConfig(@TempDir Path tmp) throws IOException {
        java.util.Properties props = new java.util.Properties();
        try (Storage s = StorageFactory.open(tmp.toUri(), props)) {
            assertThat(s).isNotNull();
            assertThat(s.capabilities().writes()).isTrue();
        }
    }

    @Test
    void openWithPropertiesOnlyReadsUriFromProps(@TempDir Path tmp) throws IOException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("storage.uri", tmp.toUri().toString());
        try (Storage s = StorageFactory.open(props)) {
            assertThat(s.baseUri().toString()).startsWith("file:");
        }
    }

    @Test
    void findProviderResolvesByUri(@TempDir Path tmp) {
        io.tileverse.storage.spi.StorageConfig config = new io.tileverse.storage.spi.StorageConfig().uri(tmp.toUri());
        assertThat(StorageFactory.findProvider(config).getId()).isEqualTo("file");
    }

    @Test
    void openRangeReaderReadsLeafFile(@TempDir Path tmp) throws IOException {
        byte[] payload = "hello tileverse".getBytes(StandardCharsets.UTF_8);
        Path file = tmp.resolve("greeting.bin");
        Files.write(file, payload);
        try (RangeReader r = StorageFactory.openRangeReader(file.toUri())) {
            assertThat(r.size()).hasValue((long) payload.length);
            ByteBuffer buf = r.readRange(0, payload.length);
            buf.flip();
            byte[] got = new byte[buf.remaining()];
            buf.get(got);
            assertThat(got).isEqualTo(payload);
        }
    }

    @Test
    void openRangeReaderRejectsContainerUri(@TempDir Path tmp) {
        // tmp.toUri() is a directory, not a leaf object.
        assertThatThrownBy(() -> StorageFactory.openRangeReader(tmp.toUri()))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);
    }

    @Test
    void openRangeReaderHttpUriBuildsHttpReader() throws IOException {
        // No actual server needed - just verify the dispatch picks the HTTP provider
        // and constructs a reader. size() will fail to fetch but the construction works.
        URI uri = URI.create("http://127.0.0.1:1/notthere/file.bin");
        try (RangeReader r = StorageFactory.openRangeReader(uri)) {
            assertThat(r).isNotNull();
            assertThat(r.getSourceIdentifier()).contains("file.bin");
        }
    }

    @Test
    void openRangeReaderClosesOwnedStorageOnClose(@TempDir Path tmp) throws IOException {
        Files.write(tmp.resolve("x.bin"), new byte[] {1, 2, 3, 4});
        RangeReader r = StorageFactory.openRangeReader(tmp.resolve("x.bin").toUri());
        // Read once to verify the reader is functional, then close.
        ByteBuffer buf = r.readRange(0, 4);
        buf.flip();
        assertThat(buf.remaining()).isEqualTo(4);
        r.close();
        // Idempotent close shouldn't blow up.
        r.close();
    }
}
