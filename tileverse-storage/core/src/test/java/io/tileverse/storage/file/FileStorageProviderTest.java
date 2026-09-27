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
package io.tileverse.storage.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileStorageProviderTest {

    private final FileStorageProvider provider = new FileStorageProvider();

    @Test
    void acceptsExistingDirectory(@TempDir Path tmp) throws IOException {
        StorageConfig config = new StorageConfig(tmp.toUri());
        try (Storage storage = provider.createStorage(config)) {
            assertThat(storage.baseUri()).isEqualTo(tmp.toUri());
        }
    }

    @Test
    void rejectsRegularFileUri(@TempDir Path tmp) throws IOException {
        Path file = Files.createFile(tmp.resolve("data.pmtiles"));
        StorageConfig config = new StorageConfig(file.toUri());

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a directory")
                .hasMessageContaining(file.toString());
    }

    @Test
    void rejectsMissingPath(@TempDir Path tmp) {
        Path missing = tmp.resolve("does-not-exist");
        StorageConfig config = new StorageConfig(missing.toUri());

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must point to an existing directory")
                .hasMessageContaining(missing.toString());

        assertThat(Files.exists(missing))
                .as("provider must not materialize the missing path")
                .isFalse();
    }

    @Test
    void rejectsSubMillisecondIdleTimeoutNamingTheParameter(@TempDir Path tmp) {
        StorageConfig config = new StorageConfig(tmp.toUri())
                .setParameter(FileStorageProvider.FILE_IDLE_TIMEOUT.key(), Duration.ofNanos(500_000));

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(FileStorageProvider.FILE_IDLE_TIMEOUT.key())
                .hasMessageContaining("millisecond");
    }

    @Test
    void idleTimeoutDescriptionStatesTheFloor() {
        assertThat(FileStorageProvider.FILE_IDLE_TIMEOUT.description()).contains("1 millisecond");
    }

    @Nested
    class SingleFileEntryPoint {

        @Test
        void opensAReaderOverOneFile(@TempDir Path tmp) throws IOException {
            Path file = Files.writeString(tmp.resolve("data.bin"), "hello world");

            try (RangeReader reader = FileStorageProvider.openRangeReader(file)) {
                assertThat(reader.size()).hasValue(11);
                assertThat(reader.getSourceIdentifier())
                        .isEqualTo(file.toRealPath().toString());
                ByteBuffer read = reader.readRange(6, 5).flip();
                String content = StandardCharsets.UTF_8.decode(read).toString();
                assertThat(content).isEqualTo("world");
            }
        }

        @Test
        void oneArgumentFormUsesTheClassDefaultIdleTimeout(@TempDir Path tmp) throws IOException {
            Path file = Files.writeString(tmp.resolve("data.bin"), "hello world");

            try (RangeReader reader = FileStorageProvider.openRangeReader(file)) {
                assertThat(((FileRangeReader) reader).idleTimeout()).isEqualTo(FileRangeReader.DEFAULT_IDLE_TIMEOUT);
            }
        }

        @Test
        void twoArgumentFormHonorsTheGivenIdleTimeout(@TempDir Path tmp) throws IOException {
            Path file = Files.writeString(tmp.resolve("data.bin"), "hello world");

            try (RangeReader reader = FileStorageProvider.openRangeReader(file, Duration.ofSeconds(5))) {
                assertThat(((FileRangeReader) reader).idleTimeout()).isEqualTo(Duration.ofSeconds(5));
            }
        }

        @Test
        void missingFileIsNotFound(@TempDir Path tmp) {
            Path missing = tmp.resolve("missing.bin");

            assertThatThrownBy(() -> FileStorageProvider.openRangeReader(missing))
                    .isInstanceOf(NotFoundException.class)
                    .hasMessageContaining("missing.bin");
        }

        @Test
        void directoryIsRejected(@TempDir Path tmp) {
            assertThatThrownBy(() -> FileStorageProvider.openRangeReader(tmp))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(tmp.toString());
        }

        @Test
        void invalidIdleTimeoutIsRejected(@TempDir Path tmp) throws IOException {
            Path file = Files.writeString(tmp.resolve("data.bin"), "hello world");
            Duration negative = Duration.ofSeconds(-1);
            Duration halfAMillisecond = Duration.ofNanos(500_000);

            assertThatThrownBy(() -> FileStorageProvider.openRangeReader(file, negative))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> FileStorageProvider.openRangeReader(file, halfAMillisecond))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("millisecond");
        }

        @Test
        void nullArgumentsAreRejected(@TempDir Path tmp) throws IOException {
            Path file = Files.writeString(tmp.resolve("data.bin"), "hello world");

            assertThatThrownBy(() -> FileStorageProvider.openRangeReader(null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> FileStorageProvider.openRangeReader(file, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    void rejectsMissingLeafShapedUriWithoutCreatingDirectory(@TempDir Path tmp) {
        // Specific regression: a non-existent file:///.../missing.pmtiles previously created a
        // directory at that path. Confirm the strict contract leaves the filesystem untouched.
        Path missingLeaf = tmp.resolve("world.pmtiles");
        StorageConfig config = new StorageConfig(missingLeaf.toUri());

        assertThatThrownBy(() -> provider.createStorage(config)).isInstanceOf(IllegalArgumentException.class);

        assertThat(Files.exists(missingLeaf)).isFalse();
    }
}
