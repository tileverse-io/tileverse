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

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileStorageProviderTest {

    private final FileStorageProvider provider = new FileStorageProvider();

    @Test
    void acceptsExistingDirectory(@TempDir Path tmp) throws IOException {
        StorageConfig config = new StorageConfig().baseUri(tmp.toUri());
        try (Storage storage = provider.createStorage(config)) {
            assertThat(storage.baseUri()).isEqualTo(tmp.toUri());
        }
    }

    @Test
    void rejectsRegularFileUri(@TempDir Path tmp) throws IOException {
        Path file = Files.createFile(tmp.resolve("data.pmtiles"));
        StorageConfig config = new StorageConfig().baseUri(file.toUri());

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a directory")
                .hasMessageContaining(file.toString());
    }

    @Test
    void rejectsMissingPath(@TempDir Path tmp) {
        Path missing = tmp.resolve("does-not-exist");
        StorageConfig config = new StorageConfig().baseUri(missing.toUri());

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must point to an existing directory")
                .hasMessageContaining(missing.toString());

        assertThat(Files.exists(missing))
                .as("provider must not materialize the missing path")
                .isFalse();
    }

    @Test
    void rejectsMissingLeafShapedUriWithoutCreatingDirectory(@TempDir Path tmp) {
        // Specific regression: a non-existent file:///.../missing.pmtiles previously created a
        // directory at that path. Confirm the strict contract leaves the filesystem untouched.
        Path missingLeaf = tmp.resolve("world.pmtiles");
        StorageConfig config = new StorageConfig().baseUri(missingLeaf.toUri());

        assertThatThrownBy(() -> provider.createStorage(config)).isInstanceOf(IllegalArgumentException.class);

        assertThat(Files.exists(missingLeaf)).isFalse();
    }
}
