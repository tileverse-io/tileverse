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
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileStorageTest {

    @Test
    void putCreatesNestedDirectories(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("a/b/c/d.txt", "ok".getBytes(StandardCharsets.UTF_8));
            assertThat(Files.exists(tmp.resolve("a/b/c/d.txt"))).isTrue();
        }
    }

    @Test
    void atomicRenameLeavesNoTempfileArtifacts(@TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("file.bin", new byte[100]);
            try (Stream<Path> entries = Files.list(tmp)) {
                assertThat(entries).noneMatch(p -> p.getFileName().toString().startsWith(".tmp-"));
            }
        }
    }

    @Test
    void openInputStreamMissingThrowsNotFound(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            assertThatThrownBy(() -> s.read("missing.bin").close()).isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    void putIfNotExistsRejectsExisting(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("k", new byte[1]);
            assertThatThrownBy(() -> s.put(
                            "k",
                            new byte[2],
                            WriteOptions.builder().ifNotExists(true).build()))
                    .isInstanceOf(PreconditionFailedException.class);
        }
    }

    @Test
    void putIfNotExistsRejectionLeavesContentAndDirectoryStateUntouched(@TempDir Path tmp) throws IOException {
        // The precondition check must fire before any write-side I/O. After rejection: existing content
        // is byte-identical, no tempfile artifacts are left behind, and no spurious sibling directories
        // were materialized as a side effect of the would-be write.
        try (FileStorage s = new FileStorage(tmp)) {
            byte[] original = "original".getBytes(StandardCharsets.UTF_8);
            s.put("dir/existing.bin", original);

            assertThatThrownBy(() -> s.put(
                            "dir/existing.bin",
                            "replaced".getBytes(StandardCharsets.UTF_8),
                            WriteOptions.builder().ifNotExists(true).build()))
                    .isInstanceOf(PreconditionFailedException.class);

            assertThat(Files.readAllBytes(tmp.resolve("dir/existing.bin"))).isEqualTo(original);
            try (Stream<Path> entries = Files.list(tmp.resolve("dir"))) {
                assertThat(entries).noneMatch(p -> p.getFileName().toString().startsWith(".tmp-"));
            }
            try (Stream<Path> rootEntries = Files.list(tmp)) {
                assertThat(rootEntries.map(p -> p.getFileName().toString())).containsExactly("dir");
            }
        }
    }
}
