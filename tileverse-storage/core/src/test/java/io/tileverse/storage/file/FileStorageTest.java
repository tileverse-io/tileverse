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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
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
    void abortedOutputStreamLeavesNoStagedFile(@TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            StorageOutputStream out = s.openOutputStream("file.bin", WriteOptions.defaults());
            out.write(new byte[100]);
            out.abort();
            try (Stream<Path> entries = Files.list(tmp)) {
                assertThat(entries).isEmpty();
            }
        }
    }

    @Test
    void openInputStreamMissingThrowsNotFound(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            assertThatThrownBy(() -> s.read("missing.bin")).isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    void openRangeReaderMissingThrowsNotFound(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            assertThatThrownBy(() -> s.openRangeReader("missing.bin"))
                    .isInstanceOf(NotFoundException.class)
                    .hasMessageContaining("missing.bin");
        }
    }

    @Test
    void openRangeReaderRejectsADirectoryKey(@TempDir Path tmp) throws IOException {
        Files.createDirectory(tmp.resolve("dir"));
        try (FileStorage s = new FileStorage(tmp)) {
            assertThatThrownBy(() -> s.openRangeReader("dir")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void putIfNotExistsRejectsExisting(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("k", new byte[1]);
            WriteOptions ifNotExists = WriteOptions.builder().ifNotExists(true).build();
            assertThatThrownBy(() -> s.put("k", new byte[2], ifNotExists))
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

            byte[] replaced = "replaced".getBytes(StandardCharsets.UTF_8);
            WriteOptions ifNotExists = WriteOptions.builder().ifNotExists(true).build();
            assertThatThrownBy(() -> s.put("dir/existing.bin", replaced, ifNotExists))
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

    /**
     * How a listing spells its keys, what a prefix naming a file answers, and how a symbolic link is named.
     *
     * <p>A test of a mis-spelled prefix assumes a case-insensitive filesystem. On a case-sensitive one such a prefix
     * resolves to nothing, leaving no spelling to correct.
     */
    @Nested
    class Listing {

        @Test
        void listsKeysByTheNameOnDisk(@TempDir Path tmp) throws IOException {
            Files.createDirectories(tmp.resolve("data"));
            Files.writeString(tmp.resolve("data/a.parquet"), "a");
            assumeTrue(Files.exists(tmp.resolve("DATA")), "needs a case-insensitive filesystem");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "data/*.parquet")).containsExactly("data/a.parquet");
                assertThat(keys(storage, "DATA/*.parquet")).isEmpty();
            }
        }

        @Test
        void listsNothingForAMisspelledDirectoryPrefix(@TempDir Path tmp) throws IOException {
            Files.createDirectories(tmp.resolve("data"));
            Files.writeString(tmp.resolve("data/a.parquet"), "a");
            assumeTrue(Files.exists(tmp.resolve("DATA")), "needs a case-insensitive filesystem");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "DATA/")).isEmpty();
                assertThat(keys(storage, "DATA")).isEmpty();
            }
        }

        @Test
        void listsNothingForAMisspelledFileName(@TempDir Path tmp) throws IOException {
            Files.writeString(tmp.resolve("plain.parquet"), "p");
            assumeTrue(Files.exists(tmp.resolve("PLAIN.parquet")), "needs a case-insensitive filesystem");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "PLAIN.parquet")).isEmpty();
            }
        }

        @Test
        void listsUnderARootReachedThroughASymbolicLink(@TempDir Path tmp) throws IOException {
            Path real = Files.createDirectory(tmp.resolve("real"));
            Files.writeString(real.resolve("a.parquet"), "a");
            Path link = Files.createSymbolicLink(tmp.resolve("link"), real);

            try (Storage storage = new FileStorage(link)) {
                assertThat(keys(storage, "*.parquet")).containsExactly("a.parquet");
            }
        }

        @Test
        void listsASymbolicLinkPrefixUnderItsOwnName(@TempDir Path tmp) throws IOException {
            Files.createDirectories(tmp.resolve("data"));
            Files.writeString(tmp.resolve("data/a.parquet"), "a");
            Files.createSymbolicLink(tmp.resolve("link"), tmp.resolve("data"));

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "link/*.parquet")).containsExactly("link/a.parquet");
            }
        }

        @Test
        void listsTheFileNamedByAPatternWithoutGlobCharacters(@TempDir Path tmp) throws IOException {
            Files.writeString(tmp.resolve("plain.parquet"), "p");
            Files.writeString(tmp.resolve("plain.parquet.bak"), "b");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "plain.parquet")).containsExactly("plain.parquet");
            }
        }

        @Test
        void listsTheChildrenOfADirectoryNamedByAPatternWithoutGlobCharacters(@TempDir Path tmp) throws IOException {
            Files.createDirectories(tmp.resolve("data"));
            Files.writeString(tmp.resolve("data/a.parquet"), "a");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "data")).containsExactly("data/a.parquet");
            }
        }

        @Test
        void listsNothingForAPatternNamingNothing(@TempDir Path tmp) throws IOException {
            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "absent.parquet")).isEmpty();
            }
        }

        @Test
        void listsNothingForAGlobUnderAFileName(@TempDir Path tmp) throws IOException {
            Files.writeString(tmp.resolve("plain.parquet"), "p");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "plain.parquet/*.txt")).isEmpty();
            }
        }

        @Test
        void listsNothingWhenAskedForTheChildrenOfAFile(@TempDir Path tmp) throws IOException {
            Files.writeString(tmp.resolve("plain.parquet"), "p");

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "plain.parquet/")).isEmpty();
            }
        }

        @Test
        void listsASymbolicLinkToAFileUnderItsOwnName(@TempDir Path tmp) throws IOException {
            Files.writeString(tmp.resolve("real.parquet"), "r");
            Files.createSymbolicLink(tmp.resolve("link.parquet"), tmp.resolve("real.parquet"));

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "link.parquet")).containsExactly("link.parquet");
            }
        }

        private List<String> keys(Storage storage, String pattern) {
            try (Stream<StorageEntry> entries = storage.list(pattern)) {
                return entries.filter(entry -> entry instanceof StorageEntry.File)
                        .map(StorageEntry::key)
                        .sorted()
                        .toList();
            }
        }
    }
}
