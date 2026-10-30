/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

    @Test
    void etagChangesOnEachWrite(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            StorageEntry.File first = s.put("a.txt", bytes("one"));
            StorageEntry.File second = s.put("a.txt", bytes("two"));

            assertThat(first.etag()).isPresent();
            assertThat(second.etag()).isPresent().isNotEqualTo(first.etag());
            assertThat(s.stat("a.txt").orElseThrow().etag()).isEqualTo(second.etag());
        }
    }

    @Test
    void listReportsTheEtag(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            String etag = s.put("a.txt", bytes("one")).etag().orElseThrow();

            try (Stream<StorageEntry> entries = s.list("*.txt")) {
                Stream<Optional<String>> etags =
                        entries.map(StorageEntry.File.class::cast).map(StorageEntry.File::etag);
                assertThat(etags).containsExactly(Optional.of(etag));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("writes")
    void writeRejectsAnotherEtag(Write write, @TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("a.txt", bytes("one"));
            byte[] two = bytes("two");
            WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag("no-such-etag").build();

            assertThatThrownBy(() -> write.apply(s, "a.txt", two, ifMatch))
                    .isInstanceOf(PreconditionFailedException.class);
            assertThat(Files.readString(tmp.resolve("a.txt"))).isEqualTo("one");
        }
    }

    @ParameterizedTest
    @MethodSource("writes")
    void writeReplacesTheEtagMatch(Write write, @TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            String etag = s.put("a.txt", bytes("one")).etag().orElseThrow();
            WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag(etag).build();

            write.apply(s, "a.txt", bytes("two"), ifMatch);

            assertThat(Files.readString(tmp.resolve("a.txt"))).isEqualTo("two");
        }
    }

    @ParameterizedTest
    @MethodSource("writes")
    void writeIfMatchRejectsMissing(Write write, @TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            byte[] two = bytes("two");
            WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag("no-such-etag").build();

            assertThatThrownBy(() -> write.apply(s, "a.txt", two, ifMatch))
                    .isInstanceOf(PreconditionFailedException.class);
            assertThat(tmp.resolve("a.txt")).doesNotExist();
        }
    }

    // Another writer may replace the file while the stream is open
    @Test
    void streamChecksEtagAtClose(@TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            String etag = s.put("a.txt", bytes("one")).etag().orElseThrow();
            WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag(etag).build();
            StorageOutputStream out = s.openOutputStream("a.txt", ifMatch);
            out.write(bytes("two"));
            s.put("a.txt", bytes("three"));

            assertThatThrownBy(out::close)
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(PreconditionFailedException.class);
            assertThat(Files.readString(tmp.resolve("a.txt"))).isEqualTo("three");
        }
    }

    // Writers holding the ETag of one version race to replace it: one of them wins
    @Test
    void oneWriterWinsAnEtag(@TempDir Path tmp) throws Exception {
        int writers = 16;
        try (FileStorage s = new FileStorage(tmp)) {
            String etag = s.put("a.txt", bytes("zero")).etag().orElseThrow();
            WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag(etag).build();
            CyclicBarrier start = new CyclicBarrier(writers);
            List<Callable<Boolean>> writes = new ArrayList<>();
            for (int i = 0; i < writers; i++) {
                byte[] data = bytes("writer " + i);
                writes.add(() -> tryWrite(s, start, data, ifMatch));
            }

            ExecutorService executor = Executors.newFixedThreadPool(writers);
            try {
                assertThat(wins(executor.invokeAll(writes))).isEqualTo(1);
            } finally {
                executor.shutdownNow();
            }
            try (Stream<Path> entries = Files.list(tmp)) {
                assertThat(entries).containsExactly(tmp.resolve("a.txt"));
            }
        }
    }

    private static boolean tryWrite(Storage storage, CyclicBarrier start, byte[] data, WriteOptions options)
            throws Exception {
        start.await();
        try {
            storage.put("a.txt", data, options);
            return true;
        } catch (PreconditionFailedException e) {
            return false;
        }
    }

    private static int wins(List<Future<Boolean>> writes) throws Exception {
        int wins = 0;
        for (Future<Boolean> write : writes) {
            if (write.get()) {
                wins++;
            }
        }
        return wins;
    }

    /** One of the three ways of writing a key. */
    @FunctionalInterface
    interface Write {
        void apply(Storage storage, String key, byte[] data, WriteOptions options) throws IOException;
    }

    static Stream<Named<Write>> writes() {
        return Stream.of(
                Named.of("put bytes", Storage::put),
                Named.of("put file", FileStorageTest::putFile),
                Named.of("output stream", FileStorageTest::writeStream));
    }

    private static void putFile(Storage storage, String key, byte[] data, WriteOptions options) throws IOException {
        Path source = Files.createTempFile("source-", ".bin");
        try {
            Files.write(source, data);
            storage.put(key, source, options);
        } finally {
            Files.deleteIfExists(source);
        }
    }

    private static void writeStream(Storage storage, String key, byte[] data, WriteOptions options)
            throws IOException {
        try (StorageOutputStream out = storage.openOutputStream(key, options)) {
            out.write(data);
        }
    }

    @Test
    void readRejectsAnotherEtag(@TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("a.txt", bytes("one"));
            ReadOptions ifMatch = ifMatch("no-such-etag");

            assertThatThrownBy(() -> s.read("a.txt", ifMatch)).isInstanceOf(PreconditionFailedException.class);
        }
    }

    @Test
    void readReturnsTheEtagMatch(@TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            Optional<String> etag = s.put("a.txt", bytes("one")).etag();

            try (ReadHandle read = s.read("a.txt", ifMatch(etag.orElseThrow()))) {
                assertThat(read.content().readAllBytes()).isEqualTo(bytes("one"));
                assertThat(read.metadata().etag()).isEqualTo(etag);
            }
        }
    }

    @Test
    void readRejectsAnUnmodifiedFile(@TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            Instant lastModified = s.put("a.txt", bytes("one")).lastModified();
            ReadOptions unmodified = ifModifiedSince(lastModified);

            assertThatThrownBy(() -> s.read("a.txt", unmodified)).isInstanceOf(PreconditionFailedException.class);
            try (ReadHandle read = s.read("a.txt", ifModifiedSince(lastModified.minusSeconds(1)))) {
                assertThat(read.content().readAllBytes()).isEqualTo(bytes("one"));
            }
        }
    }

    private static ReadOptions ifMatch(String etag) {
        return new ReadOptions(0L, OptionalLong.empty(), Optional.of(etag), Optional.empty(), Optional.empty());
    }

    private static ReadOptions ifModifiedSince(Instant instant) {
        return new ReadOptions(0L, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.of(instant));
    }

    @ParameterizedTest
    @MethodSource("copies")
    void copyRejectsAnotherEtag(Copy copy, @TempDir Path tmp) {
        try (FileStorage s = new FileStorage(tmp)) {
            s.put("a.txt", bytes("one"));
            CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag("no-such-etag").build();

            assertThatThrownBy(() -> copy.apply(s, "a.txt", "b.txt", ifMatch))
                    .isInstanceOf(PreconditionFailedException.class);
            assertThat(tmp.resolve("a.txt")).exists();
            assertThat(tmp.resolve("b.txt")).doesNotExist();
        }
    }

    @ParameterizedTest
    @MethodSource("copies")
    void copyAcceptsTheEtagMatch(Copy copy, @TempDir Path tmp) throws IOException {
        try (FileStorage s = new FileStorage(tmp)) {
            String etag = s.put("a.txt", bytes("one")).etag().orElseThrow();
            CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag(etag).build();

            copy.apply(s, "a.txt", "b.txt", ifMatch);

            assertThat(Files.readString(tmp.resolve("b.txt"))).isEqualTo("one");
        }
    }

    /** One of the three ways of copying a key; a move copies before it deletes. */
    @FunctionalInterface
    interface Copy {
        void apply(FileStorage storage, String srcKey, String dstKey, CopyOptions options);
    }

    // Passing the storage as its own destination takes the path of a copy to another local storage
    static Stream<Named<Copy>> copies() {
        return Stream.of(
                Named.of("copy", FileStorage::copy),
                Named.of("copy to a storage", (storage, src, dst, options) -> storage.copy(src, storage, dst, options)),
                Named.of("move", FileStorage::move));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
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

        @Test
        void listsPastASymbolicLinkToAMissingFile(@TempDir Path tmp) throws IOException {
            Files.writeString(tmp.resolve("real.parquet"), "r");
            Files.createSymbolicLink(tmp.resolve("broken.parquet"), tmp.resolve("gone.parquet"));

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "*.parquet")).containsExactly("real.parquet");
            }
        }

        @Test
        void walksPastASymbolicLinkToAMissingFile(@TempDir Path tmp) throws IOException {
            Files.createDirectories(tmp.resolve("data"));
            Files.writeString(tmp.resolve("data/real.parquet"), "r");
            Files.createSymbolicLink(tmp.resolve("data/broken.parquet"), tmp.resolve("data/gone.parquet"));

            try (Storage storage = new FileStorage(tmp)) {
                assertThat(keys(storage, "**/*.parquet")).containsExactly("data/real.parquet");
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
