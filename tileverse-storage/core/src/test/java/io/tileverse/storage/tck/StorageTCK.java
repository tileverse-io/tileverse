/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.tck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.ListOptions;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageCapabilities;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.StorageOutputStream;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Abstract Storage Technology Compatibility Kit. Concrete provider ITs subclass this and provide an
 * {@link #openStorage()} method that returns a freshly-isolated Storage rooted in the test container or temp directory.
 * Tests are gated by the Storage's declared capabilities and skipped via JUnit assumptions when a capability is not
 * supported.
 */
public abstract class StorageTCK {

    protected Storage storage;
    protected StorageCapabilities caps;

    /** Open a fresh, isolated Storage instance for the current test. */
    protected abstract Storage openStorage() throws IOException;

    /** Optional cleanup hook called after each test method. */
    protected void cleanUp(Storage s) throws IOException {
        // default: no-op
    }

    @BeforeEach
    void setUpTck() throws IOException {
        storage = openStorage();
        caps = storage.capabilities();
    }

    @AfterEach
    void tearDownTck() throws IOException {
        if (storage != null) {
            try {
                cleanUp(storage);
            } finally {
                storage.close();
            }
        }
    }

    protected void requireList() {
        assumeTrue(caps.list(), "list capability not supported by this backend");
    }

    protected void requireWrites() {
        assumeTrue(caps.writes(), "writes capability not supported by this backend");
    }

    protected void requireServerSideCopy() {
        assumeTrue(caps.serverSideCopy(), "serverSideCopy capability not supported by this backend");
    }

    protected void requirePresignedUrls() {
        assumeTrue(caps.presignedUrls(), "presignedUrls capability not supported by this backend");
    }

    protected void requireBulkDelete() {
        assumeTrue(caps.bulkDelete(), "bulkDelete capability not supported by this backend");
    }

    protected void requireAtomicMove() {
        assumeTrue(caps.atomicMove(), "atomicMove capability not supported by this backend");
    }

    protected void requireConditionalWrite() {
        assumeTrue(caps.conditionalWrite(), "conditionalWrite capability not supported by this backend");
    }

    // ---------------------------------------------------------------- read paths

    @Test
    void statReturnsEmptyForMissingKey() {
        Optional<StorageEntry.File> entry = storage.stat("does/not/exist");
        assertThat(entry).isEmpty();
    }

    @Test
    void existsIsFalseForMissingKey() {
        assertThat(storage.exists("does/not/exist")).isFalse();
    }

    @Test
    void putThenStatReturnsEntry() {
        requireWrites();
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        StorageEntry.File written = storage.put("hello.txt", data);
        assertThat(written.key()).isEqualTo("hello.txt");
        assertThat(written.size()).isEqualTo(data.length);

        Optional<StorageEntry.File> stat = storage.stat("hello.txt");
        assertThat(stat).isPresent();
        assertThat(stat.get().size()).isEqualTo(data.length);
    }

    @Test
    void readReturnsExactContent() throws IOException {
        requireWrites();
        byte[] data = "tileverse".getBytes(StandardCharsets.UTF_8);
        storage.put("data.bin", data);
        try (ReadHandle r = storage.read("data.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo(data);
        }
    }

    @Test
    void readFromOffsetReadsTail() throws IOException {
        requireWrites();
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        storage.put("offsets.bin", data);
        try (ReadHandle r = storage.read("offsets.bin", ReadOptions.fromOffset(4))) {
            byte[] tail = r.content().readAllBytes();
            assertThat(new String(tail, StandardCharsets.UTF_8)).isEqualTo("456789");
        }
    }

    @Test
    void readRangeReadsExactWindow() throws IOException {
        requireWrites();
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        storage.put("range.bin", data);
        try (ReadHandle r = storage.read("range.bin", ReadOptions.range(2, 5))) {
            byte[] window = r.content().readAllBytes();
            assertThat(new String(window, StandardCharsets.UTF_8)).isEqualTo("23456");
        }
    }

    /** Each row names the range of a conditional read of {@code "0123456789"}, and the content it returns. */
    static Stream<Arguments> conditionalReads() {
        return Stream.of(
                Arguments.argumentSet("whole object", 0L, OptionalLong.empty(), "0123456789"),
                Arguments.argumentSet("range", 2L, OptionalLong.of(5L), "23456"),
                Arguments.argumentSet("from an offset", 4L, OptionalLong.empty(), "456789"));
    }

    @ParameterizedTest
    @MethodSource("conditionalReads")
    protected void readOfUnmodifiedObjectFails(long offset, OptionalLong length) {
        requireWrites();
        storage.put("since.bin", "0123456789".getBytes(StandardCharsets.UTF_8));
        Instant lastModified = storage.stat("since.bin").orElseThrow().lastModified();
        ReadOptions since = ifModifiedSince(offset, length, lastModified);

        assertThatThrownBy(() -> storage.read("since.bin", since)).isInstanceOf(PreconditionFailedException.class);
    }

    @ParameterizedTest
    @MethodSource("conditionalReads")
    void readOfModifiedObjectSucceeds(long offset, OptionalLong length, String expected) throws IOException {
        requireWrites();
        storage.put("since.bin", "0123456789".getBytes(StandardCharsets.UTF_8));
        Instant lastModified = storage.stat("since.bin").orElseThrow().lastModified();
        ReadOptions since = ifModifiedSince(offset, length, lastModified.minus(Duration.ofHours(1)));

        try (ReadHandle r = storage.read("since.bin", since)) {
            byte[] content = r.content().readAllBytes();
            assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo(expected);
        }
    }

    protected static ReadOptions ifModifiedSince(long offset, OptionalLong length, Instant since) {
        return new ReadOptions(offset, length, Optional.empty(), Optional.empty(), Optional.of(since));
    }

    @ParameterizedTest
    @MethodSource("conditionalReads")
    void staleEtagReadFails(long offset, OptionalLong length) {
        requireWrites();
        String staleEtag = replacedEtag("cas.bin");
        ReadOptions ifMatch = ifMatchEtag(offset, length, staleEtag);

        assertThatThrownBy(() -> storage.read("cas.bin", ifMatch)).isInstanceOf(PreconditionFailedException.class);
    }

    @ParameterizedTest
    @MethodSource("conditionalReads")
    void currentEtagReadSucceeds(long offset, OptionalLong length, String expected) throws IOException {
        requireWrites();
        String etag = storage.put("cas.bin", utf8("0123456789")).etag().orElseThrow();
        ReadOptions ifMatch = ifMatchEtag(offset, length, etag);

        try (ReadHandle r = storage.read("cas.bin", ifMatch)) {
            byte[] content = r.content().readAllBytes();
            assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo(expected);
        }
    }

    /** Writes two contents at the key, and returns the ETag of the first. */
    private String replacedEtag(String key) {
        String etag = storage.put(key, utf8("0123456789")).etag().orElseThrow();
        storage.put(key, utf8("9876543210"));
        return etag;
    }

    private static ReadOptions ifMatchEtag(long offset, OptionalLong length, String etag) {
        return new ReadOptions(offset, length, Optional.of(etag), Optional.empty(), Optional.empty());
    }

    private static byte[] utf8(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    private String contentOf(String key) throws IOException {
        try (ReadHandle r = storage.read(key)) {
            return new String(r.content().readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void openRangeReturnsThreadSafeRangeReader() throws IOException {
        requireWrites();
        byte[] data = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        storage.put("rr.bin", data);
        try (RangeReader rr = storage.openRangeReader("rr.bin")) {
            assertThat(rr.size()).hasValue(data.length);
            // RangeReader.readRange convenience method returns an unflipped ByteBuffer
            // (position advanced by bytesRead). Flip before reading the bytes back.
            ByteBuffer buf = rr.readRange(2, 4);
            buf.flip();
            byte[] out = new byte[buf.remaining()];
            buf.get(out);
            assertThat(new String(out, StandardCharsets.UTF_8)).isEqualTo("cdef");
        }
    }

    @Test
    void openRangeReaderFromListingEntrySeedsSize() throws IOException {
        requireWrites();
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        storage.put("seeded.bin", data);
        StorageEntry.File entry = storage.stat("seeded.bin").orElseThrow();
        try (RangeReader rr = storage.openRangeReader(entry)) {
            assertThat(rr.size()).hasValue((long) data.length);
            ByteBuffer buf = rr.readRange(2, 4);
            buf.flip();
            byte[] out = new byte[buf.remaining()];
            buf.get(out);
            assertThat(new String(out, StandardCharsets.UTF_8)).isEqualTo("2345");
        }
    }

    /**
     * Round-trip a payload large enough to cross the multipart upload threshold (8 MiB for S3 / GCS, configurable for
     * Azure) and the CRT async client's default {@code minimumPartSize} (8 MiB), then read it back three ways:
     *
     * <ol>
     *   <li>Whole-object via {@link Storage#read(String)}.
     *   <li>A large interior range via {@link Storage#read(String, ReadOptions)}.
     *   <li>A large range via {@link RangeReader#readRange(long, int)}.
     * </ol>
     *
     * Exercises the multi-connection / multi-part code paths that single-byte tests don't reach: S3CrtAsyncClient's
     * split-GET for {@code read}, S3TransferManager's multipart upload for the {@code put(Path)} side, and Azure / GCS
     * chunked transfers.
     */
    @Test
    protected void multiPartRoundTrip16MiB(@TempDir Path tmp) throws IOException {
        requireWrites();
        final int size = 16 * 1024 * 1024;
        byte[] data = generateDeterministicBytes(size);
        Path src = tmp.resolve("large-source.bin");
        Files.write(src, data);
        storage.put("large.bin", src, WriteOptions.defaults());

        try (ReadHandle r = storage.read("large.bin")) {
            byte[] roundTrip = r.content().readAllBytes();
            assertThat(roundTrip).as("whole-object read length").hasSize(size);
            assertBytesEqual(data, 0, roundTrip, "whole-object read");
        }

        // Pick a window that straddles the 8 MiB CRT default part size so the split path
        // produces at least two sub-GETs of meaningful size.
        long offset = 5L * 1024 * 1024;
        int length = 6 * 1024 * 1024;
        try (ReadHandle r = storage.read("large.bin", ReadOptions.range(offset, length))) {
            byte[] window = r.content().readAllBytes();
            assertThat(window).as("interior range read length").hasSize(length);
            assertBytesEqual(data, (int) offset, window, "interior range read");
        }

        try (RangeReader rr = storage.openRangeReader("large.bin")) {
            assertThat(rr.size()).hasValue((long) size);
            ByteBuffer buf = rr.readRange(offset, length);
            buf.flip();
            byte[] window = new byte[buf.remaining()];
            buf.get(window);
            assertThat(window).as("RangeReader read length").hasSize(length);
            assertBytesEqual(data, (int) offset, window, "RangeReader.readRange");
        }
    }

    private static byte[] generateDeterministicBytes(int size) {
        byte[] out = new byte[size];
        // Cheap LCG-style fill: deterministic, avoids long blocks of zeros that some
        // backends might sparse-encode and skew the test against the wire path.
        int state = 0x12345678;
        for (int i = 0; i < size; i++) {
            state = state * 1103515245 + 12345;
            out[i] = (byte) (state >>> 16);
        }
        return out;
    }

    private static void assertBytesEqual(byte[] expected, int expectedFrom, byte[] actual, String label) {
        if (actual.length == 0) {
            assertThat(actual).as(label + " is empty").isEqualTo(new byte[0]);
            return;
        }
        for (int i = 0; i < actual.length; i++) {
            if (expected[expectedFrom + i] != actual[i]) {
                throw new AssertionError(label + " mismatch at offset " + i + " (absolute "
                        + (expectedFrom + i) + "): expected 0x"
                        + Integer.toHexString(expected[expectedFrom + i] & 0xff) + " actual 0x"
                        + Integer.toHexString(actual[i] & 0xff));
            }
        }
    }

    @Test
    void readMissingKeyThrowsNotFound() {
        // read() of a missing key throws NotFoundException; we only need to invoke read() in the lambda.
        assertThatThrownBy(() -> storage.read("missing.bin")).isInstanceOf(NotFoundException.class);
    }

    // --------------------------------------------------------------- write paths

    @Test
    void putAtomicVisibility() {
        requireWrites();
        storage.put("atomic.bin", new byte[100]);
        assertThat(storage.exists("atomic.bin")).isTrue();
        assertThat(storage.stat("atomic.bin").orElseThrow().size()).isEqualTo(100);
    }

    @Test
    void putReplacesExistingKey() {
        requireWrites();
        storage.put("replace.bin", new byte[10]);
        storage.put("replace.bin", new byte[20]);
        assertThat(storage.stat("replace.bin").orElseThrow().size()).isEqualTo(20);
    }

    @Test
    protected void putIfNotExistsRejectsExistingKey() {
        requireWrites();
        requireConditionalWrite();
        storage.put("cond.bin", new byte[5]);
        WriteOptions ifNotExists = WriteOptions.builder().ifNotExists(true).build();
        assertThatThrownBy(() -> storage.put("cond.bin", new byte[10], ifNotExists))
                .isInstanceOf(PreconditionFailedException.class);
    }

    /** Writes an object through one of the write methods of a storage. */
    @FunctionalInterface
    protected interface Writer {
        void write(Storage storage, String key, byte[] data, WriteOptions options) throws IOException;
    }

    /** Each row writes through one of the write methods: from a byte array, from a local file, or to a stream. */
    static Stream<Arguments> writers() {
        return Stream.of(
                Arguments.argumentSet("bytes", (Writer) Storage::put),
                Arguments.argumentSet("file", (Writer) StorageTCK::putFromFile),
                Arguments.argumentSet("stream", (Writer) StorageTCK::writeToStream));
    }

    private static void putFromFile(Storage storage, String key, byte[] data, WriteOptions options)
            throws IOException {
        Path source = Files.createTempFile("tck-", ".bin");
        try {
            Files.write(source, data);
            storage.put(key, source, options);
        } finally {
            Files.delete(source);
        }
    }

    /** A stream reports a failed commit as the I/O failure of its close, caused by the storage failure. */
    private static void writeToStream(Storage storage, String key, byte[] data, WriteOptions options)
            throws IOException {
        try (OutputStream out = storage.openOutputStream(key, options)) {
            out.write(data);
        } catch (IOException e) {
            if (e.getCause() instanceof StorageException failure) {
                throw failure;
            }
            throw e;
        }
    }

    @ParameterizedTest
    @MethodSource("writers")
    protected void staleEtagWriteFails(Writer writer) throws IOException {
        requireWrites();
        requireConditionalWrite();
        String staleEtag = replacedEtag("cas.bin");
        WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag(staleEtag).build();
        byte[] data = utf8("abc");

        assertThatThrownBy(() -> writer.write(storage, "cas.bin", data, ifMatch))
                .isInstanceOf(PreconditionFailedException.class);
        assertThat(contentOf("cas.bin")).isEqualTo("9876543210");
    }

    @ParameterizedTest
    @MethodSource("writers")
    void currentEtagWriteSucceeds(Writer writer) throws IOException {
        requireWrites();
        requireConditionalWrite();
        String etag = storage.put("cas.bin", utf8("0123456789")).etag().orElseThrow();
        WriteOptions ifMatch = WriteOptions.builder().ifMatchEtag(etag).build();

        writer.write(storage, "cas.bin", utf8("abc"), ifMatch);

        assertThat(contentOf("cas.bin")).isEqualTo("abc");
    }

    // The object exists: ifNotExists fails although the ETag matches.
    @ParameterizedTest
    @MethodSource("writers")
    protected void ifNotExistsWithEtagFails(Writer writer) throws IOException {
        requireWrites();
        requireConditionalWrite();
        String etag = storage.put("cas.bin", utf8("0123456789")).etag().orElseThrow();
        WriteOptions both = WriteOptions.builder().ifNotExists(true).ifMatchEtag(etag).build();
        byte[] data = utf8("abc");

        assertThatThrownBy(() -> writer.write(storage, "cas.bin", data, both))
                .isInstanceOf(PreconditionFailedException.class);
        assertThat(contentOf("cas.bin")).isEqualTo("0123456789");
    }

    @Test
    void putFromPathHonorsContent(@TempDir Path tmp) throws IOException {
        requireWrites();
        Path src = tmp.resolve("upload.bin");
        Files.write(src, "from-file".getBytes(StandardCharsets.UTF_8));
        storage.put("uploaded.bin", src, WriteOptions.defaults());
        try (ReadHandle r = storage.read("uploaded.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo("from-file".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void openOutputStreamWritesAndIsAtomic() throws IOException {
        requireWrites();
        try (OutputStream out = storage.openOutputStream("stream.bin", WriteOptions.defaults())) {
            out.write("streamed".getBytes(StandardCharsets.UTF_8));
        }
        try (ReadHandle r = storage.read("stream.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo("streamed".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    protected void openOutputStreamEmptyWriteCreatesZeroLengthBlob() throws IOException {
        requireWrites();
        try (OutputStream out = storage.openOutputStream("empty.bin", WriteOptions.defaults())) {
            // no writes
        }
        assertThat(storage.stat("empty.bin").orElseThrow().size()).isZero();
    }

    @Test
    void abortedOutputStreamLeavesNoObject() throws IOException {
        requireWrites();
        StorageOutputStream out = storage.openOutputStream("aborted.bin", WriteOptions.defaults());
        out.write("partial".getBytes(StandardCharsets.UTF_8));
        out.abort();
        out.close();
        assertThat(storage.stat("aborted.bin")).isEmpty();
    }

    @Test
    void abortAfterCloseKeepsTheCommittedObject() throws IOException {
        requireWrites();
        StorageOutputStream out = storage.openOutputStream("committed.bin", WriteOptions.defaults());
        out.write("done".getBytes(StandardCharsets.UTF_8));
        out.close();
        out.abort();
        try (ReadHandle r = storage.read("committed.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo("done".getBytes(StandardCharsets.UTF_8));
        }
    }

    // ---------------------------------------------------------- list / glob / delete / copy / move

    @Test
    void listFlatReturnsAllImmediateEntries() {
        requireWrites();
        requireList();
        storage.put("a.txt", new byte[1]);
        storage.put("b.txt", new byte[1]);
        storage.put("nested/c.txt", new byte[1]);
        try (Stream<StorageEntry> s = storage.list("", ListOptions.defaults())) {
            Set<String> keys = s.map(StorageEntry::key).collect(Collectors.toSet());
            // Default ListOptions is NOT recursive: should include direct children.
            // nested/c.txt should appear via a Prefix or Directory entry on capable backends.
            assertThat(keys).contains("a.txt", "b.txt");
        }
    }

    @Test
    void listRecursiveReturnsAllFiles() {
        requireWrites();
        requireList();
        storage.put("a.txt", new byte[1]);
        storage.put("nested/b.txt", new byte[1]);
        storage.put("nested/deep/c.txt", new byte[1]);
        try (Stream<StorageEntry> s = storage.list("**")) {
            Set<String> fileKeys = s.filter(e -> e instanceof StorageEntry.File)
                    .map(StorageEntry::key)
                    .collect(Collectors.toSet());
            assertThat(fileKeys).containsExactlyInAnyOrder("a.txt", "nested/b.txt", "nested/deep/c.txt");
        }
    }

    @Test
    void listGlobFiltersClientSide() {
        requireWrites();
        requireList();
        storage.put("data/x.parquet", new byte[1]);
        storage.put("data/y.parquet", new byte[1]);
        storage.put("data/z.txt", new byte[1]);
        try (Stream<StorageEntry> s = storage.list("**/*.parquet")) {
            Set<String> keys = s.filter(e -> e instanceof StorageEntry.File)
                    .map(StorageEntry::key)
                    .collect(Collectors.toSet());
            assertThat(keys).containsExactlyInAnyOrder("data/x.parquet", "data/y.parquet");
        }
    }

    @Test
    void deleteIsIdempotentOnMissingKey() {
        requireWrites();
        assertThatNoException().isThrownBy(() -> storage.delete("never-existed"));
    }

    @Test
    protected void deleteAllReportsPerKeyOutcome() {
        requireWrites();
        storage.put("a.txt", new byte[1]);
        storage.put("b.txt", new byte[1]);
        DeleteResult result = storage.deleteAll(List.of("a.txt", "b.txt", "missing.txt"));
        // missing.txt counts as success (idempotent)
        assertThat(result.isComplete()).isTrue();
        assertThat(result.deleted()).contains("a.txt", "b.txt");
        assertThat(storage.exists("a.txt")).isFalse();
        assertThat(storage.exists("b.txt")).isFalse();
    }

    @Test
    void copyPreservesContent() throws IOException {
        requireWrites();
        requireServerSideCopy();
        byte[] data = "copy-me".getBytes(StandardCharsets.UTF_8);
        storage.put("src.bin", data);
        storage.copy("src.bin", "dst.bin", CopyOptions.defaults());
        try (ReadHandle r = storage.read("dst.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo(data);
        }
        assertThat(storage.exists("src.bin")).isTrue();
    }

    /** Copies an object through one of the copy methods of a storage. */
    @FunctionalInterface
    protected interface Copier {
        StorageEntry.File copy(Storage storage, String srcKey, String dstKey, CopyOptions options);
    }

    /** Each row copies an object within the storage, to a storage of the same backend, or moves it. */
    static Stream<Arguments> copiers() {
        return Stream.of(
                Arguments.argumentSet("copy", (Copier) Storage::copy),
                Arguments.argumentSet("copy to a storage", (Copier) (s, src, dst, o) -> s.copy(src, s, dst, o)),
                Arguments.argumentSet("move", (Copier) Storage::move));
    }

    @ParameterizedTest
    @MethodSource("copiers")
    void staleSourceEtagCopyFails(Copier copier) throws IOException {
        requireWrites();
        requireServerSideCopy();
        String staleEtag = replacedEtag("src.bin");
        CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag(staleEtag).build();

        assertThatThrownBy(() -> copier.copy(storage, "src.bin", "dst.bin", ifMatch))
                .isInstanceOf(PreconditionFailedException.class);
        assertThat(storage.exists("dst.bin")).isFalse();
        assertThat(contentOf("src.bin")).isEqualTo("9876543210");
    }

    @ParameterizedTest
    @MethodSource("copiers")
    void currentSourceEtagCopies(Copier copier) throws IOException {
        requireWrites();
        requireServerSideCopy();
        String etag = storage.put("src.bin", utf8("0123456789")).etag().orElseThrow();
        CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag(etag).build();

        copier.copy(storage, "src.bin", "dst.bin", ifMatch);

        assertThat(contentOf("dst.bin")).isEqualTo("0123456789");
    }

    @Test
    void moveIsAtomicWhereSupported() throws IOException {
        requireWrites();
        requireAtomicMove();
        byte[] data = "move-me".getBytes(StandardCharsets.UTF_8);
        storage.put("src.bin", data);
        storage.move("src.bin", "moved.bin");
        try (ReadHandle r = storage.read("moved.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo(data);
        }
        assertThat(storage.exists("src.bin")).isFalse();
    }

    @Test
    void moveFallbackOnNonAtomicBackends() throws IOException {
        requireWrites();
        assumeTrue(
                !caps.atomicMove() && caps.serverSideCopy(),
                "this test only runs when atomicMove is false but copy is supported");
        byte[] data = "move-me".getBytes(StandardCharsets.UTF_8);
        storage.put("src2.bin", data);
        storage.move("src2.bin", "moved2.bin");
        try (ReadHandle r = storage.read("moved2.bin")) {
            assertThat(r.content().readAllBytes()).isEqualTo(data);
        }
        assertThat(storage.exists("src2.bin")).isFalse();
    }

    @Test
    void presignGetUnsupportedThrows() {
        assumeTrue(!caps.presignedUrls(), "this test runs only on backends without presign support");
        Duration ttl = Duration.ofMinutes(1);
        assertThatThrownBy(() -> storage.presignGet("k", ttl)).isInstanceOf(UnsupportedCapabilityException.class);
    }

    @Test
    void moveWithIfNotExistsAtDestinationFailsIfDestinationExists() {
        if (!storage.capabilities().writes()) {
            return;
        }
        storage.put("src.bin", new byte[] {1, 2, 3});
        storage.put("dst.bin", new byte[] {9});
        CopyOptions ifNotExistsAtDestination =
                CopyOptions.builder().ifNotExistsAtDestination(true).build();
        assertThatThrownBy(() -> storage.move("src.bin", "dst.bin", ifNotExistsAtDestination))
                .isInstanceOf(PreconditionFailedException.class);
    }

    @Test
    void readReturnsSameMetadataAsStat() throws Exception {
        if (!storage.capabilities().writes()) {
            return;
        }
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        storage.put("read-meta.txt", payload);
        StorageEntry.File statResult = storage.stat("read-meta.txt").orElseThrow();
        try (ReadHandle r = storage.read("read-meta.txt")) {
            byte[] got = r.content().readAllBytes();
            assertThat(got).isEqualTo(payload);
            assertThat(r.metadata().size()).isEqualTo(payload.length);
            assertThat(r.metadata().etag()).isEqualTo(statResult.etag());
            assertThat(r.metadata().key()).isEqualTo(statResult.key());
        }
    }

    @Test
    void deleteAllPartitionsExistingFromMissingWhenCapable() {
        if (!storage.capabilities().writes()) {
            return;
        }
        if (!storage.capabilities().deleteReportsExistence()) {
            return;
        }
        storage.put("a.txt", "a".getBytes(StandardCharsets.UTF_8));
        storage.put("b.txt", "b".getBytes(StandardCharsets.UTF_8));
        DeleteResult r = storage.deleteAll(List.of("a.txt", "b.txt", "c.txt"));
        assertThat(r.deleted()).containsExactlyInAnyOrder("a.txt", "b.txt");
        assertThat(r.didNotExist()).containsExactly("c.txt");
        assertThat(r.failed()).isEmpty();
    }

    @Test
    protected void deleteAllLumpsAllIntoDeletedWhenIncapable() {
        if (!storage.capabilities().writes()) {
            return;
        }
        if (storage.capabilities().deleteReportsExistence()) {
            return;
        }
        storage.put("a.txt", "a".getBytes(StandardCharsets.UTF_8));
        DeleteResult r = storage.deleteAll(List.of("a.txt", "missing.txt"));
        assertThat(r.deleted()).containsExactlyInAnyOrder("a.txt", "missing.txt");
        assertThat(r.didNotExist()).isEmpty();
    }

    @Test
    void nonRecursiveListEmitsDirectoryWhenCapable() {
        if (!storage.capabilities().writes() || !storage.capabilities().list()) {
            return;
        }
        storage.put("a/b/file.txt", "x".getBytes(StandardCharsets.UTF_8));
        try (java.util.stream.Stream<StorageEntry> s = storage.list("a/")) {
            java.util.List<StorageEntry> entries = s.toList();
            if (storage.capabilities().realDirectories()) {
                assertThat(entries).hasOnlyElementsOfType(StorageEntry.Directory.class);
            } else {
                assertThat(entries).hasOnlyElementsOfType(StorageEntry.Prefix.class);
            }
        }
    }

    @Test
    void recursiveListEmitsOnlyFiles() {
        if (!storage.capabilities().writes() || !storage.capabilities().list()) {
            return;
        }
        storage.put("r/x/y.txt", "x".getBytes(StandardCharsets.UTF_8));
        try (java.util.stream.Stream<StorageEntry> s = storage.list("r/**")) {
            java.util.List<StorageEntry> entries = s.toList();
            assertThat(entries).hasOnlyElementsOfType(StorageEntry.File.class);
        }
    }
}
