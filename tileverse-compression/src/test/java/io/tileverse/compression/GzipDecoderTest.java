/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class GzipDecoderTest {

    private static final byte[] TEXT = "tile data ".repeat(2000).getBytes(StandardCharsets.UTF_8);

    static Stream<Named<GzipDecoder>> decoders() {
        return Stream.of(named("native", GzipDecoder.nativeOrPureJava()), named("pure Java", GzipDecoder.pureJava()));
    }

    // CI runs on Linux x64, Windows x64 and macOS arm64, all bundled in the jlibdeflate jar: a failure means a
    // packaging regression
    @Test
    void nativeLoadsOnThisPlatform() {
        assertThat(GzipDecoder.nativeOrPureJava().isNative()).isTrue();
    }

    // The member sits between unrelated bytes, as in a buffer holding several parts of an archive
    @ParameterizedTest
    @MethodSource("decoders")
    void decodesText(GzipDecoder decoder) throws IOException {
        byte[] compressed = gzip(TEXT);
        byte[] padded = new byte[compressed.length + 8];
        System.arraycopy(compressed, 0, padded, 3, compressed.length);

        assertThat(decodeAll(decoder, padded, 3, compressed.length)).isEqualTo(TEXT);
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void decodesAllMembers(GzipDecoder decoder) throws IOException {
        byte[] first = "first member ".repeat(300).getBytes(StandardCharsets.UTF_8);
        byte[] second = "second ".repeat(100).getBytes(StandardCharsets.UTF_8);
        byte[] members = concat(gzip(first), gzip(second));

        assertThat(decodeAll(decoder, members)).isEqualTo(concat(first, second));
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void decodesEmptyMember(GzipDecoder decoder) throws IOException {
        byte[] compressed = gzip(new byte[0]);

        assertThat(decodeAll(decoder, compressed)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void wrongSizeFails(GzipDecoder decoder) throws IOException {
        byte[] compressed = gzip(TEXT);
        writeTrailerSize(compressed, TEXT.length + 1);

        assertThatThrownBy(() -> decodeAll(decoder, compressed)).isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void truncatedMemberFails(GzipDecoder decoder) throws IOException {
        byte[] compressed = gzip(TEXT);
        byte[] truncated = Arrays.copyOf(compressed, compressed.length - 20);

        assertThatThrownBy(() -> decodeAll(decoder, truncated)).isInstanceOf(IOException.class);
    }

    // Both inputs are shorter than a gzip header and trailer; the second one starts with a valid header
    @ParameterizedTest
    @MethodSource("decoders")
    void rejectsNonGzipBytes(GzipDecoder decoder) {
        byte[] notGzip = "not gzip".getBytes(StandardCharsets.US_ASCII);
        byte[] headerOnly = {0x1f, (byte) 0x8b, 8, 0, 0, 0, 0, 0, 0, 3};

        assertThatThrownBy(() -> decodeAll(decoder, notGzip)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> decodeAll(decoder, headerOnly)).isInstanceOf(IOException.class);
    }

    @Test
    void decodesAboveTheCap() throws IOException {
        int size = GzipDecoder.MAX_NATIVE_OUTPUT_BYTES + 1;
        byte[] compressed = gzip(new byte[size]);

        GzipDecoder decoder = GzipDecoder.nativeOrPureJava();
        try (InputStream decoded = decoder.decode(ByteBuffer.wrap(compressed))) {
            long decodedLength = decoded.transferTo(OutputStream.nullOutputStream());
            assertThat(decodedLength).isEqualTo(size);
        }
    }

    @Test
    void nativeDecodesOneMember() throws IOException {
        Optional<byte[]> decoded = decodeNatively(gzip(TEXT));

        assertThat(decoded).contains(TEXT);
    }

    // jlibdeflate decodes direct buffers through a separate native call
    @Test
    void nativeDecodesDirectBuffer() throws IOException {
        byte[] compressed = gzip(TEXT);
        ByteBuffer direct = ByteBuffer.allocateDirect(compressed.length);
        direct.put(compressed).flip();
        Optional<InputStream> decoded = GzipDecoder.nativeOrPureJava().decodeNatively(direct);
        assertThat(decoded).isPresent();
        try (InputStream stream = decoded.get()) {
            assertThat(stream.readAllBytes()).isEqualTo(TEXT);
        }
        assertThat(direct.position()).isEqualTo(direct.limit());
    }

    // The pool may lend the decoded buffer to another borrower once the stream is closed
    @ParameterizedTest
    @MethodSource("decoders")
    void closedStreamFails(GzipDecoder decoder) throws IOException {
        InputStream decoded = decoder.decode(ByteBuffer.wrap(gzip(TEXT)));
        decoded.close();
        assertThatThrownBy(decoded::read).isInstanceOf(IOException.class);
    }

    // A read-only buffer exposes no array: jlibdeflate copies it
    @ParameterizedTest
    @MethodSource("decoders")
    void decodesReadOnlyBuffer(GzipDecoder decoder) throws IOException {
        ByteBuffer readOnly = ByteBuffer.wrap(gzip(TEXT)).asReadOnlyBuffer();
        assertThat(decodeAll(decoder, readOnly)).isEqualTo(TEXT);
    }

    // A slice starts at a non-zero offset of its array
    @ParameterizedTest
    @MethodSource("decoders")
    void decodesSlicedBuffer(GzipDecoder decoder) throws IOException {
        byte[] compressed = gzip(TEXT);
        ByteBuffer padded = ByteBuffer.allocate(compressed.length + 5);
        padded.position(5);
        ByteBuffer slice = padded.slice();
        slice.put(compressed).flip();
        assertThat(slice.arrayOffset()).isEqualTo(5);
        assertThat(decodeAll(decoder, slice)).isEqualTo(TEXT);
    }

    // Several members in a direct buffer go to the JDK decoder, which reads the buffer through a stream
    @ParameterizedTest
    @MethodSource("decoders")
    void decodesDirectMembers(GzipDecoder decoder) throws IOException {
        byte[] members = concat(gzip(TEXT), gzip(TEXT));
        ByteBuffer direct = ByteBuffer.allocateDirect(members.length);
        direct.put(members).flip();
        assertThat(decodeAll(decoder, direct)).isEqualTo(concat(TEXT, TEXT));
    }

    // A blob left to the JDK decoder must still be readable from its first byte
    @Test
    void nativeLeavesSkippedInput() throws IOException {
        ByteBuffer members = ByteBuffer.wrap(concat(gzip(TEXT), gzip(TEXT)));
        Optional<InputStream> decoded = GzipDecoder.nativeOrPureJava().decodeNatively(members);
        assertThat(decoded).isEmpty();
        assertThat(members.position()).isZero();
    }

    @Test
    void nativeSkipsSeveralMembers() throws IOException {
        byte[] members = concat(gzip(TEXT), gzip(TEXT));

        assertThat(decodeNatively(members)).isEmpty();
    }

    @Test
    void nativeSkipsAboveTheCap() throws IOException {
        byte[] compressed = gzip(new byte[GzipDecoder.MAX_NATIVE_OUTPUT_BYTES + 1]);

        assertThat(decodeNatively(compressed)).isEmpty();
    }

    // DEFLATE cannot expand its input more than 1032 times
    @Test
    void nativeSkipsImplausibleSize() throws IOException {
        byte[] compressed = gzip(TEXT);
        writeTrailerSize(compressed, compressed.length * 1032 + 1);

        assertThat(decodeNatively(compressed)).isEmpty();
    }

    // Each decompressor in the pool must serve one thread at a time: a shared one would mix the outputs of two members
    @Test
    void decodesConcurrently() throws InterruptedException, ExecutionException, IOException {
        List<byte[]> sources = distinctSources(4);
        List<byte[]> members = new ArrayList<>();
        for (byte[] source : sources) {
            members.add(gzip(source));
        }
        GzipDecoder decoder = GzipDecoder.nativeOrPureJava();
        List<Callable<byte[]>> decodes = new ArrayList<>();
        for (int i = 0; i < 16 * 200; i++) {
            byte[] member = members.get(i % members.size());
            decodes.add(() -> decodeAll(decoder, member));
        }

        ExecutorService threads = Executors.newFixedThreadPool(16);
        try {
            List<Future<byte[]>> results = threads.invokeAll(decodes);
            for (int i = 0; i < results.size(); i++) {
                byte[] expected = sources.get(i % sources.size());
                assertThat(results.get(i).get()).isEqualTo(expected);
            }
        } finally {
            threads.shutdownNow();
        }
    }

    private static List<byte[]> distinctSources(int count) {
        List<byte[]> sources = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String text = ("source " + i + " ").repeat(500 * (i + 1));
            sources.add(text.getBytes(StandardCharsets.UTF_8));
        }
        return sources;
    }

    private static Optional<byte[]> decodeNatively(byte[] compressed) throws IOException {
        Optional<InputStream> decoded = GzipDecoder.nativeOrPureJava().decodeNatively(ByteBuffer.wrap(compressed));
        if (decoded.isEmpty()) {
            return Optional.empty();
        }
        try (InputStream stream = decoded.get()) {
            return Optional.of(stream.readAllBytes());
        }
    }

    private static byte[] decodeAll(GzipDecoder decoder, ByteBuffer compressed) throws IOException {
        try (InputStream decoded = decoder.decode(compressed)) {
            return decoded.readAllBytes();
        }
    }

    private static byte[] decodeAll(GzipDecoder decoder, byte[] compressed) throws IOException {
        return decodeAll(decoder, compressed, 0, compressed.length);
    }

    private static byte[] decodeAll(GzipDecoder decoder, byte[] compressed, int offset, int length)
            throws IOException {
        try (InputStream decoded = decoder.decode(ByteBuffer.wrap(compressed, offset, length))) {
            return decoded.readAllBytes();
        }
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(data);
        }
        return compressed.toByteArray();
    }

    /** Writes {@code size} into the last four bytes of a gzip member, little-endian as the format requires. */
    private static void writeTrailerSize(byte[] member, int size) {
        int start = member.length - 4;
        for (int i = 0; i < 4; i++) {
            member[start + i] = (byte) (size >>> (8 * i));
        }
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }
}
