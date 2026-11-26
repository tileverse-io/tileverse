/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CodecTest {

    private static final String TEXT = "tile data ".repeat(20);

    // TEXT compressed by the command line tools: gzip -n (Apple gzip 457), zstd 1.5.7 and brotli 1.2.0
    private static final byte[] GZIP_TEXT = {
        31, -117, 8, 0, 0, 0, 0, 0, 0, 3, 43, -55, -52, 73, 85, 72, 73, 44, 73, 84, 40, 25, -46, 44, 0, -4, 22, -65, 11,
        -56, 0, 0, 0
    };

    private static final byte[] ZSTD_TEXT = {
        40, -75, 47, -3, 4, 88, -115, 0, 0, 80, 116, 105, 108, 101, 32, 100, 97, 116, 97, 32, 1, 0, -69, 82, 5, 9, 95,
        70, 107, 49
    };

    private static final byte[] BROTLI_TEXT = {
        31, -57, 0, -8, -115, -108, -87, -29, 91, -125, -112, -79, -47, -55, -80, -78, -76, 16, 48, 20, 0, -1, 66, 3
    };

    static Stream<Arguments> compressedText() {
        return Stream.of(
                arguments(named("none", Codec.none()), TEXT.getBytes(StandardCharsets.UTF_8)),
                arguments(named("gzip", Codec.gzip()), GZIP_TEXT),
                arguments(named("brotli", Codec.brotli()), BROTLI_TEXT),
                arguments(named("zstd", Codec.zstd()), ZSTD_TEXT));
    }

    static Stream<Named<Codec>> encoders() {
        return Stream.of(named("none", Codec.none()), named("gzip", Codec.gzip()), named("zstd", Codec.zstd()));
    }

    // The compressed bytes sit between unrelated bytes, as in a buffer holding several parts of an archive
    @ParameterizedTest(name = "{0}")
    @MethodSource("compressedText")
    void decompressesEachCodec(Codec codec, byte[] compressed) throws IOException {
        ByteBuffer buffer = between(new byte[] {1, 2, 3}, compressed, new byte[] {4, 5, 6});
        assertThat(decompressAll(codec, buffer)).isEqualTo(TEXT);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressedText")
    void decodesFromDirectBuffer(Codec codec, byte[] compressed) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(compressed.length);
        buffer.put(compressed).flip();
        assertThat(decompressAll(codec, buffer)).isEqualTo(TEXT);
        assertThat(buffer.position()).isEqualTo(buffer.limit());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("encoders")
    void roundTrips(Codec codec) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (OutputStream encoder = codec.compress(compressed)) {
            encoder.write(TEXT.getBytes(StandardCharsets.UTF_8));
        }
        ByteBuffer buffer = ByteBuffer.wrap(compressed.toByteArray());
        assertThat(decompressAll(codec, buffer)).isEqualTo(TEXT);
    }

    @Test
    void brotliDoesNotEncode() {
        OutputStream target = new ByteArrayOutputStream();
        Codec brotli = Codec.brotli();
        assertThatThrownBy(() -> brotli.compress(target)).isInstanceOf(UnsupportedOperationException.class);
    }

    private static String decompressAll(Codec codec, ByteBuffer buffer) throws IOException {
        try (InputStream decompressed = codec.decompress(buffer)) {
            return new String(decompressed.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Returns a buffer whose readable bytes are {@code content}, with {@code before} and {@code after} outside them.
     */
    private static ByteBuffer between(byte[] before, byte[] content, byte[] after) {
        ByteBuffer buffer = ByteBuffer.allocate(before.length + content.length + after.length);
        buffer.put(before);
        buffer.put(content);
        buffer.put(after);
        buffer.position(before.length);
        buffer.limit(before.length + content.length);
        return buffer;
    }
}
