/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.tileverse.compression.Codec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class CompressionTest {

    private static final String TEXT = "tile data ".repeat(20);

    // TEXT compressed by brotli 1.2.0: Brotli4j encodes only with its native library
    private static final byte[] BROTLI_TEXT = {
        31, -57, 0, -8, -115, -108, -87, -29, 91, -125, -112, -79, -47, -55, -80, -78, -76, 16, 48, 20, 0, -1, 66, 3
    };

    static Stream<Arguments> compressedText() throws IOException {
        byte[] text = TEXT.getBytes(StandardCharsets.UTF_8);
        return Stream.of(
                arguments(Compression.NONE, text),
                arguments(Compression.GZIP, compress(Codec.gzip(), text)),
                arguments(Compression.BROTLI, BROTLI_TEXT),
                arguments(Compression.ZSTD, compress(Codec.zstd(), text)));
    }

    @ParameterizedTest
    @EnumSource(Compression.class)
    void readsItsOwnCode(Compression compression) {
        assertThat(Compression.fromCode(compression.code())).isSameAs(compression);
    }

    // The codes of the PMTiles v3 specification
    @Test
    void codesFollowTheSpecification() {
        assertThat(Compression.fromCode((byte) 0)).isSameAs(Compression.UNKNOWN);
        assertThat(Compression.fromCode((byte) 1)).isSameAs(Compression.NONE);
        assertThat(Compression.fromCode((byte) 2)).isSameAs(Compression.GZIP);
        assertThat(Compression.fromCode((byte) 3)).isSameAs(Compression.BROTLI);
        assertThat(Compression.fromCode((byte) 4)).isSameAs(Compression.ZSTD);
    }

    @Test
    void rejectsUndefinedCodes() {
        assertThatThrownBy(() -> Compression.fromCode((byte) 5)).isInstanceOf(UnsupportedCompressionException.class);
    }

    @Test
    void unknownDoesNotDecode() {
        ByteBuffer buffer = ByteBuffer.allocate(0);
        assertThatThrownBy(() -> Compression.UNKNOWN.decompress(buffer))
                .isInstanceOf(UnsupportedCompressionException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressedText")
    void decodesWithItsCodec(Compression compression, byte[] compressed) throws IOException {
        try (InputStream decoded = compression.decompress(ByteBuffer.wrap(compressed))) {
            String text = new String(decoded.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(text).isEqualTo(TEXT);
        }
    }

    private static byte[] compress(Codec codec, byte[] data) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (OutputStream encoder = codec.compress(compressed)) {
            encoder.write(data);
        }
        return compressed.toByteArray();
    }
}
