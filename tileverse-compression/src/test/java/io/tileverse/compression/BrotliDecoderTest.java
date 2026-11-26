/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Named.named;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.encoder.Encoder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BrotliDecoderTest {

    private static final String TEXT = "tile data ".repeat(2000);

    private static byte[] compressed;

    @BeforeAll
    static void compressText() throws IOException {
        // The encoder needs the native library, loaded by brotli4j only on request
        Brotli4jLoader.ensureAvailability();
        compressed = Encoder.compress(TEXT.getBytes(StandardCharsets.UTF_8));
    }

    static Stream<Named<BrotliDecoder>> decoders() {
        return Stream.of(
                named("native", BrotliDecoder.nativeOrPureJava()), named("pure Java", BrotliDecoder.pureJava()));
    }

    // CI runs on Linux x64, Windows x64 and macOS arm64, all with a native jar declared: a failure means a packaging
    // regression
    @Test
    void nativeLoadsOnThisPlatform() {
        assertThat(BrotliDecoder.nativeOrPureJava().isNative()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void decodesText(BrotliDecoder decoder) throws IOException {
        try (InputStream decoded = decoder.decode(new ByteArrayInputStream(compressed))) {
            String text = new String(decoded.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(text).isEqualTo(TEXT);
        }
    }

    // Brotli has no checksum: a flipped byte in the literals decodes to wrong text without an error. Byte 3 lies in the
    // headers before the literals, where the decoders detect the damage.
    @ParameterizedTest
    @MethodSource("decoders")
    void corruptStreamFails(BrotliDecoder decoder) {
        byte[] corrupt = compressed.clone();
        corrupt[3] = (byte) ~corrupt[3];

        assertThatThrownBy(() -> decodeAll(decoder, corrupt)).isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void truncatedStreamFails(BrotliDecoder decoder) {
        byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);

        assertThatThrownBy(() -> decodeAll(decoder, truncated)).isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @MethodSource("decoders")
    void byteAfterStreamFails(BrotliDecoder decoder) {
        byte[] padded = Arrays.copyOf(compressed, compressed.length + 1);

        assertThatThrownBy(() -> decodeAll(decoder, padded)).isInstanceOf(IOException.class);
    }

    private static byte[] decodeAll(BrotliDecoder decoder, byte[] bytes) throws IOException {
        try (InputStream decoded = decoder.decode(new ByteArrayInputStream(bytes))) {
            return decoded.readAllBytes();
        }
    }
}
