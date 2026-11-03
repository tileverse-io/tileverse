/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import io.tileverse.compression.Codec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * The compression of the metadata and of the tile bodies in a Versatiles container, as recorded at byte 15 of the
 * header.
 *
 * <p>The block index and the tile indexes are always compressed with Brotli, regardless of this setting.
 */
public enum Compression {
    NONE((byte) 0x00, Codec.none()),
    GZIP((byte) 0x01, Codec.gzip()),
    BROTLI((byte) 0x02, Codec.brotli()),
    ZSTD((byte) 0x03, Codec.zstd());

    private final byte code;

    private final Codec codec;

    Compression(byte code, Codec codec) {
        this.code = code;
        this.codec = codec;
    }

    /** Returns the code of this compression in the header. */
    public byte code() {
        return code;
    }

    /**
     * Returns the compression recorded with {@code code} in a header.
     *
     * @throws IllegalArgumentException if the specification defines no compression with this code
     */
    public static Compression fromCode(byte code) {
        for (Compression compression : values()) {
            if (compression.code == code) {
                return compression;
            }
        }
        throw new IllegalArgumentException("Unknown compression code: 0x%02X".formatted(code & 0xFF));
    }

    /**
     * Returns a stream over the decompressed bytes of {@code compressed}, read from its position to its limit. The
     * caller must not change the buffer before the stream is closed, and must close the stream.
     *
     * @throws IOException if the compressed data is corrupt
     * @see Codec#decompress(ByteBuffer)
     */
    public InputStream decompress(ByteBuffer compressed) throws IOException {
        return codec.decompress(compressed);
    }
}
