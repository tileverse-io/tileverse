/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import io.tileverse.compression.Codec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.jspecify.annotations.Nullable;

/**
 * The compressions defined by the PMTiles specification, with their codes in the header. The header gives one for the
 * tiles and one for the directories and the metadata.
 *
 * @see PMTilesHeader#tileCompression()
 * @see PMTilesHeader#internalCompression()
 */
public enum Compression {
    /** The writer did not record the compression: the bytes cannot be decoded. */
    UNKNOWN(PMTilesHeader.COMPRESSION_UNKNOWN, null),
    NONE(PMTilesHeader.COMPRESSION_NONE, Codec.none()),
    GZIP(PMTilesHeader.COMPRESSION_GZIP, Codec.gzip()),
    BROTLI(PMTilesHeader.COMPRESSION_BROTLI, Codec.brotli()),
    ZSTD(PMTilesHeader.COMPRESSION_ZSTD, Codec.zstd());

    // Read once: values() copies its array on each call, and the readers look a code up for each tile
    private static final Compression[] VALUES = values();

    private final byte code;

    private final @Nullable Codec codec;

    Compression(byte code, @Nullable Codec codec) {
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
     * @throws UnsupportedCompressionException if the specification defines no compression with this code
     */
    public static Compression fromCode(byte code) {
        for (Compression compression : VALUES) {
            if (compression.code == code) {
                return compression;
            }
        }
        throw new UnsupportedCompressionException("Compression type not supported: " + code);
    }

    /**
     * Returns a stream over the decompressed bytes of {@code compressed}, read from its position to its limit. The
     * caller must not change the buffer before the stream is closed, and must close the stream.
     *
     * @throws UnsupportedCompressionException if this compression is {@link #UNKNOWN}
     * @throws IOException if the compressed data is corrupt
     * @see Codec#decompress(ByteBuffer)
     */
    public InputStream decompress(ByteBuffer compressed) throws IOException {
        if (codec == null) {
            throw new UnsupportedCompressionException("Compression type not supported: " + code);
        }
        return codec.decompress(compressed);
    }
}
