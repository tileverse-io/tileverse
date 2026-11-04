/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A compression codec: none, gzip, Brotli or Zstandard. Formats map the compression codes of their headers to a codec
 * and leave the byte work to it.
 *
 * <p>gzip and Brotli decode with native libraries, libdeflate and Brotli4j, loaded on first use. When a native library
 * cannot load on this platform, decoding falls back to a slower pure-Java decoder, and one warning is logged for the
 * life of the JVM.
 *
 * <p>Codecs are stateless and safe to share across threads.
 */
public sealed interface Codec permits NoneCodec, GzipCodec, BrotliCodec, ZstdCodec {

    /** Returns the codec of uncompressed data. */
    static Codec none() {
        return NoneCodec.INSTANCE;
    }

    /** Returns the gzip codec. A blob of several gzip members decodes to their concatenation. */
    static Codec gzip() {
        return GzipCodec.INSTANCE;
    }

    /** Returns the Brotli codec. It decodes only: no pure-Java Brotli encoder exists for the fallback. */
    static Codec brotli() {
        return BrotliCodec.INSTANCE;
    }

    /** Returns the Zstandard codec. */
    static Codec zstd() {
        return ZstdCodec.INSTANCE;
    }

    /**
     * Returns a stream over the decompressed bytes of {@code compressed}, read from its position to its limit. The
     * position advances as the stream is read, possibly to the limit at once. The caller must not change the buffer
     * before the stream is closed, and must close the stream: it may hold native decoder state or a pooled buffer.
     * Corrupt data fails with an {@link IOException}, from this method or while the stream is read.
     *
     * @throws IOException if the compressed data is corrupt
     */
    InputStream decompress(ByteBuffer compressed) throws IOException;

    /**
     * Returns a stream that compresses the bytes written to it into {@code target}. Closing the stream finishes the
     * compressed data and closes {@code target}.
     *
     * @throws UnsupportedOperationException if this codec only decodes
     */
    OutputStream compress(OutputStream target) throws IOException;
}
