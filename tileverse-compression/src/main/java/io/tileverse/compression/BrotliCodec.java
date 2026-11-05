/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import io.tileverse.io.ByteBufferInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Brotli through Brotli4j, with {@code org.brotli:dec} as the pure-Java fallback. Decode only. */
// One stateless instance, returned by the Codec factories
@SuppressWarnings("java:S6548")
final class BrotliCodec implements Codec {

    static final BrotliCodec INSTANCE = new BrotliCodec();

    private BrotliCodec() {}

    @Override
    public InputStream decompress(ByteBuffer compressed) throws IOException {
        InputStream source = ByteBufferInputStream.over(compressed);
        InputStream decoded = BrotliDecoder.nativeOrPureJava().decode(source);
        return DecodedStreams.buffered(decoded);
    }

    @Override
    public OutputStream compress(OutputStream target) {
        throw new UnsupportedOperationException("Brotli compression is not supported");
    }

    @Override
    public String toString() {
        return "brotli";
    }
}
