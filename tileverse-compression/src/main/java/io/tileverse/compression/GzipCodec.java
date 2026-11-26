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
import java.util.zip.GZIPOutputStream;

/** gzip through libdeflate, with the JDK decoder as the fallback. Compression uses the JDK encoder. */
// One stateless instance, returned by the Codec factories
@SuppressWarnings("java:S6548")
final class GzipCodec implements Codec {

    static final GzipCodec INSTANCE = new GzipCodec();

    private GzipCodec() {}

    @Override
    public InputStream decompress(ByteBuffer compressed) throws IOException {
        return GzipDecoder.nativeOrPureJava().decode(compressed);
    }

    @Override
    public OutputStream compress(OutputStream target) throws IOException {
        return new GZIPOutputStream(target);
    }

    @Override
    public String toString() {
        return "gzip";
    }
}
