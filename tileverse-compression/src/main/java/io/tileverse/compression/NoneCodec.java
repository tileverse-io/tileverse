/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import io.tileverse.io.ByteBufferInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Uncompressed data: the bytes pass through unchanged. */
// One stateless instance, returned by the Codec factories
@SuppressWarnings("java:S6548")
final class NoneCodec implements Codec {

    static final NoneCodec INSTANCE = new NoneCodec();

    private NoneCodec() {}

    @Override
    public InputStream decompress(ByteBuffer compressed) {
        return ByteBufferInputStream.over(compressed);
    }

    @Override
    public OutputStream compress(OutputStream target) {
        return target;
    }

    @Override
    public String toString() {
        return "none";
    }
}
