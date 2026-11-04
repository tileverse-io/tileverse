/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import io.tileverse.io.ByteBufferInputStream;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Zstandard through zstd-jni. The streams take their buffers from a pool and have no finalizer: a stream with its own
 * 128 KiB buffer and a finalizer lengthens the garbage collection pauses under load.
 */
// One stateless instance, returned by the Codec factories
@SuppressWarnings("java:S6548")
final class ZstdCodec implements Codec {

    static final ZstdCodec INSTANCE = new ZstdCodec();

    private ZstdCodec() {}

    @Override
    public InputStream decompress(ByteBuffer compressed) throws IOException {
        InputStream source = ByteBufferInputStream.over(compressed);
        InputStream decoded = new ZstdInputStreamNoFinalizer(source, RecyclingBufferPool.INSTANCE);
        return DecodedStreams.buffered(decoded);
    }

    @Override
    public OutputStream compress(OutputStream target) throws IOException {
        return new ZstdOutputStreamNoFinalizer(target, RecyclingBufferPool.INSTANCE);
    }

    @Override
    public String toString() {
        return "zstd";
    }
}
