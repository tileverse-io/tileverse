/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.rastertile.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import javax.imageio.stream.ImageInputStreamImpl;
import javax.imageio.stream.MemoryCacheImageInputStream;

/**
 * An image stream over the remaining bytes of a {@link ByteBuffer}. The stream reads the buffer in place, where
 * {@link MemoryCacheImageInputStream} would copy the bytes into blocks of its own. The position of the buffer does not
 * change.
 */
final class ByteBufferImageInputStream extends ImageInputStreamImpl {

    private final ByteBuffer buffer;
    private final int start;
    private final int length;

    ByteBufferImageInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        this.start = buffer.position();
        this.length = buffer.remaining();
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        bitOffset = 0;
        if (streamPos >= length) {
            return -1;
        }
        int value = buffer.get(start + (int) streamPos) & 0xff;
        streamPos++;
        return value;
    }

    @Override
    public int read(byte[] target, int offset, int count) throws IOException {
        checkClosed();
        Objects.checkFromIndexSize(offset, count, target.length);
        bitOffset = 0;
        if (count == 0) {
            return 0;
        }
        if (streamPos >= length) {
            return -1;
        }
        int read = (int) Math.min(count, length - streamPos);
        buffer.get(start + (int) streamPos, target, offset, read);
        streamPos += read;
        return read;
    }

    @Override
    public long length() {
        return length;
    }

    /**
     * Does nothing. The finalizer of {@link ImageInputStreamImpl} closes a stream left open, and this stream holds
     * nothing to close. The JVM registers each instance of a class with a finalizer, and collects it only after the
     * finalizer has run. An empty finalizer opts out of both, as in {@link MemoryCacheImageInputStream}.
     */
    @Override
    @SuppressWarnings({"deprecation", "removal", "java:S1113"})
    protected void finalize() {
        // empty on purpose, see above
    }
}
