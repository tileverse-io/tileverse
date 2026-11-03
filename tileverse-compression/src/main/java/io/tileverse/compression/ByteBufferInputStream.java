/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.jspecify.annotations.Nullable;

/**
 * Reads the bytes of a buffer from its position to its limit, advancing the position. A closed stream fails on each
 * read, as the JDK streams do. For a pooled buffer, this also protects the next borrower: closing the stream returns
 * the buffer to its pool.
 */
final class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    private @Nullable PooledByteBuffer pooled;

    private boolean closed;

    private ByteBufferInputStream(ByteBuffer buffer, @Nullable PooledByteBuffer pooled) {
        this.buffer = buffer;
        this.pooled = pooled;
    }

    /** Returns a stream over {@code buffer}. The caller keeps ownership of the buffer. */
    static ByteBufferInputStream over(ByteBuffer buffer) {
        return new ByteBufferInputStream(buffer, null);
    }

    /** Returns a stream over the buffer of {@code pooled}. Closing the stream returns the buffer to its pool. */
    static ByteBufferInputStream releasing(PooledByteBuffer pooled) {
        return new ByteBufferInputStream(pooled.buffer(), pooled);
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        if (buffer.hasRemaining()) {
            return buffer.get() & 0xff;
        }
        return -1;
    }

    @Override
    public int read(byte[] target, int offset, int length) throws IOException {
        ensureOpen();
        if (length == 0) {
            return 0;
        }
        if (!buffer.hasRemaining()) {
            return -1;
        }
        int count = Math.min(length, buffer.remaining());
        buffer.get(target, offset, count);
        return count;
    }

    // One copy of the remaining bytes, where the default reads and copies them in chunks
    @Override
    public byte[] readAllBytes() throws IOException {
        ensureOpen();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public long transferTo(OutputStream target) throws IOException {
        ensureOpen();
        if (!buffer.hasArray()) {
            return super.transferTo(target);
        }
        int count = buffer.remaining();
        target.write(buffer.array(), buffer.arrayOffset() + buffer.position(), count);
        buffer.position(buffer.limit());
        return count;
    }

    @Override
    public long skip(long count) throws IOException {
        ensureOpen();
        int skipped = (int) Math.min(Math.max(count, 0), buffer.remaining());
        buffer.position(buffer.position() + skipped);
        return skipped;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return buffer.remaining();
    }

    @Override
    public void close() {
        closed = true;
        if (pooled != null) {
            pooled.close();
            pooled = null;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }
}
