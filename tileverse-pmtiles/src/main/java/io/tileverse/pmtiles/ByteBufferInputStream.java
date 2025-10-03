/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.pmtiles;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Provides an InputStream backed by a ByteBuffer, supporting mark/reset functionality.
 * This implementation is thread-safe through synchronization and provides efficient
 * streaming access to ByteBuffer contents without copying data.
 *
 * <p>The stream maintains the ByteBuffer's position and supports all standard
 * InputStream operations including mark/reset. Once closed, the internal ByteBuffer
 * reference is cleared to aid garbage collection.
 *
 * @since 1.0
 */
final class ByteBufferInputStream extends InputStream {

    /** Size of the buffer. */
    private final int size;

    /**
     * Not final so that in close() it will be set to null, which
     * may result in faster cleanup of the buffer.
     */
    private ByteBuffer byteBuffer;

    /**
     * Creates a new ByteBufferInputStream backed by the specified ByteBuffer.
     *
     * @param size the size of the buffer (should match byteBuffer.limit())
     * @param byteBuffer the ByteBuffer to read from
     * @throws IllegalArgumentException if size is negative or byteBuffer is null
     */
    public ByteBufferInputStream(int size, ByteBuffer byteBuffer) {
        if (size < 0) {
            throw new IllegalArgumentException("Size cannot be negative: " + size);
        }
        if (byteBuffer == null) {
            throw new IllegalArgumentException("ByteBuffer cannot be null");
        }
        this.size = size;
        this.byteBuffer = byteBuffer;
    }

    /**
     * Creates a new ByteBufferInputStream backed by the specified ByteBuffer.
     * The size is automatically determined from the buffer's remaining bytes.
     *
     * @param byteBuffer the ByteBuffer to read from
     * @throws IllegalArgumentException if byteBuffer is null
     */
    public ByteBufferInputStream(ByteBuffer byteBuffer) {
        this(byteBuffer != null ? byteBuffer.remaining() : 0, byteBuffer);
    }

    /**
     * After the stream is closed, set the local reference to the byte
     * buffer to null; this guarantees that future attempts to use
     * stream methods will fail.
     */
    @Override
    public synchronized void close() {
        byteBuffer = null;
    }

    /**
     * Is the stream open?
     * @return true if the stream has not been closed.
     */
    public synchronized boolean isOpen() {
        return byteBuffer != null;
    }

    /**
     * Verify that the stream is open.
     * @throws IOException if the stream is closed
     */
    private void verifyOpen() throws IOException {
        if (byteBuffer == null) {
            throw new IOException("Stream is closed");
        }
    }

    /**
     * Check the open state.
     * @throws IllegalStateException if the stream is closed.
     */
    private void checkIsOpen() {
        if (!isOpen()) {
            throw new IllegalStateException("Stream is closed");
        }
    }

    @Override
    public synchronized int read() throws IOException {
        if (available() > 0) {
            return byteBuffer.get() & 0xFF;
        } else {
            return -1;
        }
    }

    @Override
    public synchronized long skip(long offset) throws IOException {
        verifyOpen();
        long newPos = position() + offset;
        if (newPos < 0) {
            throw new EOFException("Cannot seek to negative position: " + newPos);
        }
        if (newPos > size) {
            throw new EOFException("Cannot seek past end of buffer: " + newPos + " > " + size);
        }
        byteBuffer.position((int) newPos);
        return offset;
    }

    @Override
    public synchronized int available() {
        checkIsOpen();
        return byteBuffer.remaining();
    }

    /**
     * Get the current buffer position.
     * @return the buffer position
     */
    public synchronized int position() {
        checkIsOpen();
        return byteBuffer.position();
    }

    /**
     * Check if there is data left.
     * @return true if there is data remaining in the buffer.
     */
    public synchronized boolean hasRemaining() {
        checkIsOpen();
        return byteBuffer.hasRemaining();
    }

    @Override
    public synchronized void mark(int readlimit) {
        checkIsOpen();
        byteBuffer.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        checkIsOpen();
        try {
            byteBuffer.reset();
        } catch (java.nio.InvalidMarkException e) {
            throw new IOException("Invalid mark", e);
        }
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Read data into the specified byte array.
     * @param b destination buffer.
     * @param offset offset within the buffer.
     * @param length length of bytes to read.
     * @return the number of bytes read, or -1 if end of stream
     * @throws IOException if the stream is closed
     * @throws IllegalArgumentException if arguments are invalid.
     * @throws IndexOutOfBoundsException if there isn't space for the amount of data requested.
     */
    @Override
    public synchronized int read(byte[] b, int offset, int length) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (b == null) {
            throw new IllegalArgumentException("Destination buffer cannot be null");
        }
        if (offset < 0 || offset > b.length) {
            throw new IndexOutOfBoundsException("Offset out of bounds: " + offset);
        }
        if (b.length - offset < length) {
            throw new IndexOutOfBoundsException("Not enough space in destination buffer: request length=" + length
                    + ", with offset=" + offset
                    + "; buffer capacity=" + (b.length - offset));
        }
        verifyOpen();
        if (!hasRemaining()) {
            return -1;
        }

        int toRead = Math.min(length, available());
        byteBuffer.get(b, offset, toRead);
        return toRead;
    }

    @Override
    public String toString() {
        synchronized (this) {
            return "ByteBufferInputStream{"
                    + "size=" + size
                    + ", byteBuffer=" + byteBuffer
                    + ((byteBuffer != null) ? ", available=" + byteBuffer.remaining() : "")
                    + "} " + super.toString();
        }
    }
}
