/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.adapters;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * An {@link OutputStream} that writes into a {@link ByteBuffer} at its current position, accepting at most a fixed
 * number of bytes.
 *
 * <p>Backends hand this stream to SDK download calls, or to {@link java.io.InputStream#transferTo}, to land a response
 * body straight in a caller's buffer, heap or direct, without a full-size intermediate array; the transient heap per
 * read is then bounded by the producer's chunk size.
 *
 * <p>Writes advance the buffer's position and never touch its limit. A write that would exceed the accepted byte count
 * throws {@link ByteBufferSinkException} before touching the buffer, and a write refused by the buffer itself (a
 * read-only, closed, or thread-confined buffer) throws the same exception with the buffer's failure as its cause. The
 * checked type lets an SDK pipeline propagate the failure and lets the caller tell it from an I/O failure of the
 * producer. {@link #close()} and {@link #flush()} do nothing: the stream owns neither the buffer nor any resource.
 *
 * <p>Not thread-safe.
 */
public final class ByteBufferOutputStream extends OutputStream {

    private final ByteBuffer target;
    private final int maxBytes;
    private int written;

    /**
     * Creates a stream writing into {@code target} from its current position.
     *
     * @param target the buffer to fill; must have at least {@code maxBytes} remaining
     * @param maxBytes the most bytes the stream accepts before throwing {@link ByteBufferSinkException}
     * @throws IllegalArgumentException if {@code maxBytes} is negative or exceeds the buffer's remaining capacity
     */
    public ByteBufferOutputStream(ByteBuffer target, int maxBytes) {
        this.target = requireNonNull(target, "target cannot be null");
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes cannot be negative: " + maxBytes);
        }
        if (maxBytes > target.remaining()) {
            throw new IllegalArgumentException(
                    "target has " + target.remaining() + " bytes remaining, fewer than maxBytes " + maxBytes);
        }
        this.maxBytes = maxBytes;
    }

    /**
     * Returns how many bytes have been written into the buffer so far.
     *
     * @return the byte count written, at most {@code maxBytes}
     */
    public int bytesWritten() {
        return written;
    }

    @Override
    public void write(int b) throws IOException {
        ensureRoomFor(1);
        try {
            target.put((byte) b);
        } catch (RuntimeException refused) {
            throw refusedWrite(refused);
        }
        written++;
    }

    @Override
    public void write(byte[] source, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, source.length);
        ensureRoomFor(length);
        try {
            target.put(source, offset, length);
        } catch (RuntimeException refused) {
            throw refusedWrite(refused);
        }
        written += length;
    }

    private void ensureRoomFor(int length) throws ByteBufferSinkException {
        if (length > maxBytes - written) {
            throw new ByteBufferSinkException("Server returned more data than requested (" + maxBytes + " bytes)");
        }
    }

    private static ByteBufferSinkException refusedWrite(RuntimeException refused) {
        return new ByteBufferSinkException("Target buffer refused the write: " + refused, refused);
    }
}
