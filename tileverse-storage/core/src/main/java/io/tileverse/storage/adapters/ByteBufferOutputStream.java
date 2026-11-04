/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An output stream that writes into a {@link ByteBuffer} at its current position, accepting at most a fixed number of
 * bytes.
 *
 * <p>Its write failures are instances of the deprecated {@link ByteBufferSinkException}, for the code that catches
 * that type.
 *
 * @deprecated moved to {@link io.tileverse.io.ByteBufferOutputStream} in the tileverse-io module, and removed in 2.3.0
 */
@Deprecated(since = "2.2", forRemoval = true)
// The deprecated 2.1 name, kept until 2.3.0, shadows the class it extends
@SuppressWarnings({"java:S1133", "java:S2176"})
public final class ByteBufferOutputStream extends io.tileverse.io.ByteBufferOutputStream {

    /**
     * Creates a stream writing into {@code target} from its current position.
     *
     * @param target the buffer to fill; must have at least {@code maxBytes} remaining
     * @param maxBytes the most bytes the stream accepts before throwing {@link ByteBufferSinkException}
     * @throws IllegalArgumentException if {@code maxBytes} is negative or exceeds the buffer's remaining capacity
     */
    public ByteBufferOutputStream(ByteBuffer target, int maxBytes) {
        super(target, maxBytes);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        try {
            super.write(b);
        } catch (io.tileverse.io.ByteBufferSinkException failure) {
            throw asDeprecatedType(failure);
        }
    }

    @Override
    public synchronized void write(byte[] source, int offset, int length) throws IOException {
        try {
            super.write(source, offset, length);
        } catch (io.tileverse.io.ByteBufferSinkException failure) {
            throw asDeprecatedType(failure);
        }
    }

    private static ByteBufferSinkException asDeprecatedType(io.tileverse.io.ByteBufferSinkException failure) {
        ByteBufferSinkException deprecated = new ByteBufferSinkException(failure.getMessage(), failure.getCause());
        deprecated.setStackTrace(failure.getStackTrace());
        return deprecated;
    }
}
