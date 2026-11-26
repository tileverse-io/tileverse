/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

/**
 * Signals that a {@link ByteBufferOutputStream} could not take the bytes offered to it.
 *
 * @deprecated moved to {@link io.tileverse.io.ByteBufferSinkException} in the tileverse-io module, and removed in
 *     2.3.0
 */
@Deprecated(since = "2.2", forRemoval = true)
// The deprecated 2.1 name, kept until 2.3.0, shadows the class it extends
@SuppressWarnings({"java:S1133", "java:S2176"})
public final class ByteBufferSinkException extends io.tileverse.io.ByteBufferSinkException {

    private static final long serialVersionUID = 1L;

    /** Creates a sink failure with the given message. */
    public ByteBufferSinkException(String message) {
        super(message);
    }

    /** Creates a sink failure caused by the target buffer's own exception. */
    public ByteBufferSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
