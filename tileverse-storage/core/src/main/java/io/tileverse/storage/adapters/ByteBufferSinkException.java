/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

import java.io.IOException;

/**
 * Signals that a {@link ByteBufferOutputStream} could not take the bytes offered to it: the producer sent more than the
 * accepted count or wrote after the stream was closed, or the target buffer refused the write. None is an I/O failure
 * of the producer, and retrying the download does not help.
 *
 * <p>Checked, because the sink lives inside SDK download pipelines that propagate an {@link IOException} raised by the
 * stream but may drop anything else, leaving the read to wait out its timeout.
 */
public final class ByteBufferSinkException extends IOException {

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
