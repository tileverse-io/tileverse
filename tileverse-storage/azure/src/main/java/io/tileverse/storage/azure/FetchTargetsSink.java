/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static java.util.Objects.requireNonNull;

import io.tileverse.storage.adapters.ByteBufferSinkException;
import io.tileverse.storage.batch.FetchTargets;
import java.nio.ByteBuffer;

/**
 * Writes the chunks of one download into the targets of a joined fetch, in order from the start of the fetch, at most
 * the fetch length. The bytes between two requests are dropped.
 *
 * <p>A chunk running past the fetch, or refused by a target, throws {@link ByteBufferSinkException}. {@link #close()}
 * fences the targets off, as {@link io.tileverse.storage.adapters.ByteBufferOutputStream} does for one buffer: it waits
 * for a write in progress, and later writes throw without touching a target. A caller closes the sink before handing
 * the targets back, because the SDK may keep delivering the body from its own thread after the download call has
 * failed. Writes, {@link #bytesWritten()} and {@link #close()} synchronize on the sink.
 */
final class FetchTargetsSink implements AutoCloseable {

    private final FetchTargets targets;
    private int written;
    private boolean closed;

    FetchTargetsSink(FetchTargets targets) {
        this.targets = requireNonNull(targets, "targets cannot be null");
    }

    /** Returns how many bytes of the fetch have been written so far, the dropped ones included. */
    synchronized int bytesWritten() {
        return written;
    }

    /**
     * Writes the remaining bytes of {@code chunk} after the bytes already written. The chunk's position does not move.
     *
     * @throws ByteBufferSinkException if the sink is closed, the chunk runs past the fetch, or a target refuses it
     */
    synchronized void write(ByteBuffer chunk) throws ByteBufferSinkException {
        if (closed) {
            throw new ByteBufferSinkException("Sink closed, the target buffers no longer accept writes");
        }
        int length = chunk.remaining();
        if (length > targets.length() - written) {
            throw new ByteBufferSinkException(
                    "Server returned more data than requested (" + targets.length() + " bytes)");
        }
        try {
            targets.write(written, chunk, chunk.position(), length);
        } catch (RuntimeException refused) {
            throw new ByteBufferSinkException("Target buffer refused the write: " + refused, refused);
        }
        written += length;
    }

    /** Rejects later writes once a write in progress has finished. Closing again has no effect. */
    @Override
    public synchronized void close() {
        closed = true;
    }
}
