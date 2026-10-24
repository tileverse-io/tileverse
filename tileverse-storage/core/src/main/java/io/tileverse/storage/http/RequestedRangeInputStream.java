/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import io.tileverse.storage.StorageException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

/**
 * The body of an answer to a ranged GET without a usable {@code Content-Range}, standing for at most the requested
 * bytes as in {@link HttpRangeReader}: reads stop after the last requested byte, and a body going on past it fails with
 * an IOException caused by a {@link StorageException}. A shorter body ends early: the object ends before the end of the
 * range.
 */
final class RequestedRangeInputStream extends CappedBodyInputStream {

    private final URI uri;

    /**
     * @param body the body of the answer
     * @param requestedLength the length of the requested range
     * @param uri the URI of the GET, quoted in failures
     */
    RequestedRangeInputStream(InputStream body, long requestedLength, URI uri) {
        super(body, requestedLength);
        this.uri = Objects.requireNonNull(uri);
    }

    @Override
    protected void checkEndBeforeCount(long received) {
        // the object ends before the end of the range
    }

    @Override
    protected IOException longerBodyFailure() {
        String message = "Server returned more data than requested (" + byteCount() + " bytes) for " + uri;
        return new IOException(message, new StorageException(message));
    }
}
