/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
