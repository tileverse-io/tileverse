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

import io.tileverse.storage.ContentRange;
import io.tileverse.storage.TransientStorageException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

/**
 * The body of an answer to a ranged GET, held to the bytes named by its {@code Content-Range} as in
 * {@link HttpRangeReader}: reads stop after the last named byte, and a body ending before it or going on past it fails
 * with an IOException caused by a {@link TransientStorageException}. The JDK client reports no error for a body cut
 * short when the close of the connection delimits it, or on HTTP/2.
 */
final class AnsweredRangeInputStream extends CappedBodyInputStream {

    private final ContentRange.Bytes answered;
    private final URI uri;

    /**
     * @param body the body of the answer
     * @param answered the bytes named by the {@code Content-Range} of the answer
     * @param uri the URI of the GET, quoted in failures
     */
    AnsweredRangeInputStream(InputStream body, ContentRange.Bytes answered, URI uri) {
        super(body, answered.length());
        this.answered = answered;
        this.uri = Objects.requireNonNull(uri);
    }

    @Override
    protected void checkEndBeforeCount(long received) throws IOException {
        throw disagreement(received + " bytes");
    }

    @Override
    protected IOException longerBodyFailure() {
        return disagreement("more than " + byteCount() + " bytes");
    }

    private IOException disagreement(String received) {
        String message = "Response body and Content-Range disagree (" + received + ", Content-Range " + contentRange()
                + ") for " + uri;
        return new IOException(message, new TransientStorageException(message));
    }

    /** Returns the answered bytes as written in a {@code Content-Range} header. */
    private String contentRange() {
        String total = "*";
        if (answered.total().isPresent()) {
            total = Long.toString(answered.total().getAsLong());
        }
        return "bytes " + answered.firstPos() + "-" + answered.lastPos() + "/" + total;
    }
}
