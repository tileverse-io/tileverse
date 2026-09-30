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
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The version of an object named by the first answer to a range GET: its ETag, its Last-Modified date, and its size
 * from the {@code Content-Range}. It decides how {@link HttpRangeReader} and {@link HttpStorage} read the answers to
 * the GETs completing a range answered in part:
 *
 * <ul>
 *   <li>A 416 ends the read, unless the first answer tells that the object goes on past the rest: then the object
 *       shrank.
 *   <li>With the object size unknown, a 200 also ends the read, unless its Content-Length tells that the object goes on
 *       past the rest: a server may ignore a range past the end of the object instead of answering 416.
 *   <li>An answer holding bytes of the rest must name the same version: the same ETag when both answers have one, the
 *       same Last-Modified date otherwise, and the same object size when both answers tell it. Equal weak ETags pass,
 *       as a strong comparison would fail every read from a server sending only weak ones.
 * </ul>
 *
 * A read finding another version of the object fails with a {@link TransientStorageException}.
 */
record AnsweredVersion(Optional<String> etag, Optional<String> lastModified, OptionalLong objectSize) {

    /** Returns the version named by the headers of {@code response}. */
    static AnsweredVersion of(HttpResponse<?> response) {
        HttpHeaders headers = response.headers();
        Optional<String> etag = headers.firstValue(HttpHeaderNames.ETAG);
        Optional<String> lastModified = headers.firstValue(HttpHeaderNames.LAST_MODIFIED);
        String contentRange = headers.firstValue(HttpHeaderNames.CONTENT_RANGE).orElse(null);
        OptionalLong objectSize = ContentRange.totalOf(contentRange);
        return new AnsweredVersion(etag, lastModified, objectSize);
    }

    /**
     * Returns whether the answer to the GET of the rest from {@code offset} ends the read, closing its body when it
     * does.
     *
     * @throws TransientStorageException for a 416 while the first answer tells that the object goes on past
     *     {@code offset}
     */
    boolean endsTheRead(HttpResponse<InputStream> rest, long offset, URI uri) {
        if (rest.statusCode() == 416) {
            ResponseBodies.closeQuietly(rest);
            requireObjectEndBefore(offset, uri);
            return true;
        }
        if (ignoresARangePastTheEnd(rest, offset)) {
            ResponseBodies.closeQuietly(rest);
            return true;
        }
        return false;
    }

    /** Fails a read whose first answer tells that the object goes on past {@code offset}. */
    private void requireObjectEndBefore(long offset, URI uri) {
        if (objectSize.isPresent() && offset < objectSize.getAsLong()) {
            throw objectChanged(uri);
        }
    }

    /**
     * Returns whether a 200 answers the GET of the rest of an object of unknown size with no Content-Length past
     * {@code offset}.
     */
    private boolean ignoresARangePastTheEnd(HttpResponse<?> rest, long offset) {
        return objectSize.isEmpty() && rest.statusCode() == 200 && !declaresALengthPast(rest, offset);
    }

    private static boolean declaresALengthPast(HttpResponse<?> answer, long offset) {
        OptionalLong length = answer.headers().firstValueAsLong(HttpHeaderNames.CONTENT_LENGTH);
        return length.isPresent() && length.getAsLong() > offset;
    }

    /**
     * Fails an answer holding bytes of the rest from another version of the object, closing its body.
     *
     * @throws TransientStorageException when {@code rest} names another version than the first answer
     */
    void requireSameVersion(HttpResponse<InputStream> rest, URI uri) {
        AnsweredVersion restVersion = of(rest);
        if (differsFrom(restVersion)) {
            ResponseBodies.closeQuietly(rest);
            throw objectChanged(uri);
        }
    }

    private boolean differsFrom(AnsweredVersion later) {
        return otherValidator(later) || otherObjectSize(later);
    }

    private boolean otherValidator(AnsweredVersion later) {
        if (etag.isPresent() && later.etag.isPresent()) {
            return !etag.equals(later.etag);
        }
        if (lastModified.isPresent() && later.lastModified.isPresent()) {
            return !lastModified.equals(later.lastModified);
        }
        return false;
    }

    private boolean otherObjectSize(AnsweredVersion later) {
        if (objectSize.isEmpty() || later.objectSize.isEmpty()) {
            return false;
        }
        return objectSize.getAsLong() != later.objectSize.getAsLong();
    }

    private static TransientStorageException objectChanged(URI uri) {
        return new TransientStorageException("Object changed during the read of " + uri);
    }
}
