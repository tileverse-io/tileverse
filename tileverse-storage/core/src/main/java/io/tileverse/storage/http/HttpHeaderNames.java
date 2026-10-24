/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

/** Names of the response headers read by the HTTP storage and its range reader. */
final class HttpHeaderNames {

    static final String CONTENT_LENGTH = "Content-Length";

    static final String CONTENT_RANGE = "Content-Range";

    static final String CONTENT_TYPE = "Content-Type";

    static final String ETAG = "ETag";

    static final String LAST_MODIFIED = "Last-Modified";

    private HttpHeaderNames() {}
}
