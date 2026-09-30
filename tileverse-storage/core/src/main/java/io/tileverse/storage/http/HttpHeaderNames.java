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

/** Names of the response headers read by the HTTP storage and its range reader. */
final class HttpHeaderNames {

    static final String CONTENT_LENGTH = "Content-Length";

    static final String CONTENT_RANGE = "Content-Range";

    static final String CONTENT_TYPE = "Content-Type";

    static final String ETAG = "ETag";

    static final String LAST_MODIFIED = "Last-Modified";

    private HttpHeaderNames() {}
}
