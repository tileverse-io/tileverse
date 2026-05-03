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

import java.net.http.HttpClient;
import java.net.http.HttpRequest;

/**
 * Interface for HTTP authentication strategies used by HttpRangeReader.
 *
 * <p>Implementations of this interface provide authentication for HTTP requests by adding appropriate headers or other
 * authentication mechanisms to the request builder.
 *
 * <p>Implementations also produce an {@link HttpAuthFingerprint} that captures their credential identity. The
 * fingerprint is used to key shared HTTP clients in caches; two authentications with the same fingerprint represent the
 * same principal and may share an underlying {@link HttpClient}.
 */
@FunctionalInterface
public interface HttpAuthentication {

    /**
     * Apply authentication to an HTTP request.
     *
     * @param httpClient the HTTP client the request is being built for
     * @param requestBuilder The HTTP request builder to authenticate
     * @return The same request builder with authentication applied
     */
    HttpRequest.Builder authenticate(HttpClient httpClient, HttpRequest.Builder requestBuilder);

    /**
     * Returns a stable, equality-comparable fingerprint of this authentication's credential identity. Implementations
     * that hold live state (e.g. {@link DigestAuthentication}'s challenge cache) MUST exclude that state from the
     * fingerprint so two instances configured with the same credentials compare equal.
     *
     * <p>The default returns {@link HttpAuthFingerprint#NONE}, which is the correct value for {@link #NONE} and any
     * lambda that does not actually authenticate.
     */
    default HttpAuthFingerprint fingerprint() {
        return HttpAuthFingerprint.NONE;
    }

    /** A no-op authentication implementation that does not modify the request. */
    static final HttpAuthentication NONE = (c, b) -> b;
}
