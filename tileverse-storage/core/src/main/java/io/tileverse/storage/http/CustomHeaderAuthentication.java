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
import java.net.http.HttpRequest.Builder;
import java.util.Map;
import java.util.Objects;

/**
 * Authentication implementation that adds an arbitrary set of headers to every request.
 *
 * <p>Used for custom authentication schemes or APIs that require multiple authentication headers (e.g. an API key plus
 * a tenant identifier).
 */
public record CustomHeaderAuthentication(Map<String, String> headers) implements HttpAuthentication {

    public CustomHeaderAuthentication {
        Objects.requireNonNull(headers, "Headers map cannot be null");
        if (headers.isEmpty()) {
            throw new IllegalArgumentException("Headers map cannot be empty");
        }
        // Map.copyOf rejects null keys/values, which would otherwise blow up at request-building time.
        headers = Map.copyOf(headers);
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        headers.forEach(requestBuilder::header);
        return requestBuilder;
    }

    @Override
    public HttpAuthFingerprint fingerprint() {
        StringBuilder canonical = new StringBuilder("custom-header:");
        headers.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> appendLengthPrefixed(canonical, entry.getKey(), entry.getValue()));
        return new HttpAuthFingerprint("custom-header", HttpAuthFingerprint.sha256Hex(canonical.toString()));
    }

    private static void appendLengthPrefixed(StringBuilder out, String name, String value) {
        out.append(name.length())
                .append(':')
                .append(name)
                .append(':')
                .append(value.length())
                .append(':')
                .append(value);
    }
}
