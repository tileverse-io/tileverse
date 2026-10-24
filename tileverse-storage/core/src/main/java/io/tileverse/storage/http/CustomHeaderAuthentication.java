/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
