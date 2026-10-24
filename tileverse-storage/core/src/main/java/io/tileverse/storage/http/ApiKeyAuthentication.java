/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import java.net.http.HttpClient;
import java.net.http.HttpRequest.Builder;
import java.util.Objects;

/**
 * API Key Authentication implementation for {@link HttpRangeReader}.
 *
 * <p>Sends a single header with a configurable name and an optional value prefix (e.g. {@code "ApiKey "} or
 * {@code "Token "}) before the key value.
 */
public record ApiKeyAuthentication(String headerName, String apiKey, String valuePrefix) implements HttpAuthentication {

    public ApiKeyAuthentication {
        Objects.requireNonNull(headerName, "headerName cannot be null");
        Objects.requireNonNull(apiKey, "apiKey cannot be null");
        if (valuePrefix == null) {
            valuePrefix = "";
        }
    }

    /**
     * Convenience constructor for the no-prefix case. Equivalent to {@code new ApiKeyAuthentication(headerName, apiKey,
     * "")}.
     */
    public ApiKeyAuthentication(String headerName, String apiKey) {
        this(headerName, apiKey, "");
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        return requestBuilder.header(headerName, valuePrefix + apiKey);
    }

    @Override
    public HttpAuthFingerprint fingerprint() {
        String canonical = "apikey:" + headerName + ":" + valuePrefix + ":" + apiKey;
        return new HttpAuthFingerprint("apikey", HttpAuthFingerprint.sha256Hex(canonical));
    }
}
