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
 * HTTP Bearer Token Authentication implementation for {@link HttpRangeReader}.
 *
 * <p>Adds {@code Authorization: Bearer <token>} to every request. Used for OAuth 2.0 and JWT-based schemes.
 */
public record BearerTokenAuthentication(String token) implements HttpAuthentication {

    public BearerTokenAuthentication {
        Objects.requireNonNull(token, "Token cannot be null");
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        return requestBuilder.header("Authorization", "Bearer " + token);
    }

    @Override
    public HttpAuthFingerprint fingerprint() {
        return new HttpAuthFingerprint("bearer", HttpAuthFingerprint.sha256Hex("bearer:" + token));
    }
}
