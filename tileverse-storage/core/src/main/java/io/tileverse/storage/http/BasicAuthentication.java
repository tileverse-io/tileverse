/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import java.net.http.HttpClient;
import java.net.http.HttpRequest.Builder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * HTTP Basic Authentication implementation for {@link HttpRangeReader}.
 *
 * <p>Adds the standard {@code Authorization: Basic <base64(user:password)>} header to every request.
 */
public record BasicAuthentication(String username, String password) implements HttpAuthentication {

    public BasicAuthentication {
        Objects.requireNonNull(username, "Username cannot be null");
        Objects.requireNonNull(password, "Password cannot be null");
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        String credentials = username + ":" + password;
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return requestBuilder.header("Authorization", "Basic " + encoded);
    }

    @Override
    public HttpAuthFingerprint fingerprint() {
        String canonical = "basic:" + username + ":" + password;
        return new HttpAuthFingerprint("basic", HttpAuthFingerprint.sha256Hex(canonical));
    }
}
