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
