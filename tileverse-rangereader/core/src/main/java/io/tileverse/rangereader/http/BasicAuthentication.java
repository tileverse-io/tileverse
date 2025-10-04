/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.rangereader.http;

import java.net.http.HttpClient;
import java.net.http.HttpRequest.Builder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * HTTP Basic Authentication implementation for HttpRangeReader.
 * <p>
 * This authenticator adds the standard HTTP Basic Authentication header
 * to requests, which encodes username and password in Base64.
 */
public class BasicAuthentication implements HttpAuthentication {

    private final String username;

    @SuppressWarnings("unused")
    private final String password;

    private final String encodedCredentials;

    /**
     * Creates a new Basic Authentication instance.
     *
     * @param username The username
     * @param password The password
     */
    public BasicAuthentication(String username, String password) {
        this.username = Objects.requireNonNull(username, "Username cannot be null");
        this.password = Objects.requireNonNull(password, "Password cannot be null");

        // Pre-compute the encoded credentials
        String credentials = username + ":" + password;
        this.encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        return requestBuilder.header("Authorization", "Basic " + encodedCredentials);
    }

    /**
     * Gets the username.
     *
     * @return The username
     */
    public String getUsername() {
        return username;
    }
}
