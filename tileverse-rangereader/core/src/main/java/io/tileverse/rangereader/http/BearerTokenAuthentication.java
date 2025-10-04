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
import java.util.Objects;

/**
 * HTTP Bearer Token Authentication implementation for HttpRangeReader.
 * <p>
 * This authenticator adds the standard Bearer token Authorization header
 * to requests, which is commonly used for OAuth and JWT tokens.
 */
public class BearerTokenAuthentication implements HttpAuthentication {

    private final String token;

    /**
     * Creates a new Bearer Token Authentication instance.
     *
     * @param token The bearer token
     */
    public BearerTokenAuthentication(String token) {
        this.token = Objects.requireNonNull(token, "Token cannot be null");
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        return requestBuilder.header("Authorization", "Bearer " + token);
    }
}
