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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Custom HTTP Header Authentication implementation for HttpRangeReader.
 * <p>
 * This authenticator allows arbitrary headers to be added to requests,
 * which is useful for custom authentication schemes or when multiple
 * headers are required.
 */
public class CustomHeaderAuthentication implements HttpAuthentication {

    private final Map<String, String> headers;

    /**
     * Creates a new Custom Header Authentication instance.
     *
     * @param headers A map of header names to values
     */
    public CustomHeaderAuthentication(Map<String, String> headers) {
        this.headers = Collections.unmodifiableMap(Objects.requireNonNull(headers, "Headers map cannot be null"));

        if (headers.isEmpty()) {
            throw new IllegalArgumentException("Headers map cannot be empty");
        }
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        // Add all headers to the request builder
        headers.forEach(requestBuilder::header);
        return requestBuilder;
    }
}
