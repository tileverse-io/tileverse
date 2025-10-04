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
import lombok.NonNull;

/**
 * API Key Authentication implementation for HttpRangeReader.
 * <p>
 * This authenticator adds an API key header to requests, which is commonly used
 * for API authentication. It supports different header names and value formats.
 */
public class ApiKeyAuthentication implements HttpAuthentication {

    private final String headerName;
    private final String apiKey;
    private final String valuePrefix;

    /**
     * Creates a new API Key Authentication instance with a custom header name.
     *
     * @param headerName The name of the header to use (e.g., "X-API-Key")
     * @param apiKey The API key value
     */
    public ApiKeyAuthentication(String headerName, String apiKey) {
        this(headerName, apiKey, "");
    }

    /**
     * Creates a new API Key Authentication instance with custom header name and value prefix.
     * <p>
     * This is useful for APIs that require a specific format for the API key value,
     * such as "ApiKey " or "Key " followed by the actual key.
     *
     * @param headerName The name of the header to use (e.g., "X-API-Key")
     * @param apiKey The API key value
     * @param valuePrefix An optional prefix for the API key value (e.g., "ApiKey ")
     */
    public ApiKeyAuthentication(@NonNull String headerName, @NonNull String apiKey, String valuePrefix) {
        this.headerName = headerName;
        this.apiKey = apiKey;
        this.valuePrefix = valuePrefix != null ? valuePrefix : "";
    }

    @Override
    public Builder authenticate(HttpClient httpClient, Builder requestBuilder) {
        return requestBuilder.header(headerName, valuePrefix + apiKey);
    }
}
