/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import java.net.http.HttpClient;
import java.util.Objects;

/**
 * {@link HttpClientHandle} that wraps a caller-supplied {@link HttpClient}. {@link #close()} is a no-op so the caller
 * retains ownership of the client (used by {@link HttpStorageProvider#open(java.net.URI, HttpClient)}).
 */
record BorrowedHttpHandle(HttpClient client) implements HttpClientHandle {

    BorrowedHttpHandle {
        Objects.requireNonNull(client, "client");
    }

    @Override
    public void close() {
        // borrowed; caller owns the HttpClient
    }
}
