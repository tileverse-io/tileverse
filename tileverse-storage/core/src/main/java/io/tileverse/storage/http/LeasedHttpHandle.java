/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import java.net.http.HttpClient;

/**
 * {@link HttpClientHandle} that wraps an {@link HttpClientCache.Lease}. {@link #close()} releases the lease,
 * decrementing the cache refcount; the cached {@link HttpClient} shuts down when the last lease is released.
 */
final class LeasedHttpHandle implements HttpClientHandle {

    private final HttpClientCache.Lease lease;

    LeasedHttpHandle(HttpClientCache.Lease lease) {
        this.lease = lease;
    }

    @Override
    public HttpClient client() {
        return lease.client();
    }

    @Override
    public void close() {
        lease.close();
    }
}
