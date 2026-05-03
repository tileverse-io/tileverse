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
