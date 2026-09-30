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
package io.tileverse.storage.s3;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * The HTTP client shared by every async S3 client of the process. The first lease builds it, releasing the last lease
 * closes it, and a later lease builds a new one.
 *
 * <p>An async S3 client built on its own CRT S3 engine reserves a native buffer pool of at least 1 GiB, per client. A
 * process holding clients for several endpoints cannot afford one pool each. The CRT HTTP client keeps a connection
 * pool per host and no such buffer pool.
 *
 * <p>The SDK never closes an HTTP client supplied by its caller; the leases of this class decide when it closes.
 */
@NullMarked
@SuppressWarnings("java:S6548") // one HTTP client per process is the point: native memory stays independent of
// the number of endpoints read by the process
final class S3SharedHttpClient {

    static final S3SharedHttpClient INSTANCE = new S3SharedHttpClient(S3SharedHttpClient::newHttpClientOfTheProcess);

    private final Supplier<SdkAsyncHttpClient> factory;

    private @Nullable SdkAsyncHttpClient client;
    private int leases;

    S3SharedHttpClient(Supplier<SdkAsyncHttpClient> factory) {
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    private static SdkAsyncHttpClient newHttpClientOfTheProcess() {
        S3HttpClientSettings settings = S3HttpClientSettings.ofProcess();
        return settings.newAsyncHttpClient();
    }

    synchronized Lease acquire() {
        if (client == null) {
            client = factory.get();
        }
        leases++;
        return new Lease(client);
    }

    @SuppressWarnings("java:S3398") // runs under the holder's monitor, shared with acquire()
    private synchronized void release() {
        leases--;
        if (leases > 0) {
            return;
        }
        SdkAsyncHttpClient idle = Objects.requireNonNull(client);
        client = null;
        idle.close();
    }

    /** One holder's share of the client. Closing a lease again releases nothing. */
    final class Lease implements SdkAutoCloseable {

        private final SdkAsyncHttpClient leased;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(SdkAsyncHttpClient leased) {
            this.leased = leased;
        }

        SdkAsyncHttpClient client() {
            return leased;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release();
            }
        }
    }
}
