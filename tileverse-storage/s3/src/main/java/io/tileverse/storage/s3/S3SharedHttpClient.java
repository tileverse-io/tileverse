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
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * The HTTP client shared by the async S3 clients of the process. The first lease builds it from settings resolved at
 * that moment, releasing the last lease closes it, and a later lease builds a new one.
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

    static final S3SharedHttpClient INSTANCE = new S3SharedHttpClient(S3HttpClientSettings::ofProcess);

    private final Supplier<S3HttpClientSettings> settingsSource;
    private final Function<S3HttpClientSettings, SdkAsyncHttpClient> factory;

    private @Nullable SdkAsyncHttpClient client;
    private @Nullable S3HttpClientSettings clientSettings;
    private int leases;

    S3SharedHttpClient(Supplier<S3HttpClientSettings> settingsSource) {
        this(settingsSource, S3HttpClientSettings::newAsyncHttpClient);
    }

    S3SharedHttpClient(
            Supplier<S3HttpClientSettings> settingsSource, Function<S3HttpClientSettings, SdkAsyncHttpClient> factory) {
        this.settingsSource = Objects.requireNonNull(settingsSource, "settingsSource");
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    synchronized Lease acquire() {
        if (client == null) {
            S3HttpClientSettings settings = settingsSource.get();
            client = factory.apply(settings);
            clientSettings = settings;
        }
        S3HttpClientSettings leasedSettings = Objects.requireNonNull(clientSettings);
        leases++;
        return new Lease(client, leasedSettings);
    }

    @SuppressWarnings("java:S3398") // runs under the holder's monitor, shared with acquire()
    private synchronized void release() {
        leases--;
        if (leases > 0) {
            return;
        }
        SdkAsyncHttpClient idle = Objects.requireNonNull(client);
        client = null;
        clientSettings = null;
        idle.close();
    }

    /** One holder's share of the client. Closing a lease again releases nothing. */
    final class Lease implements SdkAutoCloseable {

        private final SdkAsyncHttpClient leased;
        private final S3HttpClientSettings settings;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(SdkAsyncHttpClient leased, S3HttpClientSettings settings) {
            this.leased = leased;
            this.settings = settings;
        }

        SdkAsyncHttpClient client() {
            return leased;
        }

        /** The settings used to build the client. */
        S3HttpClientSettings settings() {
            return settings;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release();
            }
        }
    }
}
