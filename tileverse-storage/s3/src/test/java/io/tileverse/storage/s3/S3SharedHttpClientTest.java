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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

class S3SharedHttpClientTest {

    private static final S3HttpClientSettings SETTINGS =
            new S3HttpClientSettings(16, Duration.ofSeconds(2), Duration.ofSeconds(30));

    private final List<SdkAsyncHttpClient> built = new ArrayList<>();
    private final AtomicInteger settingsResolved = new AtomicInteger();
    private final S3SharedHttpClient shared = new S3SharedHttpClient(this::resolveSettings, settings -> buildClient());

    private S3HttpClientSettings resolveSettings() {
        settingsResolved.incrementAndGet();
        return SETTINGS;
    }

    private synchronized SdkAsyncHttpClient buildClient() {
        SdkAsyncHttpClient client = mock(SdkAsyncHttpClient.class);
        built.add(client);
        return client;
    }

    @Test
    void leasesShareOneClient() {
        try (S3SharedHttpClient.Lease first = shared.acquire();
                S3SharedHttpClient.Lease second = shared.acquire()) {
            assertThat(first.client()).isSameAs(second.client());
            assertThat(built).hasSize(1);
        }
    }

    @Test
    void leasesShareTheSettingsOfTheirClientResolvedOnce() {
        try (S3SharedHttpClient.Lease first = shared.acquire();
                S3SharedHttpClient.Lease second = shared.acquire()) {
            assertThat(first.settings()).isSameAs(SETTINGS);
            assertThat(second.settings()).isSameAs(SETTINGS);
            assertThat(settingsResolved).hasValue(1);
        }
    }

    @Test
    void theClientClosesWithTheLastLease() {
        S3SharedHttpClient.Lease first = shared.acquire();
        S3SharedHttpClient.Lease second = shared.acquire();

        first.close();
        verify(built.get(0), never()).close();

        second.close();
        verify(built.get(0)).close();
    }

    @Test
    void closingALeaseAgainReleasesNothing() {
        S3SharedHttpClient.Lease first = shared.acquire();
        S3SharedHttpClient.Lease second = shared.acquire();

        first.close();
        first.close();

        verify(built.get(0), never()).close();
        second.close();
        verify(built.get(0), times(1)).close();
    }

    @Test
    void aLeaseAfterTheLastReleaseBuildsANewClient() {
        shared.acquire().close();

        try (S3SharedHttpClient.Lease later = shared.acquire()) {
            assertThat(built).hasSize(2);
            assertThat(later.client()).isSameAs(built.get(1));
            verify(built.get(1), never()).close();
        }
    }

    @Test
    void aClientFailingToBuildLeavesTheHolderReusable() {
        IllegalStateException failure = new IllegalStateException("client failed to build");
        AtomicInteger builds = new AtomicInteger();
        S3SharedHttpClient failingFirst = new S3SharedHttpClient(() -> SETTINGS, settings -> {
            if (builds.incrementAndGet() == 1) {
                throw failure;
            }
            return buildClient();
        });

        assertThatThrownBy(failingFirst::acquire).isSameAs(failure);

        S3SharedHttpClient.Lease later = failingFirst.acquire();
        assertThat(later.client()).isSameAs(built.get(0));
        later.close();
        verify(built.get(0)).close();
    }

    @Test
    void concurrentHoldersNeverReceiveAClosedClient() throws InterruptedException {
        AtomicInteger closedWhileLeased = new AtomicInteger();
        S3SharedHttpClient tracked = new S3SharedHttpClient(() -> SETTINGS, settings -> new ClosedFlagClient());
        int threads = 8;
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            workers.submit(() -> {
                for (int i = 0; i < 2_000; i++) {
                    try (S3SharedHttpClient.Lease lease = tracked.acquire()) {
                        ClosedFlagClient client = (ClosedFlagClient) lease.client();
                        if (client.closed) {
                            closedWhileLeased.incrementAndGet();
                        }
                    }
                }
                done.countDown();
            });
        }

        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        workers.shutdown();
        assertThat(closedWhileLeased).hasValue(0);
    }

    @Test
    void theProcessWideHolderBuildsACrtHttpClient() {
        try (S3SharedHttpClient.Lease lease = S3SharedHttpClient.INSTANCE.acquire()) {
            assertThat(lease.client()).isInstanceOf(AwsCrtAsyncHttpClient.class);
        }
    }

    /** Remembers being closed; never sends a request. */
    private static final class ClosedFlagClient implements SdkAsyncHttpClient {

        private volatile boolean closed;

        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
