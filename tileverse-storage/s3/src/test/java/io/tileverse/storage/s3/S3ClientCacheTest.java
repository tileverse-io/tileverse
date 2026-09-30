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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.utils.AttributeMap;

class S3ClientCacheTest {

    private static final S3ClientCache.Key EAST = S3ClientCache.key("us-east-1", null, true, null, null, null, false);
    private static final S3ClientCache.Key WEST = S3ClientCache.key("us-west-2", null, true, null, null, null, false);

    private final List<CloseCountingHttpClient> httpClients = new ArrayList<>();
    private final List<CloseCountingSyncHttpClient> syncHttpClients = new ArrayList<>();

    private S3ClientCache cacheCountingHttpClients() {
        return cacheCountingHttpClients(CloseCountingHttpClient::new);
    }

    private S3ClientCache cacheCountingHttpClients(Supplier<CloseCountingHttpClient> httpClientFactory) {
        S3SharedHttpClient sharedHttpClient = new S3SharedHttpClient(() -> newHttpClient(httpClientFactory));
        return new S3ClientCache(sharedHttpClient, CountingSyncHttpClientBuilder::new);
    }

    private SdkAsyncHttpClient newHttpClient() {
        return newHttpClient(CloseCountingHttpClient::new);
    }

    private SdkAsyncHttpClient newHttpClient(Supplier<CloseCountingHttpClient> httpClientFactory) {
        CloseCountingHttpClient client = httpClientFactory.get();
        httpClients.add(client);
        return client;
    }

    @Test
    void theAsyncClientSendsItsRequestsThroughTheSharedHttpClient() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            CompletableFuture<?> request =
                    lease.asyncClient().headObject(head -> head.bucket("bucket").key("key"));

            assertThatThrownBy(() -> request.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertThat(httpClients.get(0).requests).isPositive();
        }
    }

    @Test
    void clientSetsOfDifferentKeysShareOneHttpClient() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease east = cache.acquire(EAST);
                S3ClientCache.Lease west = cache.acquire(WEST)) {
            assertThat(east.asyncClient()).isNotSameAs(west.asyncClient());
            assertThat(httpClients).hasSize(1);
        }
    }

    @Test
    void theSharedHttpClientClosesWithTheLastClientSet() {
        S3ClientCache cache = cacheCountingHttpClients();
        S3ClientCache.Lease east = cache.acquire(EAST);
        S3ClientCache.Lease west = cache.acquire(WEST);

        east.close();
        assertThat(httpClients.get(0).closes).isZero();

        west.close();
        assertThat(httpClients.get(0).closes).isEqualTo(1);
    }

    @Test
    void aClientSetBuiltAfterTheLastReleaseGetsANewHttpClient() {
        S3ClientCache cache = cacheCountingHttpClients();
        cache.acquire(EAST).close();

        try (S3ClientCache.Lease later = cache.acquire(EAST)) {
            assertThat(httpClients).hasSize(2);
            assertThat(httpClients.get(1).closes).isZero();
        }
    }

    @Test
    void aClientSetFailingToBuildReleasesTheHttpClient() {
        S3ClientCache cache = cacheCountingHttpClients();
        S3ClientCache.Key blankRegion = S3ClientCache.key(" ", null, true, null, null, null, false);

        assertThatThrownBy(() -> cache.acquire(blankRegion)).isInstanceOf(RuntimeException.class);

        assertThat(cache.entryCount()).isZero();
        assertThat(httpClients.get(0).closes).isEqualTo(1);
    }

    @Test
    void aClientSetWithSettingsFailingToResolveLeavesNoHttpClientOpen() {
        S3SharedHttpClient sharedHttpClient = new S3SharedHttpClient(this::newHttpClient);
        S3ClientCache cache = new S3ClientCache(sharedHttpClient, () -> {
            throw new IllegalArgumentException("Invalid system property");
        });

        assertThatThrownBy(() -> cache.acquire(EAST))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid system property");

        assertThat(cache.entryCount()).isZero();
        assertThat(httpClients).allSatisfy(client -> assertThat(client.closes).isEqualTo(1));
    }

    @Test
    void aClientSetFailingToBuildWithAnErrorLeavesNoHttpClientOpen() {
        S3SharedHttpClient sharedHttpClient = new S3SharedHttpClient(this::newHttpClient);
        AssertionError failure = new AssertionError("settings failed to load");
        S3ClientCache cache = new S3ClientCache(sharedHttpClient, () -> {
            throw failure;
        });

        assertThatThrownBy(() -> cache.acquire(EAST)).isSameAs(failure);

        assertThat(cache.entryCount()).isZero();
        assertThat(httpClients)
                .hasSize(1)
                .allSatisfy(client -> assertThat(client.closes).isEqualTo(1));
    }

    @Test
    void aClientSetFailingAfterItsSyncClientIsBuiltClosesIt() {
        // the async client names its HTTP client when built, after the sync client
        S3ClientCache cache = cacheCountingHttpClients(UnnamedHttpClient::new);

        assertThatThrownBy(() -> cache.acquire(EAST)).hasMessage("no name");

        assertThat(syncHttpClients)
                .singleElement()
                .satisfies(client -> assertThat(client.closes).isEqualTo(1));
        assertThat(httpClients)
                .singleElement()
                .satisfies(client -> assertThat(client.closes).isEqualTo(1));
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void closingALeaseTwiceKeepsTheHttpClientForTheOtherClientSets() {
        S3ClientCache cache = cacheCountingHttpClients();
        S3ClientCache.Lease east = cache.acquire(EAST);
        try (S3ClientCache.Lease west = cache.acquire(WEST)) {
            east.close();
            east.close();

            assertThat(httpClients.get(0).closes).isZero();
        }
    }

    @Test
    void aFailingCloseStillForgetsTheClientSet() {
        IllegalStateException closeFailure = new IllegalStateException("close failed");
        S3ClientCache cache = cacheCountingHttpClients(() -> new FailingToCloseHttpClient(closeFailure));
        S3ClientCache.Lease lease = cache.acquire(EAST);

        assertThatThrownBy(lease::close).isSameAs(closeFailure);

        assertThat(syncHttpClients)
                .singleElement()
                .satisfies(client -> assertThat(client.closes).isEqualTo(1));
        assertThat(cache.entryCount()).isZero();
    }

    /** Counts its requests and closes; fails every request, since these tests have no endpoint to reach. */
    private static class CloseCountingHttpClient implements SdkAsyncHttpClient {

        private volatile int requests;
        private int closes;

        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
            requests++;
            IllegalStateException failure = new IllegalStateException("no endpoint in this test");
            request.responseHandler().onError(failure);
            return CompletableFuture.failedFuture(failure);
        }

        @Override
        public void close() {
            closes++;
        }
    }

    /** Fails when an async S3 client asks for its name while it builds. */
    private static final class UnnamedHttpClient extends CloseCountingHttpClient {

        @Override
        public String clientName() {
            throw new IllegalStateException("no name");
        }
    }

    /** Fails every close after counting it. */
    private static final class FailingToCloseHttpClient extends CloseCountingHttpClient {

        private final RuntimeException closeFailure;

        FailingToCloseHttpClient(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            super.close();
            throw closeFailure;
        }
    }

    /** Builds the HTTP client of a sync S3 client; the S3 client closes it with itself. */
    private final class CountingSyncHttpClientBuilder implements SdkHttpClient.Builder<CountingSyncHttpClientBuilder> {

        @Override
        public SdkHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
            CloseCountingSyncHttpClient client = new CloseCountingSyncHttpClient();
            syncHttpClients.add(client);
            return client;
        }
    }

    /** Counts its closes; fails every request, since these tests have no endpoint to reach. */
    private static final class CloseCountingSyncHttpClient implements SdkHttpClient {

        private int closes;

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            throw new UnsupportedOperationException("no endpoint in this test");
        }

        @Override
        public void close() {
            closes++;
        }
    }

    @Test
    void sameKeyReturnsSameLeaseHandle() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease a = cache.acquire(key);
                S3ClientCache.Lease b = cache.acquire(key)) {
            assertThat(a.client()).isSameAs(b.client());
        }
    }

    @Test
    void differentKeysReturnDifferentClients() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key k1 = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        S3ClientCache.Key k2 = S3ClientCache.key("us-west-2", null, false, null, null, null, false);
        try (S3ClientCache.Lease a = cache.acquire(k1);
                S3ClientCache.Lease b = cache.acquire(k2)) {
            assertThat(a.client()).isNotSameAs(b.client());
        }
    }

    @Test
    void clientIsClosedWhenLastLeaseReleased() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        S3ClientCache.Lease a = cache.acquire(key);
        S3ClientCache.Lease b = cache.acquire(key);
        a.close();
        assertThat(cache.entryCount()).isEqualTo(1);
        b.close();
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void leaseExposesPresigner() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.presigner()).isNotNull();
        }
    }

    @Test
    void leaseExposesAsyncClientAndTransferManager() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.asyncClient()).isNotNull();
            assertThat(lease.transferManager()).isNotNull();
        }
    }
}
