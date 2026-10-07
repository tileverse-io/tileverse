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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

class S3ClientCacheTest {

    private static final S3ClientCache.Key EAST = S3ClientCache.key("us-east-1", null, true, null, null, null, false);
    private static final S3ClientCache.Key WEST = S3ClientCache.key("us-west-2", null, true, null, null, null, false);
    private static final S3HttpClientSettings HTTP_CLIENT_SETTINGS =
            new S3HttpClientSettings(50, Duration.ofSeconds(2), Duration.ofSeconds(30));

    private final List<CloseCountingHttpClient> httpClients = new ArrayList<>();

    private S3ClientCache cacheCountingHttpClients() {
        return cacheCountingHttpClients(CloseCountingHttpClient::new);
    }

    private S3ClientCache cacheCountingHttpClients(Supplier<CloseCountingHttpClient> httpClientFactory) {
        S3SharedHttpClient sharedHttpClient =
                new S3SharedHttpClient(() -> HTTP_CLIENT_SETTINGS, settings -> newHttpClient(httpClientFactory));
        return new S3ClientCache(sharedHttpClient);
    }

    private SdkAsyncHttpClient newHttpClient(Supplier<CloseCountingHttpClient> httpClientFactory) {
        CloseCountingHttpClient client = httpClientFactory.get();
        httpClients.add(client);
        return client;
    }

    @Test
    void withoutKeysOrProfileTheClientsUseTheDefaultCredentialsChain() {
        S3ClientCache.Key nothingSet = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(nothingSet)) {
            assertThat(lease.client().serviceClientConfiguration().credentialsProvider())
                    .isInstanceOf(DefaultCredentialsChain.class);
        }
    }

    @Test
    void theClientSendsItsRequestsThroughTheSharedHttpClient() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            CompletableFuture<?> request =
                    lease.client().headObject(head -> head.bucket("bucket").key("key"));

            assertThatThrownBy(() -> request.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertThat(httpClients.get(0).requests).isPositive();
        }
    }

    @Test
    void theClientOfASetSendsAWholeObjectGetAsOneRequest() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            GetObjectRequest wholeObject =
                    GetObjectRequest.builder().bucket("bucket").key("key").build();

            CompletableFuture<?> read = lease.client().getObject(wholeObject, AsyncResponseTransformer.toBytes());

            assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertThat(httpClients.get(0).sent).isNotEmpty().allSatisfy(request -> {
                assertThat(request.rawQueryParameters()).doesNotContainKey("partNumber");
                assertThat(request.firstMatchingHeader("Range")).isEmpty();
            });
        }
    }

    @Test
    void theTransferManagerOfASetUploadsAFileAboveTheThresholdInParts(@TempDir Path dir) throws IOException {
        // the multipart client splits uploads above 8 MiB, its default threshold and part size
        int aboveTheThreshold = 8 * 1024 * 1024 + 1;
        Path source = dir.resolve("source.bin");
        Files.write(source, new byte[aboveTheThreshold]);
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            UploadFileRequest request = UploadFileRequest.builder()
                    .source(source)
                    .putObjectRequest(put -> put.bucket("bucket").key("key"))
                    .build();
            S3TransferManager transferManager = lease.transferManager();

            FileUpload upload = transferManager.uploadFile(request);

            CompletableFuture<?> uploaded = upload.completionFuture();
            assertThatThrownBy(() -> uploaded.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
            assertThat(httpClients.get(0).sent).first().satisfies(createMultipartUpload -> {
                assertThat(createMultipartUpload.method()).isEqualTo(SdkHttpMethod.POST);
                assertThat(createMultipartUpload.rawQueryParameters()).containsKey("uploads");
            });
        }
    }

    @Test
    void clientSetsOfDifferentKeysShareOneHttpClient() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease east = cache.acquire(EAST);
                S3ClientCache.Lease west = cache.acquire(WEST)) {
            assertThat(east.client()).isNotSameAs(west.client());
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
    void aSharedHttpClientFailingToBuildLeavesNoClientSet() {
        S3SharedHttpClient failing = new S3SharedHttpClient(() -> {
            throw new IllegalArgumentException("Invalid system property");
        });
        S3ClientCache cache = new S3ClientCache(failing);

        assertThatThrownBy(() -> cache.acquire(EAST))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid system property");
        assertThat(cache.entryCount()).isZero();
    }

    @Test
    void aClientSetFailingToBuildWithAnErrorLeavesNoHttpClientOpen() {
        AssertionError failure = new AssertionError("client failed to build");
        S3ClientCache cache = cacheCountingHttpClients(() -> new UnnamedHttpClient(1, failure));

        assertThatThrownBy(() -> cache.acquire(EAST)).isSameAs(failure);

        assertThat(cache.entryCount()).isZero();
        assertThat(httpClients)
                .singleElement()
                .satisfies(client -> assertThat(client.closes).isEqualTo(1));
    }

    @Test
    void aClientSetOpensOnlyItsClientUntilAskedForMore() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            assertThat(lease.openedClients())
                    .as("the HTTP client lease and the client")
                    .isEqualTo(2);
        }
    }

    @Test
    void theFirstTransferManagerRequestOpensTheMultipartClientWithIt() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            S3TransferManager first = lease.transferManager();

            assertThat(lease.transferManager()).isSameAs(first);
            assertThat(lease.openedClients())
                    .as("plus the multipart client and the transfer manager")
                    .isEqualTo(4);
        }
    }

    @Test
    void concurrentFirstRequestsBuildOneTransferManager() throws Exception {
        S3ClientCache cache = cacheCountingHttpClients();
        int callers = 8;
        ExecutorService threads = Executors.newFixedThreadPool(callers);
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<S3TransferManager>> requests = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                requests.add(threads.submit(() -> {
                    start.await();
                    return lease.transferManager();
                }));
            }
            start.countDown();
            Set<S3TransferManager> built = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Future<S3TransferManager> request : requests) {
                built.add(request.get(10, TimeUnit.SECONDS));
            }

            assertThat(built).hasSize(1);
            assertThat(lease.openedClients()).isEqualTo(4);
        } finally {
            threads.shutdownNow();
        }
    }

    @Test
    void thePresignerOpensOnFirstUse() {
        S3ClientCache cache = cacheCountingHttpClients();
        try (S3ClientCache.Lease lease = cache.acquire(EAST)) {
            S3Presigner presigner = lease.presigner();

            assertThat(lease.presigner()).isSameAs(presigner);
            assertThat(lease.openedClients()).isEqualTo(3);
        }
    }

    @Test
    void aLeasedHandlePresignsWithoutOpeningThePresigner() {
        S3ClientCache cache = cacheCountingHttpClients();
        S3ClientCache.Lease lease = cache.acquire(EAST);
        try (LeasedS3Handle handle = new LeasedS3Handle(lease)) {
            assertThat(handle.presigns()).isTrue();
            assertThat(lease.openedClients())
                    .as("the HTTP client lease and the client")
                    .isEqualTo(2);
        }
    }

    @Test
    void aMemberRequestedAfterTheSetClosedFails() {
        S3ClientCache cache = cacheCountingHttpClients();
        S3ClientCache.Lease lease = cache.acquire(EAST);
        lease.close();
        int openedBeforeTheRequests = lease.openedClients();

        assertThatThrownBy(lease::transferManager).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(lease::presigner).isInstanceOf(IllegalStateException.class);
        assertThat(lease.openedClients()).as("nothing opened").isEqualTo(openedBeforeTheRequests);
    }

    @Test
    void aTransferManagerFailingToBuildLeavesTheSetUsable() {
        // a client asks the HTTP client for its name twice as it builds: the third call comes from the multipart
        // client
        S3ClientCache cache = cacheCountingHttpClients(() -> new UnnamedHttpClient(3, null));
        S3ClientCache.Lease lease = cache.acquire(EAST);

        assertThatThrownBy(lease::transferManager)
                .hasMessage("no name")
                .hasStackTraceContaining("ClientFactory.multipartClient");

        assertThat(lease.openedClients()).isEqualTo(2);
        assertThat(lease.presigner()).isNotNull();
        assertThat(lease.openedClients()).as("plus the presigner").isEqualTo(3);
        assertThat(httpClients.get(0).closes).isZero();
        lease.close();
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

        assertThat(cache.entryCount()).isZero();
    }

    /** Counts its requests and closes; fails every request, since these tests have no endpoint to reach. */
    private static class CloseCountingHttpClient implements SdkAsyncHttpClient {

        final List<SdkHttpRequest> sent = new CopyOnWriteArrayList<>();
        private volatile int requests;
        private int closes;

        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
            sent.add(request.request());
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

    /** Fails from the {@code failingFrom}th time an async S3 client asks for its name while it builds. */
    private static final class UnnamedHttpClient extends CloseCountingHttpClient {

        private final int failingFrom;
        private final Error error;
        private int names;

        /** @param error thrown instead of an IllegalStateException, when not null */
        UnnamedHttpClient(int failingFrom, Error error) {
            this.failingFrom = failingFrom;
            this.error = error;
        }

        @Override
        public String clientName() {
            names++;
            if (names < failingFrom) {
                return super.clientName();
            }
            if (error != null) {
                throw error;
            }
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
    void leaseExposesClientAndTransferManager() {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key("us-east-1", null, false, null, null, null, false);
        try (S3ClientCache.Lease lease = cache.acquire(key)) {
            assertThat(lease.client()).isNotNull();
            assertThat(lease.transferManager()).isNotNull();
        }
    }
}
