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

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.Storage;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.it.GarageContainer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * The async batch path against a real S3-compatible endpoint, on a client built as {@link S3ClientCache} builds it: it
 * keeps at most {@code storage.batch.max-in-flight-fetches} requests outstanding, counted by a proxy around the client
 * from each {@code getObject} call to the completion of its future, and it reports what a batch cost, the bridged gaps
 * included.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3RangeReaderAsyncBatchIT {

    private static final String BUCKET = "async-batch";
    private static final String KEY = "object.bin";
    private static final int OBJECT_SIZE = 4 * 1024 * 1024;
    private static final int FETCH_COUNT = 12;
    private static final int FETCH_LENGTH = 4096;
    private static final long FETCH_STRIDE = 300_000L;

    /** The connections pooled per host by the shared HTTP client, an SDK default. */
    private static final int POOLED_CONNECTIONS = 50;

    @Container
    static GarageContainer garage = new GarageContainer();

    private static byte[] object;
    private static S3Client syncClient;
    private static S3SharedHttpClient.Lease httpClient;
    private static S3AsyncClient asyncClient;

    @BeforeAll
    static void uploadObject() {
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(garage.getAccessKeyId(), garage.getSecretAccessKey()));
        URI endpoint = URI.create(garage.getS3URL());
        syncClient = S3Client.builder()
                .endpointOverride(endpoint)
                .region(Region.of(GarageContainer.REGION))
                .credentialsProvider(credentials)
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
        httpClient = S3SharedHttpClient.INSTANCE.acquire();
        asyncClient = S3AsyncClient.builder()
                .httpClient(httpClient.client())
                .endpointOverride(endpoint)
                .region(Region.of(GarageContainer.REGION))
                .credentialsProvider(credentials)
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
        object = new byte[OBJECT_SIZE];
        new Random(42).nextBytes(object);
        syncClient.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        syncClient.putObject(PutObjectRequest.builder().bucket(BUCKET).key(KEY).build(), RequestBody.fromBytes(object));
    }

    @AfterAll
    static void closeClients() {
        if (asyncClient != null) {
            asyncClient.close();
        }
        if (httpClient != null) {
            httpClient.close();
        }
        if (syncClient != null) {
            syncClient.close();
        }
    }

    @Test
    void theAsyncPathKeepsAtMostTheBoundInFlight() throws IOException {
        int bound = 3;
        InFlightCounter counter = new InFlightCounter(asyncClient);
        BatchSettings noMerging = new BatchSettings(-1, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES, bound);

        BatchReadResult result = readFarApartRanges(counter.client(), noMerging);

        assertThat(counts(result)).containsOnly(FETCH_LENGTH);
        assertThat(counter.total()).as("one request per planned fetch").isEqualTo(FETCH_COUNT);
        assertThat(counter.peak()).as("requests in flight at once").isBetween(1, bound);
        assertThat(result.fetches()).isEqualTo(FETCH_COUNT);
        assertThat(result.bytesTransferred()).isEqualTo(result.bytesRequested());
    }

    @Test
    void aBatchWiderThanTheConnectionPoolCompletes() throws IOException {
        int fetchCount = 200;
        int length = 1024;
        long stride = OBJECT_SIZE / fetchCount;
        InFlightCounter counter = new InFlightCounter(asyncClient);
        BatchSettings unbounded = new BatchSettings(-1, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES, 0);
        List<RangeRequest> requests = new ArrayList<>();
        for (int i = 0; i < fetchCount; i++) {
            requests.add(RangeRequest.of(i * stride, length, ByteBuffer.allocate(length)));
        }

        BatchReadResult result = read(counter.client(), unbounded, requests);

        assertThat(counts(result)).containsOnly(length);
        assertThat(result.fetches()).isEqualTo(fetchCount);
        assertThat(counter.peak()).as("fetches outstanding at once").isGreaterThan(POOLED_CONNECTIONS);
    }

    @Test
    void closingOneClientSetLeavesTheOthersReading() throws IOException {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Lease closing = cache.acquire(keyFor(URI.create(garage.getS3URL())));
        S3ClientCache.Lease reading = cache.acquire(keyFor(URI.create(garage.getS3URL() + "/")));
        URI baseUri = URI.create("s3://" + BUCKET + "/");
        List<RangeRequest> requests = List.of(
                RangeRequest.of(0, FETCH_LENGTH, ByteBuffer.allocate(FETCH_LENGTH)),
                RangeRequest.of(2 * FETCH_STRIDE, FETCH_LENGTH, ByteBuffer.allocate(FETCH_LENGTH)));

        assertThat(reading.client()).isNotSameAs(closing.client());
        closing.close();
        try (Storage storage = new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), reading, false);
                RangeReader reader = storage.openRangeReader(KEY)) {
            assertThat(counts(reader.readRanges(requests))).containsOnly(FETCH_LENGTH);
        }
    }

    private static S3ClientCache.Key keyFor(URI endpoint) {
        return S3ClientCache.key(
                GarageContainer.REGION,
                endpoint,
                false,
                garage.getAccessKeyId(),
                garage.getSecretAccessKey(),
                null,
                true);
    }

    @Test
    void aMergedFetchTransfersTheRequestedBytesPlusTheBridgedGap() throws IOException {
        int gap = 50;
        BatchSettings merging = new BatchSettings(1024, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES, 8);
        List<RangeRequest> requests = List.of(
                RangeRequest.of(1000, 100, ByteBuffer.allocate(100)),
                RangeRequest.of(1000 + 100 + gap, 100, ByteBuffer.allocate(100)));

        BatchReadResult result = read(asyncClient, merging, requests);

        assertThat(counts(result)).containsExactly(100, 100);
        assertThat(result.fetches()).isEqualTo(1);
        assertThat(result.bytesRequested()).isEqualTo(200);
        assertThat(result.bytesTransferred()).isEqualTo(200 + gap);
        assertThat(result.bytesFromCache()).isZero();
    }

    private BatchReadResult readFarApartRanges(S3AsyncClient asyncClient, BatchSettings settings) throws IOException {
        List<RangeRequest> requests = new ArrayList<>();
        for (int i = 0; i < FETCH_COUNT; i++) {
            requests.add(RangeRequest.of(i * FETCH_STRIDE, FETCH_LENGTH, ByteBuffer.allocate(FETCH_LENGTH)));
        }

        BatchReadResult result = read(asyncClient, settings, requests);

        for (int i = 0; i < FETCH_COUNT; i++) {
            int offset = (int) (i * FETCH_STRIDE);
            byte[] expected = Arrays.copyOfRange(object, offset, offset + FETCH_LENGTH);
            assertThat(requests.get(i).target().flip()).isEqualTo(ByteBuffer.wrap(expected));
        }
        return result;
    }

    private BatchReadResult read(S3AsyncClient asyncClient, BatchSettings settings, List<RangeRequest> requests)
            throws IOException {
        URI baseUri = URI.create("s3://" + BUCKET + "/");
        try (Storage storage = new S3Storage(
                        baseUri,
                        S3StorageBucketKey.parse(baseUri),
                        new BorrowedS3Handle(asyncClient),
                        false,
                        settings);
                RangeReader reader = storage.openRangeReader(KEY)) {
            return reader.readRanges(requests);
        }
    }

    /**
     * Wraps an {@link S3AsyncClient} in a proxy that counts the {@code getObject} calls outstanding at once, from the
     * call to the completion of the future it returns. Every other method passes through.
     */
    private static final class InFlightCounter {

        private final S3AsyncClient client;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicInteger total = new AtomicInteger();

        InFlightCounter(S3AsyncClient delegate) {
            this.client = (S3AsyncClient) Proxy.newProxyInstance(
                    S3AsyncClient.class.getClassLoader(),
                    new Class<?>[] {S3AsyncClient.class},
                    (proxy, method, args) -> {
                        boolean rangeGet = "getObject".equals(method.getName())
                                && args != null
                                && args.length == 2
                                && args[1] instanceof AsyncResponseTransformer;
                        if (!rangeGet) {
                            return invoke(delegate, method, args);
                        }
                        total.incrementAndGet();
                        peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                        CompletableFuture<?> outcome = (CompletableFuture<?>) invoke(delegate, method, args);
                        // The reader chains on the returned future; completing it after the decrement keeps
                        // the count exact whatever the reader does on completion.
                        return outcome.whenComplete((ignored, failure) -> inFlight.decrementAndGet());
                    });
        }

        private static Object invoke(S3AsyncClient delegate, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        S3AsyncClient client() {
            return client;
        }

        int peak() {
            return peak.get();
        }

        int total() {
            return total.get();
        }
    }
}
