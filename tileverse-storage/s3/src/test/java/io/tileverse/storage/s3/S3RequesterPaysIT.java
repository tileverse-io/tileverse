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

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;

/**
 * Wire-level verification of {@link S3StorageProvider#S3_REQUESTER_PAYS}: every operation builds an SDK request and the
 * SDK serialises {@code RequestPayer.REQUESTER} as {@code x-amz-request-payer: requester}. An
 * {@link ExecutionInterceptor} attached to the client records each outgoing {@link SdkHttpRequest}; tests assert that
 * the header is present (when {@code requesterPays=true}) or absent (when {@code requesterPays=false}) for each
 * affected operation.
 *
 * <p>LocalStack does not enforce the header server-side, so the interceptor is the source of truth; LocalStack's role
 * is only to make the calls succeed end-to-end.
 *
 * <p>Presigned URL header embedding is asserted by inspecting the URL's query string for {@code x-amz-request-payer}
 * (and confirming it is present in {@code x-amz-signedheaders} when SigV4 chooses to sign it as a header).
 */
@Testcontainers(disabledWithoutDocker = true)
@Execution(ExecutionMode.SAME_THREAD)
class S3RequesterPaysIT {

    @Container
    @SuppressWarnings("resource")
    private static LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0")).withServices("s3");

    private static final String REQUEST_PAYER_HEADER = "x-amz-request-payer";

    private static final List<SdkHttpRequest> capturedRequests = new CopyOnWriteArrayList<>();

    private static final ExecutionInterceptor CAPTURING_INTERCEPTOR = new ExecutionInterceptor() {
        @Override
        public void beforeTransmission(Context.BeforeTransmission ctx, ExecutionAttributes attrs) {
            capturedRequests.add(ctx.httpRequest());
        }
    };

    private static S3AsyncClient client;

    private String bucket;

    @BeforeAll
    static void setUp() {
        StaticCredentialsProvider creds = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));
        client = S3AsyncClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(creds)
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(o -> o.addExecutionInterceptor(CAPTURING_INTERCEPTOR))
                .build();
    }

    @AfterAll
    static void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @BeforeEach
    void createBucket() {
        bucket = "rp-" + UUID.randomUUID().toString().substring(0, 12);
        client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
                .join();
        capturedRequests.clear();
    }

    @AfterEach
    void deleteBucket() {
        try {
            client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build())
                    .join();
        } catch (Exception ignored) {
            // best-effort
        }
    }

    @Test
    @SuppressWarnings("java:S3415") // capturedRequests is the actual value: the requests sent by the client
    void requesterPaysHeaderOnEveryStorageOperation() throws IOException {
        try (S3Storage storage = openStorage(true)) {
            storage.put("a.bin", new byte[] {1, 2, 3}, WriteOptions.defaults());
            storage.put("b.bin", new byte[] {4, 5}, WriteOptions.defaults());
            storage.stat("a.bin");
            try (var stream = storage.list("**")) {
                stream.toList();
            }
            storage.copy("a.bin", "c.bin", io.tileverse.storage.CopyOptions.defaults());
            storage.delete("c.bin");
            storage.deleteAll(List.of("a.bin", "b.bin"));
        }

        // Every captured outgoing request must carry the header.
        assertThat(capturedRequests).isNotEmpty();
        assertThat(capturedRequests)
                .allSatisfy(req -> assertThat(headerValue(req, REQUEST_PAYER_HEADER))
                        .as("expected header on %s %s", req.method(), req.encodedPath())
                        .hasValue("requester"));
    }

    @Test
    @SuppressWarnings("java:S3415") // capturedRequests is the actual value: the requests sent by the client
    void requesterPaysHeaderOnRangeReader() throws IOException {
        // Seed an object with requesterPays disabled, then re-open the reader with requesterPays enabled
        // so the captured requests reflect the read path only.
        try (S3Storage seed = openStorage(false)) {
            seed.put("data.bin", new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, WriteOptions.defaults());
        }
        capturedRequests.clear();

        try (S3Storage storage = openStorage(true);
                RangeReader reader = storage.openRangeReader("data.bin")) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            reader.readRange(0, 4, buf);
        }

        // openRangeReader issues no request; the range read above is the only captured call (GET).
        assertThat(capturedRequests).isNotEmpty();
        assertThat(capturedRequests)
                .allSatisfy(req -> assertThat(headerValue(req, REQUEST_PAYER_HEADER))
                        .as("expected header on %s %s", req.method(), req.encodedPath())
                        .hasValue("requester"));
    }

    @Test
    @SuppressWarnings("java:S3415") // capturedRequests is the actual value: the requests sent by the client
    void noHeaderWhenRequesterPaysDisabled() throws IOException {
        try (S3Storage storage = openStorage(false)) {
            storage.put("a.bin", new byte[] {1, 2, 3}, WriteOptions.defaults());
            storage.stat("a.bin");
            try (var stream = storage.list("**")) {
                stream.toList();
            }
            storage.delete("a.bin");
        }

        assertThat(capturedRequests).isNotEmpty();
        assertThat(capturedRequests)
                .allSatisfy(req -> assertThat(headerValue(req, REQUEST_PAYER_HEADER))
                        .as("expected NO header on %s %s", req.method(), req.encodedPath())
                        .isEmpty());
    }

    @Test
    void presignGetUrlEmbedsRequesterPays() throws IOException {
        S3ClientCache cache = new S3ClientCache();
        S3ClientCache.Key key = S3ClientCache.key(
                localstack.getRegion(),
                localstack.getEndpoint(),
                false,
                localstack.getAccessKey(),
                localstack.getSecretKey(),
                null,
                true);
        URI baseUri = URI.create("s3://" + bucket + "/");
        try (S3Storage storage = new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), cache.acquire(key), true)) {
            storage.put("p.bin", new byte[] {9}, WriteOptions.defaults());
            URI url = storage.presignGet("p.bin", java.time.Duration.ofMinutes(5));
            // SigV4 either embeds the header value as a query parameter or signs it as a required
            // header (declared in X-Amz-SignedHeaders). Either form binds the requester-pays
            // semantics to the URL; assert that one of them is present.
            String query = url.getQuery() == null ? "" : url.getQuery().toLowerCase();
            assertThat(query)
                    .as("presigned URL must commit to requester-pays")
                    .satisfiesAnyOf(
                            q -> assertThat((String) q).contains("x-amz-request-payer=requester"),
                            q -> assertThat((String) q).contains("x-amz-request-payer"));
        }
    }

    private S3Storage openStorage(boolean requesterPays) {
        URI baseUri = URI.create("s3://" + bucket + "/");
        S3StorageBucketKey ref = S3StorageBucketKey.parse(baseUri);
        return new S3Storage(baseUri, ref, new BorrowedS3Handle(client), requesterPays);
    }

    private static Optional<String> headerValue(SdkHttpRequest req, String name) {
        return req.headers().entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(name))
                .map(java.util.Map.Entry::getValue)
                .filter(v -> !v.isEmpty())
                .map(v -> v.get(0))
                .findFirst();
    }
}
