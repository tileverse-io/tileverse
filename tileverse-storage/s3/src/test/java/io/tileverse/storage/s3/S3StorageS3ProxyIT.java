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

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * S3 TCK against an s3proxy container. s3proxy is the strict member of the emulator family: it answers any
 * {@code x-amz-*} header it does not implement with {@code 501 NotImplemented}. In particular it rejects the checksum
 * headers the AWS SDK adds by default since 2.30 ({@code x-amz-checksum-mode}, {@code x-amz-sdk-checksum-algorithm}),
 * making it the emulator that catches checksum-default regressions that MinIO and LocalStack tolerate.
 *
 * <p>Credentials are passed as static keys through the {@link S3ClientCache.Key}, no system property mutation is
 * needed. The container uses the in-memory {@code transient} jclouds provider as its backing store.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3StorageS3ProxyIT extends StorageTCK {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("andrewgaul/s3proxy:latest");
    private static final String IDENTITY = "s3proxy-access-key";
    private static final String CREDENTIAL = "s3proxy-secret-key";

    @SuppressWarnings("resource")
    private static GenericContainer<?> s3proxy;

    private static S3ClientCache cache;
    private String bucket;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() {
        s3proxy = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withExposedPorts(80)
                .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
                .withEnv("S3PROXY_IDENTITY", IDENTITY)
                .withEnv("S3PROXY_CREDENTIAL", CREDENTIAL)
                .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:80")
                .withEnv("JCLOUDS_PROVIDER", "transient");
        s3proxy.start();
        cache = new S3ClientCache();
    }

    @AfterAll
    static void stopContainer() {
        if (s3proxy != null) {
            s3proxy.stop();
        }
    }

    private static URI endpoint() {
        return URI.create("http://" + s3proxy.getHost() + ":" + s3proxy.getMappedPort(80));
    }

    private S3ClientCache.Key keyFor() {
        return S3ClientCache.key("us-east-1", endpoint(), false, IDENTITY, CREDENTIAL, null, true);
    }

    @Override
    protected Storage openStorage() throws IOException {
        bucket = "tck-" + UUID.randomUUID().toString().substring(0, 12);
        try (S3ClientCache.Lease setup = cache.acquire(keyFor())) {
            setup.client()
                    .createBucket(software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
                            .bucket(bucket)
                            .build());
        }
        URI baseUri = URI.create("s3://" + bucket + "/");
        S3StorageBucketKey ref = S3StorageBucketKey.parse(baseUri);
        S3ClientCache.Lease lease = cache.acquire(keyFor());
        return new S3Storage(baseUri, ref, lease, false);
    }

    /**
     * Override to skip on s3proxy: the AWS CRT client fails CreateMultipartUpload against s3proxy with "Upload Id not
     * found in create-multipart-upload response". Multipart uploads are covered by S3StorageMinIOIT and
     * S3StorageLocalStackIT.
     */
    @Override
    @Test
    @Disabled("s3proxy CreateMultipartUpload response is not understood by the AWS CRT client")
    @SuppressWarnings({"java:S2699", "java:S1186"})
    protected void multiPartRoundTrip16MiB(@TempDir Path tmp) {}

    /**
     * Override to skip on s3proxy: its DeleteObjects response omits the per-key {@code <Deleted>} entries, leaving
     * {@code DeleteResult#deleted()} empty even though the objects are removed. Real S3, MinIO and LocalStack report
     * each deleted key.
     */
    @Override
    @Test
    @Disabled("s3proxy DeleteObjects response omits the per-key Deleted entries")
    @SuppressWarnings({"java:S2699", "java:S1186"})
    protected void deleteAllReportsPerKeyOutcome() {}

    /**
     * Override to skip on s3proxy: same DeleteObjects reporting limitation as {@link #deleteAllReportsPerKeyOutcome()}.
     */
    @Override
    @Test
    @Disabled("s3proxy DeleteObjects response omits the per-key Deleted entries")
    @SuppressWarnings({"java:S2699", "java:S1186"})
    protected void deleteAllLumpsAllIntoDeletedWhenIncapable() {}

    @Override
    protected void cleanUp(Storage s) throws IOException {
        try (Stream<StorageEntry> stream = s.list("**")) {
            List<String> all = stream.filter(e -> e instanceof StorageEntry.File)
                    .map(StorageEntry::key)
                    .toList();
            if (!all.isEmpty()) {
                s.deleteAll(all);
            }
        } catch (Exception ignored) {
            // best-effort
        }
        try (S3ClientCache.Lease setup = cache.acquire(keyFor())) {
            setup.client()
                    .deleteBucket(software.amazon.awssdk.services.s3.model.DeleteBucketRequest.builder()
                            .bucket(bucket)
                            .build());
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
