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

import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * S3 TCK against a LocalStack container.
 *
 * <p>Known LocalStack 3.2 limitations vs. real S3, handled by overriding the affected TCK methods to skip via JUnit
 * assumptions:
 *
 * <ul>
 *   <li>{@code If-None-Match: *} on PutObject is not enforced (no PreconditionFailedException).
 * </ul>
 *
 * <p>Mutates {@code aws.accessKeyId}/{@code aws.secretAccessKey} system properties in {@code @BeforeAll} so the SDK
 * default credentials chain finds the LocalStack container's credentials. {@link ResourceLock} on
 * {@link Resources#SYSTEM_PROPERTIES} serializes execution against {@code S3StorageMinIOIT} (which mutates the same
 * keys) and any other test that locks SYSTEM_PROPERTIES.
 */
@Testcontainers(disabledWithoutDocker = true)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class S3StorageLocalStackIT extends StorageTCK {

    @Container
    @SuppressWarnings("resource")
    private static LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0")).withServices(Service.S3);

    private static S3ClientCache cache;
    private String bucket;

    @BeforeAll
    static void setAuth() {
        cache = new S3ClientCache();
        // Set system properties so the SDK auth chain finds creds when our tests build clients.
        System.setProperty("aws.accessKeyId", localstack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localstack.getSecretKey());
    }

    @AfterAll
    static void clearAuth() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
    }

    private S3ClientCache.Key keyFor() {
        return S3ClientCache.key(
                localstack.getRegion(), localstack.getEndpoint(), false, localstack.getAccessKey(), null, null, true);
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
     * Override to skip on LocalStack 3.2: it does not enforce {@code If-None-Match: *} on PutObject (the put succeeds
     * and overwrites the existing key). Real S3 rejects. Verified via S3StorageMinIOIT (MinIO is stricter).
     */
    @Override
    @Test
    @Disabled("LocalStack 3.2 does not enforce If-None-Match: * on PutObject")
    @SuppressWarnings({"java:S2699", "java:S1186"})
    protected void putIfNotExistsRejectsExistingKey() {}

    /**
     * Override to skip on LocalStack 3.2: PutObject with empty body throws a 500 ("'NoneType' object has no attribute
     * 'to_bytes'") regardless of how the RequestBody is constructed. Real S3 and MinIO accept zero-byte uploads.
     */
    @Override
    @Test
    @Disabled("LocalStack 3.2 rejects zero-byte PutObject with internal error")
    @SuppressWarnings({"java:S2699", "java:S1186"})
    protected void openOutputStreamEmptyWriteCreatesZeroLengthBlob() {}

    @Test
    void presignPutCommitsToContentType() {
        requirePresignedUrls();
        PresignWriteOptions options = PresignWriteOptions.builder()
                .contentType("application/octet-stream")
                .build();
        URI url = storage.presignPut("k.bin", Duration.ofMinutes(5), options);
        // SigV4 does not place the content-type value in the URL; it commits to it by including
        // "content-type" in X-Amz-SignedHeaders (URL-encoded as content-type%3Bhost). The uploader
        // must then send a matching Content-Type header or the signature check fails server-side.
        String signedHeaders = url.getQuery().toLowerCase();
        assertThat(signedHeaders).contains("x-amz-signedheaders=").contains("content-type");
    }

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
