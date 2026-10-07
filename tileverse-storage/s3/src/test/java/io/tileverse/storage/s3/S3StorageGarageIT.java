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
import io.tileverse.storage.it.GarageContainer;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * S3 TCK against a Garage container. Garage accepts zero-byte uploads, unlike LocalStack.
 *
 * <p>Mutates {@code aws.accessKeyId}/{@code aws.secretAccessKey} system properties in {@code @BeforeAll} for the SDK
 * default credentials chain to find the Garage container's credentials. {@link ResourceLock} on
 * {@link Resources#SYSTEM_PROPERTIES} serializes execution against {@code S3StorageLocalStackIT} (which mutates the
 * same keys) and any other test that locks SYSTEM_PROPERTIES.
 */
@Testcontainers(disabledWithoutDocker = true)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class S3StorageGarageIT extends StorageTCK {

    @SuppressWarnings("resource")
    private static GarageContainer garage;

    private static S3ClientCache cache;
    private String bucket;

    @BeforeAll
    static void startContainer() {
        garage = new GarageContainer();
        garage.start();
        cache = new S3ClientCache();
        System.setProperty("aws.accessKeyId", garage.getAccessKeyId());
        System.setProperty("aws.secretAccessKey", garage.getSecretAccessKey());
    }

    @AfterAll
    static void stopContainer() {
        if (garage != null) {
            garage.stop();
        }
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
    }

    /**
     * Override to skip on Garage: it does not enforce {@code If-None-Match: *} on PutObject (the put succeeds and
     * overwrites the existing key). Verified via S3StorageS3ProxyIT.
     */
    @Override
    @Test
    @Disabled("Garage does not enforce If-None-Match: * on PutObject")
    @SuppressWarnings({"java:S2699", "java:S1186"})
    protected void putIfNotExistsRejectsExistingKey() {}

    private S3ClientCache.Key keyFor() {
        return S3ClientCache.key(
                GarageContainer.REGION,
                URI.create(garage.getS3URL()),
                false,
                garage.getAccessKeyId(),
                null,
                null,
                true);
    }

    @Override
    protected Storage openStorage() throws IOException {
        bucket = "tck-" + UUID.randomUUID().toString().substring(0, 12);
        try (S3ClientCache.Lease setup = cache.acquire(keyFor())) {
            setup.client()
                    .createBucket(software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder()
                            .bucket(bucket)
                            .build())
                    .join();
        }
        URI baseUri = URI.create("s3://" + bucket + "/");
        S3StorageBucketKey ref = S3StorageBucketKey.parse(baseUri);
        S3ClientCache.Lease lease = cache.acquire(keyFor());
        return new S3Storage(baseUri, ref, lease, false);
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
                            .build())
                    .join();
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
