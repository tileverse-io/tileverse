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
package io.tileverse.storage.gcs;

import com.google.cloud.storage.BucketInfo;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * StorageTCK against fake-gcs-server. The emulator does not implement HNS, versioning, or signed-URL features; HNS-only
 * and presign-only tests are either skipped via capability gates or overridden to skip explicitly.
 */
@Testcontainers(disabledWithoutDocker = true)
class GoogleCloudStorageEmulatorIT extends StorageTCK {

    static FakeGcsServerContainer gcsEmulator = new FakeGcsServerContainer();

    private static SdkStorageCache cache;
    private String bucket;

    @BeforeAll
    static void startContainer() {
        gcsEmulator.start();
        cache = new SdkStorageCache();
    }

    @AfterAll
    static void stopContainer() {
        gcsEmulator.stop();
    }

    private SdkStorageCache.Key keyFor() {
        String emulatorEndpoint = "http://" + gcsEmulator.getHost() + ":" + gcsEmulator.getFirstMappedPort();
        return new SdkStorageCache.Key(
                Optional.of(emulatorEndpoint), Optional.of("test-project"), Optional.empty(), true, Optional.empty());
    }

    @Override
    protected Storage openStorage() throws IOException {
        bucket = "tck-" + UUID.randomUUID().toString().substring(0, 12);
        try (SdkStorageCache.Lease setup = cache.acquire(keyFor())) {
            setup.client().create(BucketInfo.newBuilder(bucket).build());
        }
        URI baseUri = URI.create("gs://" + bucket + "/");
        SdkStorageLocation location = SdkStorageLocation.parse(baseUri);
        SdkStorageCache.Lease lease = cache.acquire(keyFor());
        return new GoogleCloudStorage(baseUri, location, lease, Optional.empty());
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
        try (SdkStorageCache.Lease setup = cache.acquire(keyFor())) {
            setup.client().delete(bucket);
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
