/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
        String emulatorEndpoint = gcsEmulator.url();
        return new SdkStorageCache.Key(
                Optional.of(emulatorEndpoint),
                Optional.of("test-project"),
                Optional.empty(),
                true,
                Optional.empty(),
                Optional.empty());
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
