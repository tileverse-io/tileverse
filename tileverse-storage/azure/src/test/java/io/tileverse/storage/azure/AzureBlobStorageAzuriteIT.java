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
package io.tileverse.storage.azure;

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
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * StorageTCK against an Azurite container. Azurite emulates Azure Blob Storage with the well-known dev account
 * ({@code devstoreaccount1}) and a fixed shared key. The Azurite SAS implementation works for blob URIs.
 */
@Testcontainers(disabledWithoutDocker = true)
class AzureBlobStorageAzuriteIT extends StorageTCK {

    static final String ACCOUNT_NAME = "devstoreaccount1";
    static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    @SuppressWarnings("resource")
    private static AzuriteContainer azurite;

    private static AzureClientCache cache;
    private String container;
    private String endpoint;

    @BeforeAll
    static void startContainer() {
        azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0")
                .withCommand("azurite-blob --skipApiVersionCheck --loose --blobHost 0.0.0.0");
        azurite.start();
        cache = new AzureClientCache();
    }

    @AfterAll
    static void stopContainer() {
        if (azurite != null) azurite.stop();
    }

    private String blobEndpoint() {
        return "http://" + azurite.getHost() + ":" + azurite.getMappedPort(10000) + "/" + ACCOUNT_NAME;
    }

    private AzureClientCache.Key keyFor() {
        return AzureClientCache.key(
                blobEndpoint(),
                ACCOUNT_NAME,
                ACCOUNT_KEY,
                null,
                null,
                false,
                new AzureRetryConfig(3, Duration.ofSeconds(4), Duration.ofMinutes(2), Duration.ofSeconds(60)));
    }

    @Override
    protected Storage openStorage() throws IOException {
        endpoint = blobEndpoint();
        container = "tck-" + UUID.randomUUID().toString().substring(0, 12);
        try (AzureClientCache.Lease setup = cache.acquire(keyFor())) {
            setup.blobServiceClient().getBlobContainerClient(container).create();
        }
        URI baseUri = URI.create(endpoint + "/" + container);
        AzureBlobLocation location = AzureBlobLocation.parse(baseUri);
        AzureClientCache.Lease lease = cache.acquire(keyFor());
        return new AzureBlobStorage(baseUri, location, lease);
    }

    @Override
    protected void cleanUp(Storage s) throws IOException {
        try (Stream<StorageEntry> stream = s.list("**")) {
            List<String> all = stream.filter(e -> e instanceof StorageEntry.File)
                    .map(StorageEntry::key)
                    .toList();
            if (!all.isEmpty()) s.deleteAll(all);
        } catch (Exception ignored) {
            // best-effort
        }
        try (AzureClientCache.Lease setup = cache.acquire(keyFor())) {
            setup.blobServiceClient().getBlobContainerClient(container).deleteIfExists();
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
