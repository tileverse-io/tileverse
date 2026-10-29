/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
        azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.37.0");
        azurite.start();
        cache = new AzureClientCache();
    }

    @AfterAll
    static void stopContainer() {
        if (azurite != null) {
            azurite.stop();
        }
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

    @Test
    void sasTokenInTheUrlAuthenticates() throws IOException {
        String privateContainer = "sas-" + UUID.randomUUID().toString().substring(0, 12);
        try (AzureClientCache.Lease setup = cache.acquire(keyFor())) {
            BlobContainerClient containerClient =
                    setup.blobServiceClient().getBlobContainerClient(privateContainer);
            containerClient.create();
            containerClient.getBlobClient("blob.bin").upload(BinaryData.fromBytes(new byte[] {1, 2, 3}));
            BlobContainerSasPermission readOnly = new BlobContainerSasPermission().setReadPermission(true);
            OffsetDateTime expiry = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
            String sas = containerClient.generateSas(new BlobServiceSasSignatureValues(expiry, readOnly));
            try {
                assertSasInUrlReadsAndAnonymousFails(privateContainer, sas);
            } finally {
                containerClient.delete();
            }
        }
    }

    private void assertSasInUrlReadsAndAnonymousFails(String privateContainer, String sas) throws IOException {
        AzureBlobStorageProvider provider = new AzureBlobStorageProvider(cache);
        URI containerUri = URI.create(blobEndpoint() + "/" + privateContainer + "/");
        StorageConfig anonymous = new StorageConfig(containerUri)
                .providerId(AzureBlobStorageProvider.ID)
                .setParameter(AzureBlobStorageProvider.AZURE_ANONYMOUS, true);
        try (Storage storage = provider.createStorage(anonymous)) {
            assertThatThrownBy(() -> storage.read("blob.bin")).isInstanceOf(StorageException.class);
        }
        StorageConfig withSas =
                new StorageConfig(URI.create(containerUri + "?" + sas)).providerId(AzureBlobStorageProvider.ID);
        try (Storage storage = provider.createStorage(withSas);
                ReadHandle handle = storage.read("blob.bin")) {
            assertThat(handle.content().readAllBytes()).containsExactly(1, 2, 3);
        }
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
        try (AzureClientCache.Lease setup = cache.acquire(keyFor())) {
            setup.blobServiceClient().getBlobContainerClient(container).deleteIfExists();
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
