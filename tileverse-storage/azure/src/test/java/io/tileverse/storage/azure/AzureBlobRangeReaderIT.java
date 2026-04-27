/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.it.AbstractRangeReaderIT;
import io.tileverse.storage.it.TestUtil;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for AzureBlobRangeReader using Azurite.
 *
 * <p>These tests verify that the AzureBlobRangeReader can correctly read ranges of bytes from an Azure Blob Storage
 * container using the Azure SDK against an Azurite container.
 */
@Testcontainers(disabledWithoutDocker = true)
class AzureBlobRangeReaderIT extends AbstractRangeReaderIT {

    private static final String CONTAINER_NAME = "test-container";
    private static final String BLOB_NAME = "test.bin";

    // default account name and key used by Azurite
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private static Path testFile;
    private static BlobServiceClient blobServiceClient;
    private static BlobContainerClient containerClient;

    @Container
    @SuppressWarnings("resource")
    static AzuriteContainer azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0")
            // .withEnv("AZURITE_BLOB_LOOSE", "true")
            .withCommand("azurite-blob --skipApiVersionCheck --loose --blobHost 0.0.0.0 --debug")
            .withExposedPorts(10000, 10001, 10002);

    @BeforeAll
    static void setupAzure() throws IOException {
        testFile = TestUtil.createTempTestFile(TEST_FILE_SIZE);
        blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azurite.getConnectionString())
                .buildClient();

        containerClient = blobServiceClient.createBlobContainer(CONTAINER_NAME);
        containerClient.getBlobClient(BLOB_NAME).uploadFromFile(testFile.toString(), true);
    }

    @AfterAll
    static void cleanupAzure() {
        if (containerClient != null) {
            try {
                containerClient.delete();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Creates an {@link AzureBlobRangeReader} via {@link AzureBlobStorageProvider#open(URI, BlobServiceClient)} using
     * the Azurite-backed {@link BlobServiceClient}. The returned reader owns the parent
     * {@link io.tileverse.storage.Storage} (close releases the borrowed-handle wrapper).
     */
    @Override
    protected RangeReader createBaseReader() throws IOException {
        Integer port = azurite.getMappedPort(10000);
        URI containerUri = URI.create("http://localhost:%d/%s/%s/".formatted(port, ACCOUNT_NAME, CONTAINER_NAME));
        io.tileverse.storage.Storage storage = AzureBlobStorageProvider.open(containerUri, blobServiceClient);
        try {
            return new io.tileverse.storage.spi.OwnedRangeReader(storage.openRangeReader(BLOB_NAME), storage);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Test
    void testAzureBlobWithAccountCredentials() throws IOException {

        String azuriteHost = azurite.getHost();
        Integer azuritePort = azurite.getMappedPort(10000);
        String blobEndpoint = String.format("http://%s:%d/%s", azuriteHost, azuritePort, ACCOUNT_NAME);

        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(ACCOUNT_NAME, ACCOUNT_KEY);

        BlobServiceClient privateBlobServiceClient = new BlobServiceClientBuilder()
                .endpoint(blobEndpoint)
                .credential(credential)
                .buildClient();

        BlobClient blobClient =
                privateBlobServiceClient.getBlobContainerClient(CONTAINER_NAME).getBlobClient(BLOB_NAME);
        try (AzureBlobRangeReader reader = new AzureBlobRangeReader(blobClient)) {
            assertThat(reader)
                    .as("Should create reader with account credentials")
                    .isNotNull();
        }
    }
}
