/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class GeoParquetFileDataStoreFactoryAzuriteIT {

    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final String CONTAINER_NAME = "geoparquet-it";

    @Container
    @SuppressWarnings("resource")
    static AzuriteContainer azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0")
            .withCommand("azurite-blob --skipApiVersionCheck --loose --blobHost 0.0.0.0 --debug")
            .withExposedPorts(10000, 10001, 10002);

    private static URL azuriteFixtureUrl;

    @BeforeAll
    static void setupAzurite(@TempDir Path tempDir) throws Exception {
        Path parquetFixture = GeoParquetDatastoreCloudITSupport.extractFixture(
                GeoParquetFileDataStoreFactoryAzuriteIT.class, tempDir);
        uploadFixture(parquetFixture);
        String endpoint = "http://%s:%d/%s/%s/%s"
                .formatted(
                        azurite.getHost(),
                        azurite.getMappedPort(10000),
                        ACCOUNT_NAME,
                        CONTAINER_NAME,
                        GeoParquetDatastoreCloudITSupport.OBJECT_NAME);
        azuriteFixtureUrl = new URL(endpoint);
    }

    @Test
    void createDataStore_requiresAzureConnectionParametersForAzuriteHttpUrl() {
        assertThatThrownBy(() ->
                        GeoParquetDatastoreCloudITSupport.FACTORY.createDataStore(Map.of("url", azuriteFixtureUrl)))
                .isInstanceOf(io.tileverse.storage.AccessDeniedException.class)
                .hasMessageContaining("403");
    }

    @Test
    void createDataStore_readsParquetThroughParameterizedAzureRangeReader() throws Exception {
        Map<String, Object> params =
                Map.of("url", azuriteFixtureUrl, "storage.provider", "azure", "storage.azure.account-key", ACCOUNT_KEY);

        GeoParquetDatastoreCloudITSupport.assertReadsSampleGeoParquet(params);
    }

    private static void uploadFixture(Path parquetFixture) {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azurite.getConnectionString())
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.createBlobContainer(CONTAINER_NAME);
        containerClient
                .getBlobClient(GeoParquetDatastoreCloudITSupport.OBJECT_NAME)
                .uploadFromFile(parquetFixture.toString(), true);
    }
}
