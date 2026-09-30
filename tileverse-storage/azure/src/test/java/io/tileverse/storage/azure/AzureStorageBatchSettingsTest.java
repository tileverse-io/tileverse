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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.batch.BatchProviderHelper;
import io.tileverse.storage.batch.BatchSettings;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.Test;

/** The batch settings of an Azure Storage, Blob or Data Lake, reach every reader it opens. */
class AzureStorageBatchSettingsTest {

    private static final URI BLOB_URI = URI.create("https://account.blob.core.windows.net/container/prefix/");
    private static final URI DFS_URI = URI.create("https://account.dfs.core.windows.net/container/prefix/");
    private static final BatchSettings SETTINGS = new BatchSettings(100, 2048, 2);

    @Test
    void blobStorageHandsItsSettingsToTheReader() throws IOException {
        BlobServiceClient client = serviceClientMock();
        AzureBlobLocation location = AzureBlobLocation.parse(BLOB_URI);

        try (AzureBlobStorage storage =
                        new AzureBlobStorage(BLOB_URI, location, new BorrowedAzureHandle(client), SETTINGS);
                RangeReader reader = storage.openRangeReader("file.bin")) {
            assertThat(((AzureBlobRangeReader) reader).batchSettings()).isEqualTo(SETTINGS);
        }
    }

    @Test
    void dataLakeStorageHandsItsSettingsToTheReader() throws IOException {
        BlobServiceClient blobClient = serviceClientMock();
        DataLakeServiceClient dfsClient = mock(DataLakeServiceClient.class, RETURNS_DEEP_STUBS);
        AzureBlobLocation location = AzureBlobLocation.parse(DFS_URI);

        try (AzureDataLakeStorage storage = new AzureDataLakeStorage(
                        DFS_URI, location, new BorrowedAzureHandle(blobClient, dfsClient), SETTINGS);
                RangeReader reader = storage.openRangeReader("file.bin")) {
            assertThat(((AzureBlobRangeReader) reader).batchSettings()).isEqualTo(SETTINGS);
        }
    }

    @Test
    void borrowedClientStoragesUseTheObjectStoreDefaults() throws IOException {
        BlobServiceClient client = serviceClientMock();
        AzureBlobLocation location = AzureBlobLocation.parse(BLOB_URI);

        try (AzureBlobStorage storage = new AzureBlobStorage(BLOB_URI, location, new BorrowedAzureHandle(client));
                RangeReader reader = storage.openRangeReader("file.bin")) {
            assertThat(((AzureBlobRangeReader) reader).batchSettings()).isEqualTo(BatchSettings.objectStoreDefaults());
        }
    }

    @Test
    void anInvalidParameterFailsABlobStorageOpenWithoutKeepingAClient() {
        AzureClientCache clientCache = new AzureClientCache();
        AzureBlobStorageProvider provider = new AzureBlobStorageProvider(clientCache);
        StorageConfig config = sasTokenConfig(BLOB_URI).setParameter(BatchProviderHelper.BATCH_MAX_FETCH, 0);

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxFetchBytes");

        assertThat(clientCache.entryCount()).isZero();
    }

    @Test
    void anInvalidParameterFailsADataLakeStorageOpenWithoutKeepingAClient() {
        AzureClientCache clientCache = new AzureClientCache();
        AzureDataLakeStorageProvider provider = new AzureDataLakeStorageProvider(clientCache);
        StorageConfig config = sasTokenConfig(DFS_URI).setParameter(BatchProviderHelper.BATCH_MAX_FETCH, 0);

        assertThatThrownBy(() -> provider.createStorage(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxFetchBytes");

        assertThat(clientCache.entryCount()).isZero();
    }

    @Test
    void bothProvidersDeclareTheBatchParameters() {
        assertThat(new AzureBlobStorageProvider().getParameters()).containsAll(BatchProviderHelper.configParameters());
        assertThat(new AzureDataLakeStorageProvider().getParameters())
                .containsAll(BatchProviderHelper.configParameters());
    }

    /**
     * A reader builds its asynchronous client from the URL and pipeline of the blob client, and a mocked blob client
     * returns neither.
     */
    private static BlobServiceClient serviceClientMock() {
        BlobServiceClient client = mock(BlobServiceClient.class, RETURNS_DEEP_STUBS);
        BlobClient blobClient = new BlobClientBuilder()
                .endpoint("https://account.blob.core.windows.net")
                .containerName("container")
                .blobName("prefix/file.bin")
                .buildClient();
        when(client.getBlobContainerClient(anyString()).getBlobClient(anyString()))
                .thenReturn(blobClient);
        return client;
    }

    /** A SAS token builds the Blob and Data Lake clients without reaching the network. */
    private static StorageConfig sasTokenConfig(URI uri) {
        return new StorageConfig(uri).setParameter(AzureBlobStorageProvider.AZURE_SAS_TOKEN, "sv=2024-01-01&sig=test");
    }
}
