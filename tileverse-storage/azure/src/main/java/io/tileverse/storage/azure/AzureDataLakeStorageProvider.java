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
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.List;

/**
 * StorageProvider for Azure Data Lake Storage Gen2 (HNS-enabled accounts). Handles {@code abfs://}, {@code abfss://},
 * and {@code https://*.dfs.core.windows.net/...} URIs.
 */
public class AzureDataLakeStorageProvider extends AbstractStorageProvider {

    public static final String ENABLED_KEY = "IO_TILEVERSE_STORAGE_AZURE_DATALAKE";
    public static final String ID = "azure-datalake";

    private final AzureClientCache clientCache = new AzureClientCache();

    public AzureDataLakeStorageProvider() {
        super(true);
    }

    /**
     * Opens a {@link io.tileverse.storage.Storage} backed by the supplied
     * {@link com.azure.storage.file.datalake.DataLakeServiceClient} and
     * {@link com.azure.storage.blob.BlobServiceClient}, bypassing the SPI configuration path. Both clients are
     * required: the DFS client drives directory and rename operations on HNS-enabled accounts, and the blob client
     * drives byte-range and streaming reads via the dual-API surface.
     *
     * <p>The returned {@code Storage} <b>borrows</b> the supplied clients; closing the {@code Storage} does NOT close
     * them. The caller retains ownership.
     *
     * @param uri canonical DataLake URI (e.g. {@code abfss://container@account.dfs.core.windows.net/[prefix/]} or
     *     {@code https://account.dfs.core.windows.net/container/[prefix/]})
     * @param dataLakeServiceClient a pre-configured DFS service client; not closed by the returned {@code Storage}
     * @param blobServiceClient a pre-configured Blob service client targeting the same account; not closed by the
     *     returned {@code Storage}
     * @return a borrowed-client {@code AzureDataLakeStorage}
     */
    public static Storage open(
            URI uri,
            com.azure.storage.file.datalake.DataLakeServiceClient dataLakeServiceClient,
            com.azure.storage.blob.BlobServiceClient blobServiceClient) {
        if (uri == null) throw new IllegalArgumentException("uri");
        if (dataLakeServiceClient == null) throw new IllegalArgumentException("dataLakeServiceClient");
        if (blobServiceClient == null) throw new IllegalArgumentException("blobServiceClient");
        AzureBlobLocation location = AzureBlobLocation.parse(uri);
        return new AzureDataLakeStorage(
                uri, location, new BorrowedAzureHandle(blobServiceClient, dataLakeServiceClient));
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return StorageProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "Azure Data Lake Storage Gen2 (HNS) provider.";
    }

    @Override
    public boolean canProcess(StorageConfig config) {
        if (!matches(config, "abfs", "abfss", "https", "http")) {
            return false;
        }
        URI uri = config.baseUri();
        if (uri == null) return false;
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
        if (scheme.equals("abfs") || scheme.equals("abfss")) {
            return uri.getHost() != null && uri.getHost().contains(".dfs.core.");
        }
        return uri.getHost() != null && uri.getHost().contains(".dfs.core.");
    }

    @Override
    protected List<StorageParameter<?>> buildParameters() {
        // DataLake shares the Azure parameter set with the Blob backend (account-key, sas-token,
        // connection-string, retry tuning). Listing them here makes them discoverable through this provider too.
        return List.of(
                AzureBlobStorageProvider.AZURE_ACCOUNT_KEY,
                AzureBlobStorageProvider.AZURE_SAS_TOKEN,
                AzureBlobStorageProvider.AZURE_CONNECTION_STRING,
                AzureBlobStorageProvider.AZURE_ANONYMOUS,
                AzureBlobStorageProvider.AZURE_MAX_RETRIES,
                AzureBlobStorageProvider.AZURE_RETRY_DELAY,
                AzureBlobStorageProvider.AZURE_MAX_RETRY_DELAY,
                AzureBlobStorageProvider.AZURE_TRY_TIMEOUT);
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.baseUri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.baseUri() is required for AzureDataLakeStorage");
        }
        AzureBlobLocation location = AzureBlobLocation.parse(uri);
        AzureClientCache.Lease lease = clientCache.acquire(AzureBlobStorageProvider.keyFor(config, location));
        return new AzureDataLakeStorage(uri, location, lease);
    }
}
