/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static org.mockito.Mockito.mock;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;

/**
 * TCK-driven traversal-rejection tests for {@link AzureDataLakeStorage}. Validation rejects malicious keys before any
 * SDK call, so mock clients suffice.
 */
class AzureDataLakeStorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        DataLakeServiceClient dfsMock = mock(DataLakeServiceClient.class);
        BlobServiceClient blobMock = mock(BlobServiceClient.class);
        return AzureDataLakeStorageProvider.open(
                URI.create("https://devstoreaccount1.dfs.core.windows.net/test-container/prefix/"), dfsMock, blobMock);
    }
}
