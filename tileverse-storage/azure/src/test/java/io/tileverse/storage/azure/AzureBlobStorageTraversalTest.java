/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static org.mockito.Mockito.mock;

import com.azure.storage.blob.BlobServiceClient;
import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;

/**
 * TCK-driven traversal-rejection tests for {@link AzureBlobStorage}. Validation rejects malicious keys before any SDK
 * call, so a mock BlobServiceClient suffices.
 */
class AzureBlobStorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        BlobServiceClient mockClient = mock(BlobServiceClient.class);
        return AzureBlobStorageProvider.open(
                URI.create("https://devstoreaccount1.blob.core.windows.net/test-container/prefix/"), mockClient);
    }
}
