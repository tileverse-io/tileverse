/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.util.Optional;

/**
 * {@link AzureClientHandle} that wraps an {@link AzureClientCache.Lease}. Both the blob and DFS service clients are
 * always present; the lease is released on close.
 */
final class LeasedAzureHandle implements AzureClientHandle {

    private final AzureClientCache.Lease lease;

    LeasedAzureHandle(AzureClientCache.Lease lease) {
        this.lease = lease;
    }

    @Override
    public BlobServiceClient blobServiceClient() {
        return lease.blobServiceClient();
    }

    @Override
    public Optional<DataLakeServiceClient> dataLakeServiceClient() {
        return Optional.of(lease.dataLakeServiceClient());
    }

    @Override
    public void close() {
        lease.close();
    }
}
