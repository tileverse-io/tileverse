/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link AzureClientHandle} that wraps a caller-supplied {@link BlobServiceClient} (and optionally a
 * {@link DataLakeServiceClient}). {@link #close()} is a no-op so the caller retains ownership of the SDK clients.
 */
final class BorrowedAzureHandle implements AzureClientHandle {

    private final BlobServiceClient blobServiceClient;
    private final Optional<DataLakeServiceClient> dataLakeServiceClient;

    BorrowedAzureHandle(BlobServiceClient blobServiceClient) {
        this(blobServiceClient, Optional.empty());
    }

    BorrowedAzureHandle(BlobServiceClient blobServiceClient, DataLakeServiceClient dataLakeServiceClient) {
        this(blobServiceClient, Optional.of(Objects.requireNonNull(dataLakeServiceClient, "dataLakeServiceClient")));
    }

    private BorrowedAzureHandle(
            BlobServiceClient blobServiceClient, Optional<DataLakeServiceClient> dataLakeServiceClient) {
        this.blobServiceClient = Objects.requireNonNull(blobServiceClient, "blobServiceClient");
        this.dataLakeServiceClient = dataLakeServiceClient;
    }

    @Override
    public BlobServiceClient blobServiceClient() {
        return blobServiceClient;
    }

    @Override
    public Optional<DataLakeServiceClient> dataLakeServiceClient() {
        return dataLakeServiceClient;
    }

    @Override
    public void close() {
        // borrowed; caller owns the SDK clients
    }
}
