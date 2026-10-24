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
 * Package-private indirection between {@link AzureBlobStorage} / {@link AzureDataLakeStorage} and the SDK clients they
 * use. Two implementations:
 *
 * <ul>
 *   <li>{@code LeasedAzureHandle} (SPI path): wraps {@link AzureClientCache.Lease}; {@link #close()} releases the
 *       lease.
 *   <li>{@code BorrowedAzureHandle} (escape-hatch path, added in step 3): wraps a caller-supplied
 *       {@code BlobServiceClient} (and optionally a {@code DataLakeServiceClient}); {@link #close()} is a no-op.
 * </ul>
 *
 * <p>{@link #blobServiceClient()} is always present. {@link #dataLakeServiceClient()} is an {@link Optional} because
 * the borrowed-handle variant for {@code AzureBlobStorage} may carry only the blob client; {@code AzureDataLakeStorage}
 * unwraps it via {@code .orElseThrow(...)} since it always requires the DFS endpoint.
 */
interface AzureClientHandle extends AutoCloseable {

    BlobServiceClient blobServiceClient();

    Optional<DataLakeServiceClient> dataLakeServiceClient();

    @Override
    void close();
}
