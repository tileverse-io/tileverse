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
