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
