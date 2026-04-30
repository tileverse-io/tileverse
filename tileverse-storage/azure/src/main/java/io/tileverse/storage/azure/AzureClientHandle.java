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
