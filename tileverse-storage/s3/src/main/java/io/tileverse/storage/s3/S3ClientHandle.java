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
package io.tileverse.storage.s3;

import java.util.Optional;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Package-private indirection between {@link S3Storage} and the SDK clients it uses. Two implementations:
 *
 * <ul>
 *   <li>{@code LeasedS3Handle} (SPI path): wraps {@link S3ClientCache.Lease}; {@link #close()} releases the lease so
 *       refcounted SDK clients can shut down when the last reference drops.
 *   <li>{@code BorrowedS3Handle} (escape-hatch path, added in step 3): wraps a caller-supplied {@code S3ClientBundle};
 *       {@link #close()} is a no-op so the caller retains ownership of the SDK clients.
 * </ul>
 *
 * <p>The async client, transfer manager, and presigner are exposed as {@link Optional} because the borrowed-handle
 * variant may carry only a sync {@link S3Client}; in that case {@code read}, multipart upload, and presigned URL
 * operations throw {@link io.tileverse.storage.UnsupportedCapabilityException}.
 */
interface S3ClientHandle extends AutoCloseable {

    S3Client client();

    Optional<S3AsyncClient> asyncClient();

    Optional<S3TransferManager> transferManager();

    Optional<S3Presigner> presigner();

    @Override
    void close();
}
