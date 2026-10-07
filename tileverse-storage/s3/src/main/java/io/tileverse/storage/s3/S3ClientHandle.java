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
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * Package-private indirection between {@link S3Storage} and its SDK objects. Two implementations:
 *
 * <ul>
 *   <li>{@code LeasedS3Handle} (SPI path): wraps an {@link S3ClientCache.Lease}; {@link #close()} releases the lease,
 *       and the client set closes with its last lease.
 *   <li>{@code BorrowedS3Handle}: wraps an {@link S3AsyncClient} passed by the caller; {@link #close()} closes the
 *       transfer manager built by the handle and leaves the client open.
 * </ul>
 */
interface S3ClientHandle extends AutoCloseable {

    /** The client of the Storage and reader operations. */
    S3AsyncClient client();

    /** The transfer manager of multipart uploads, built on the first call. */
    S3TransferManager transferManager();

    /** Whether {@link #presigner()} returns a presigner. Answering builds nothing. */
    boolean presigns();

    /** The presigner; empty for a caller-supplied client. */
    Optional<S3Presigner> presigner();

    @Override
    void close();
}
