/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
