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
 * {@link S3ClientHandle} over an {@link S3ClientCache.Lease}: the client of a cached client set, and its transfer
 * manager and presigner, built on first use. Closing releases the lease.
 */
final class LeasedS3Handle implements S3ClientHandle {

    private final S3ClientCache.Lease lease;

    LeasedS3Handle(S3ClientCache.Lease lease) {
        this.lease = lease;
    }

    @Override
    public S3AsyncClient client() {
        return lease.client();
    }

    @Override
    public S3TransferManager transferManager() {
        return lease.transferManager();
    }

    @Override
    public boolean presigns() {
        return true;
    }

    @Override
    public Optional<S3Presigner> presigner() {
        return Optional.of(lease.presigner());
    }

    @Override
    public void close() {
        lease.close();
    }
}
