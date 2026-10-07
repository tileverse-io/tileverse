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
