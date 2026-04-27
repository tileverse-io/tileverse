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
 * {@link S3ClientHandle} that wraps an {@link S3ClientCache.Lease}. Exposes the four cached SDK objects (sync, async,
 * transfer manager, presigner) and releases the lease on close, which decrements the cache refcount.
 */
final class LeasedS3Handle implements S3ClientHandle {

    private final S3ClientCache.Lease lease;

    LeasedS3Handle(S3ClientCache.Lease lease) {
        this.lease = lease;
    }

    @Override
    public S3Client client() {
        return lease.client();
    }

    @Override
    public Optional<S3AsyncClient> asyncClient() {
        return Optional.of(lease.asyncClient());
    }

    @Override
    public Optional<S3TransferManager> transferManager() {
        return Optional.of(lease.transferManager());
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
