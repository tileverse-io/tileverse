/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import com.google.cloud.storage.Storage;

/**
 * {@link GcsClientHandle} that wraps an {@link SdkStorageCache.Lease}. The lease is released on close, decrementing the
 * cache refcount.
 */
final class LeasedGcsHandle implements GcsClientHandle {

    private final SdkStorageCache.Lease lease;

    LeasedGcsHandle(SdkStorageCache.Lease lease) {
        this.lease = lease;
    }

    @Override
    public Storage client() {
        return lease.client();
    }

    @Override
    public void close() {
        lease.close();
    }
}
