/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import com.google.cloud.storage.Storage;

/**
 * Package-private indirection between {@link GoogleCloudStorage} and the GCS SDK {@link Storage} client. Two
 * implementations:
 *
 * <ul>
 *   <li>{@code LeasedGcsHandle} (SPI path): wraps {@link SdkStorageCache.Lease}; {@link #close()} releases the lease.
 *   <li>{@code BorrowedGcsHandle} (escape-hatch path, added in step 3): wraps a caller-supplied {@code Storage} client;
 *       {@link #close()} is a no-op.
 * </ul>
 */
interface GcsClientHandle extends AutoCloseable {

    Storage client();

    @Override
    void close();
}
