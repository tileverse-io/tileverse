/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import com.google.cloud.storage.Storage;
import java.util.Objects;

/**
 * {@link GcsClientHandle} that wraps a caller-supplied {@link Storage} client. {@link #close()} is a no-op so the
 * caller retains ownership of the SDK client.
 */
record BorrowedGcsHandle(Storage client) implements GcsClientHandle {

    BorrowedGcsHandle {
        Objects.requireNonNull(client, "client");
    }

    @Override
    public void close() {
        // borrowed; caller owns the SDK client
    }
}
