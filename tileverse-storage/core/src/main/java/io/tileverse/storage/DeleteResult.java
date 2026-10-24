/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.util.Map;
import java.util.Set;

/**
 * Per-key outcome of {@link Storage#deleteAll(java.util.Collection)}, partitioned three ways:
 *
 * <ul>
 *   <li>{@code deleted} - keys that were actually removed by this call.
 *   <li>{@code didNotExist} - keys that were absent before the call. Populated only on backends whose bulk-delete API
 *       reports existence; see {@link StorageCapabilities#deleteReportsExistence()}. On backends that do not (notably
 *       S3), this set is always empty and missing keys land in {@code deleted} instead.
 *   <li>{@code failed} - keys that could not be deleted, with the underlying cause.
 * </ul>
 *
 * @param deleted keys that were actually removed by this call
 * @param didNotExist keys that were absent before the call; populated only when the backend reports existence
 * @param failed keys that could not be deleted, with their underlying cause
 */
public record DeleteResult(Set<String> deleted, Set<String> didNotExist, Map<String, StorageException> failed) {

    public DeleteResult {
        deleted = (deleted == null) ? Set.of() : Set.copyOf(deleted);
        didNotExist = (didNotExist == null) ? Set.of() : Set.copyOf(didNotExist);
        failed = (failed == null) ? Map.of() : Map.copyOf(failed);
    }

    public boolean isComplete() {
        return failed.isEmpty();
    }
}
