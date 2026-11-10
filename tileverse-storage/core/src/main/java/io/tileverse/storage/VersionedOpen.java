/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;

/**
 * Opens a {@link RangeReader} from the entry returned by {@link Storage#stat(String)}. The entry gives the reader the
 * version of the object. If the stat finds no entry or fails, the reader opens without a version.
 */
@Slf4j
@NullMarked
final class VersionedOpen {

    private VersionedOpen() {}

    static RangeReader open(Storage storage, String key) {
        Optional<StorageEntry.File> entry = statOrEmpty(storage, key);
        if (entry.isPresent()) {
            return storage.openRangeReader(entry.get());
        }
        return storage.openRangeReader(key);
    }

    /**
     * Returns the entry of {@code key}, or empty when the stat fails. Some HTTP servers reject HEAD requests but still
     * serve range reads, hence a failed stat is a normal answer, logged at debug level only.
     */
    private static Optional<StorageEntry.File> statOrEmpty(Storage storage, String key) {
        try {
            return storage.stat(key);
        } catch (RuntimeException e) {
            log.debug("Opening {} without a version, its stat failed: {}", key, e.getMessage());
            return Optional.empty();
        }
    }
}
