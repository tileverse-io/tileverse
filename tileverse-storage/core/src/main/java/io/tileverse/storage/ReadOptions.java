/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Options for {@link Storage#read(String, ReadOptions)}.
 *
 * @param offset byte offset to start reading from (0 = start of object)
 * @param length number of bytes to read, at least one; empty means read to EOF. Use {@link Storage#stat(String)} for
 *     the metadata alone
 * @param ifMatchEtag conditional read: only return content when the object's ETag matches
 * @param versionId read a specific prior object version; backend errors if versioning is disabled. Empty means latest
 *     version
 * @param ifModifiedSince conditional read: only return content modified since this instant
 */
public record ReadOptions(
        long offset,
        OptionalLong length,
        Optional<String> ifMatchEtag,
        Optional<String> versionId,
        Optional<Instant> ifModifiedSince) {

    public ReadOptions {
        if (offset < 0L) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (length.isPresent() && length.getAsLong() <= 0L) {
            throw new IllegalArgumentException("length must be positive");
        }
    }

    public static ReadOptions defaults() {
        return new ReadOptions(0L, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ReadOptions fromOffset(long offset) {
        return new ReadOptions(offset, OptionalLong.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ReadOptions range(long offset, long length) {
        return new ReadOptions(offset, OptionalLong.of(length), Optional.empty(), Optional.empty(), Optional.empty());
    }
}
