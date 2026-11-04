/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static io.tileverse.cache.CacheSettings.DATA_MAX_SIZE;
import static io.tileverse.cache.CacheSettings.DEFAULT_DATA_MAX_SIZE;
import static io.tileverse.cache.CacheSettings.DEFAULT_METADATA_MAX_SIZE;
import static io.tileverse.cache.CacheSettings.METADATA_MAX_SIZE;

import org.jspecify.annotations.NullMarked;

/**
 * The memory budget of each {@link CachePool}, in bytes.
 *
 * @param dataBytes the budget of the {@link CachePool#DATA} pool
 * @param metadataBytes the budget of the {@link CachePool#METADATA} pool
 */
@NullMarked
public record CacheBudgets(long dataBytes, long metadataBytes) {

    /** @throws IllegalArgumentException if a budget is negative */
    public CacheBudgets {
        requireNonNegative(dataBytes, "dataBytes");
        requireNonNegative(metadataBytes, "metadataBytes");
    }

    private static void requireNonNegative(long budget, String name) {
        if (budget < 0) {
            throw new IllegalArgumentException(name + " must not be negative: " + budget);
        }
    }

    /**
     * Reads the budgets from the system properties and environment variables of the process.
     *
     * @throws IllegalArgumentException if a setting holds an invalid size
     */
    public static CacheBudgets fromSettings() {
        return fromSettings(CacheSettings.ofProcess());
    }

    static CacheBudgets fromSettings(CacheSettings settings) {
        long dataBytes = settings.maxSize(DATA_MAX_SIZE, DEFAULT_DATA_MAX_SIZE);
        long metadataBytes = settings.maxSize(METADATA_MAX_SIZE, DEFAULT_METADATA_MAX_SIZE);
        return new CacheBudgets(dataBytes, metadataBytes);
    }

    /** Returns the budget of the given pool, in bytes. */
    public long of(CachePool pool) {
        return switch (pool) {
            case DATA -> dataBytes;
            case METADATA -> metadataBytes;
        };
    }
}
