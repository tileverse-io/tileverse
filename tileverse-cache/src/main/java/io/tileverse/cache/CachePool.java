/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import org.jspecify.annotations.NullMarked;

/** The two shared caches of the process, each with its own memory budget. */
@NullMarked
public enum CachePool {

    /** Holds the data read by callers: raw byte ranges and decoded vector tiles. */
    DATA("tileverse-cache-data"),

    /** Holds the indexes for finding data: PMTiles directories, VersaTiles tile indexes and Parquet footers. */
    METADATA("tileverse-cache-metadata");

    private final String cacheName;

    CachePool(String cacheName) {
        this.cacheName = cacheName;
    }

    /** Returns the name of the cache of this pool. */
    public String cacheName() {
        return cacheName;
    }
}
