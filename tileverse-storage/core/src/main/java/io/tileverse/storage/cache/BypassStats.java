/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

/**
 * Counts the reads that were too large to cache. Like the cache statistics, the counts cover the
 * {@link CachingRangeReader}s built from one cache manager.
 *
 * @param calls the number of reads whose misses added up to more than the read-size limit of their reader
 * @param bytes the total size of those reads, in bytes
 */
public record BypassStats(long calls, long bytes) {}
