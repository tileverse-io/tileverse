/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.cache;

/**
 * Cache statistics record that provides consistent cache metrics across different caching implementations.
 * <p>
 * This record abstracts away the underlying caching implementation details and provides a unified
 * interface for cache statistics, whether from memory-based or disk-based caching.
 *
 * @param hitCount The number of cache hits
 * @param missCount The number of cache misses
 * @param loadCount The total number of times the cache attempted to load new values
 * @param evictionCount The number of cache entries that have been evicted
 * @param evictionWeight The sum of weights of evicted entries. This total does not include manual invalidations.
 * @param entryCount The current number of entries in the cache
 * @param hitRate The cache hit rate (hitCount / requestCount), between 0.0 and 1.0
 * @param missRate The cache miss rate (missCount / requestCount), between 0.0 and 1.0
 * @param averageLoadTime The average time spent loading new values, in nanoseconds
 */
public record CacheStats(
        long hitCount,
        long missCount,
        long loadCount,
        long evictionCount,
        long evictionWeight,
        long entryCount,
        double hitRate,
        double missRate,
        double averageLoadTime) {

    /**
     * Creates CacheStats from Caffeine CacheStats.
     *
     * @param caffeineStats The Caffeine cache statistics
     * @param entryCount The number of entries in the cache
     * @return A new CacheStats instance
     */
    public static CacheStats fromCaffeine(
            com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats, long entryCount) {
        return new CacheStats(
                caffeineStats.hitCount(),
                caffeineStats.missCount(),
                caffeineStats.loadCount(),
                caffeineStats.evictionCount(),
                caffeineStats.evictionWeight(),
                entryCount,
                caffeineStats.hitRate(),
                caffeineStats.missRate(),
                caffeineStats.averageLoadPenalty());
    }

    /**
     * Returns the total number of cache requests (hits + misses).
     *
     * @return The total request count
     */
    public long requestCount() {
        return hitCount + missCount;
    }

    //    /**
    //     * Returns a formatted string representation of the cache statistics.
    //     *
    //     * @return A human-readable string with key cache metrics
    //     */
    //    @Override
    //    public String toString() {
    //        return String.format(
    //                "CacheStats{entries=%d, hitRate=%.2f%%, hits=%d, misses=%d, evictions=%d}",
    //                entryCount, hitRate * 100.0, hitCount, missCount, evictionCount);
    //    }
}
