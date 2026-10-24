/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link CacheStats} and consistent cache statistics APIs. */
class CacheStatsTest {

    @TempDir
    private Path tempDir;

    private Path testFile;
    private static final int FILE_SIZE = 10_000;
    private static final byte[] TEST_DATA = new byte[FILE_SIZE];

    private CacheManager cacheManager;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test.bin");
        new Random(42).nextBytes(TEST_DATA);
        Files.write(testFile, TEST_DATA);
        cacheManager = CacheManager.newInstance();
    }

    @AfterEach
    void cleanUp() {
        cacheManager.invalidateAll();
    }

    @Test
    void testCacheStatsReflectCacheActivity() throws IOException {
        RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);

        try (CachingRangeReader memoryCache = CachingRangeReader.builder(baseReader)
                .cacheManager(cacheManager)
                .build()) {

            assertEquals(0, memoryCache.getCacheEntryCount());
            assertEquals(0, memoryCache.getEstimatedCacheSizeBytes());

            ByteBuffer buff = ByteBuffer.allocate(1024);
            // Read some data to populate the cache
            memoryCache.readRange(1000, 500, buff);

            assertTrue(memoryCache.getCacheEntryCount() > 0);
            assertTrue(memoryCache.getEstimatedCacheSizeBytes() > 0);

            CacheStats memoryStats = memoryCache.getCacheStats();

            assertThat(memoryStats.entryCount()).isGreaterThan(0);
            assertThat(memoryStats.loadCount()).isGreaterThan(0);
            assertThat(memoryStats.requestCount()).isGreaterThan(0);
        }
    }

    @Test
    void testCacheStatsFromCaffeine() throws IOException {
        // Test the factory method by using a real cache and getting its stats
        RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);

        try (CachingRangeReader reader = CachingRangeReader.builder(baseReader)
                .cacheManager(cacheManager)
                .build()) {
            // Generate some cache activity
            ByteBuffer buff = ByteBuffer.allocate(2048);
            reader.readRange(1000, 500, buff); // miss
            reader.readRange(1000, 500, buff); // hit
            reader.readRange(2000, 300, buff); // miss

            CacheStats stats = reader.getCacheStats();

            // Verify the stats are properly constructed
            assertThat(stats.hitCount()).isGreaterThan(0);
            assertThat(stats.missCount()).isGreaterThan(0);
            assertThat(stats.loadCount()).isGreaterThan(0);
            assertThat(stats.entryCount()).isGreaterThan(0);
            assertThat(stats.requestCount()).isEqualTo(stats.hitCount() + stats.missCount());
        }
    }
}
