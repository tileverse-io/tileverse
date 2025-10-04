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
package io.tileverse.rangereader.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link CacheStats} and consistent cache statistics APIs.
 */
class CacheStatsTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private static final int FILE_SIZE = 10_000;
    private static final byte[] TEST_DATA = new byte[FILE_SIZE];

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test.bin");
        new Random(42).nextBytes(TEST_DATA);
        Files.write(testFile, TEST_DATA);
    }

    @Test
    void testCacheStatsConsistency() throws IOException {
        // Test both caching readers have consistent APIs
        FileRangeReader baseReader = FileRangeReader.builder().path(testFile).build();

        try (CachingRangeReader memoryCache =
                        CachingRangeReader.builder(baseReader).build();
                DiskCachingRangeReader diskCache = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(tempDir.resolve("cache"))
                        .build()) {

            // Both should have consistent method names
            assertEquals(0, memoryCache.getCacheEntryCount());
            assertEquals(0, diskCache.getCacheEntryCount());

            assertEquals(0, memoryCache.getEstimatedCacheSizeBytes());
            assertEquals(0, diskCache.getEstimatedCacheSizeBytes());

            ByteBuffer buff = ByteBuffer.allocate(1024);
            // Read some data to populate caches
            memoryCache.readRange(1000, 500, buff);
            diskCache.readRange(1000, 500, buff);

            // Both should report cache entries
            assertTrue(memoryCache.getCacheEntryCount() > 0);
            assertTrue(diskCache.getCacheEntryCount() > 0);

            assertTrue(memoryCache.getEstimatedCacheSizeBytes() > 0);
            assertTrue(diskCache.getEstimatedCacheSizeBytes() > 0);

            // Both should provide CacheStats
            CacheStats memoryStats = memoryCache.getCacheStats();
            CacheStats diskStats = diskCache.getCacheStats();

            // Verify CacheStats structure
            assertThat(memoryStats.entryCount()).isGreaterThan(0);
            assertThat(memoryStats.estimatedSizeBytes()).isGreaterThan(0);
            assertThat(memoryStats.loadCount()).isGreaterThan(0);
            assertThat(memoryStats.requestCount()).isGreaterThan(0);

            assertThat(diskStats.entryCount()).isGreaterThan(0);
            assertThat(diskStats.estimatedSizeBytes()).isGreaterThan(0);
            assertThat(diskStats.loadCount()).isGreaterThan(0);
            assertThat(diskStats.requestCount()).isGreaterThan(0);

            // Test toString format
            String memoryStatsStr = memoryStats.toString();
            String diskStatsStr = diskStats.toString();

            assertThat(memoryStatsStr).contains("CacheStats{");
            assertThat(memoryStatsStr).contains("entries=");
            assertThat(memoryStatsStr).contains("sizeBytes=");
            assertThat(memoryStatsStr).contains("hitRate=");

            assertThat(diskStatsStr).contains("CacheStats{");
            assertThat(diskStatsStr).contains("entries=");
            assertThat(diskStatsStr).contains("sizeBytes=");
            assertThat(diskStatsStr).contains("hitRate=");
        }
    }

    @Test
    void testCacheStatsFromCaffeine() throws IOException {
        // Test the factory method by using a real cache and getting its stats
        FileRangeReader baseReader = FileRangeReader.builder().path(testFile).build();

        try (CachingRangeReader reader = CachingRangeReader.builder(baseReader).build()) {
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
            assertThat(stats.estimatedSizeBytes()).isGreaterThan(0);
            assertThat(stats.requestCount()).isEqualTo(stats.hitCount() + stats.missCount());
        }
    }
}
