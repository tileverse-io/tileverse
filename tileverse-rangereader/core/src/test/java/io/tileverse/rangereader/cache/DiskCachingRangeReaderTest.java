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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DiskCachingRangeReader}.
 */
@Disabled("random failures in github actions, revisit")
class DiskCachingRangeReaderTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private String sourceId;

    private Path cacheDir;
    private static final int FILE_SIZE = 100_000;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file
        testFile = tempDir.resolve("test.bin");
        sourceId = testFile.toString();
        byte[] testData = new byte[FILE_SIZE];
        new Random(42).nextBytes(testData); // Use fixed seed for reproducibility
        Files.write(testFile, testData);

        // Create a cache directory
        cacheDir = tempDir.resolve("cache");
        Files.createDirectories(cacheDir);
    }

    @AfterEach
    void tearDown() {
        // Cleanup is handled by TempDir annotation
    }

    /**
     * Helper method to count cache files recursively in the cache directory.
     * This accounts for the new subdirectory structure where cache files are stored
     * in subdirectories based on source hash.
     */
    private long countCacheFiles(Path cacheDir) throws IOException {
        if (!Files.exists(cacheDir)) {
            return 0;
        }
        try (Stream<Path> paths = Files.walk(cacheDir)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    /**
     * Helper method to get all cache file paths recursively.
     */
    private Path[] getAllCacheFiles(Path cacheDir) throws IOException {
        if (!Files.exists(cacheDir)) {
            return new Path[0];
        }
        try (Stream<Path> paths = Files.walk(cacheDir)) {
            return paths.filter(Files::isRegularFile).toArray(Path[]::new);
        }
    }

    @Test
    void testBasicCaching() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Read a range
            ByteBuffer data1 = cachingReader.readRange(1000, 500);
            assertEquals(500, data1.remaining(), "Should read 500 bytes");

            // Check cache stats
            assertEquals(1, cachingReader.getCacheEntryCount(), "Should have 1 cache entry");
            assertEquals(500, cachingReader.getEstimatedCacheSizeBytes(), "Cache size should be 500 bytes");

            // Read the same range again (should come from cache)
            ByteBuffer data2 = cachingReader.readRange(1000, 500);
            assertEquals(500, data2.remaining(), "Should read 500 bytes again");

            // Verify the data is the same
            data1.rewind();
            byte[] bytes1 = new byte[data1.remaining()];
            data1.get(bytes1);

            data2.rewind();
            byte[] bytes2 = new byte[data2.remaining()];
            data2.get(bytes2);

            assertTrue(Arrays.equals(bytes1, bytes2), "Data should be identical");

            // Read a different range
            ByteBuffer data3 = cachingReader.readRange(2000, 300);
            assertEquals(300, data3.remaining(), "Should read 300 bytes");

            // Cache should now have 2 entries
            assertEquals(2, cachingReader.getCacheEntryCount(), "Should have 2 cache entries");
            assertEquals(800, cachingReader.getEstimatedCacheSizeBytes(), "Cache size should be 800 bytes");
        }

        // Verify cache files persist after reader is closed
        assertTrue(countCacheFiles(cacheDir) > 0, "Cache directory should contain files");

        // Create a new reader to test using existing cache
        try (RangeReader baseReader2 = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader2 = DiskCachingRangeReader.builder(baseReader2)
                        .cacheDirectory(cacheDir)
                        .build()) {

            // Should initialize with existing cache entries
            assertEquals(2, cachingReader2.getCacheEntryCount(), "Should have 2 cache entries from before");

            // Read from existing cache
            ByteBuffer data = cachingReader2.readRange(1000, 500);
            assertEquals(500, data.remaining(), "Should read 500 bytes from cache");
        }
    }

    @Test
    void testCacheEviction() throws IOException {
        // Create cache with small max size
        final long maxCacheSize = 1000; // 1KB

        try (RangeReader baseReader = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(cacheDir)
                        .maxCacheSizeBytes(maxCacheSize)
                        .build()) {

            // Fill cache with entries
            cachingReader.readRange(0, 400); // Entry 1: 400 bytes
            cachingReader.readRange(1000, 400); // Entry 2: 400 bytes

            assertEquals(2, cachingReader.getCacheEntryCount(), "Should have 2 cache entries");
            assertEquals(800, cachingReader.getEstimatedCacheSizeBytes(), "Cache size should be 800 bytes");

            // Add another entry that will cause eviction
            cachingReader.readRange(2000, 600); // Entry 3: 600 bytes

            // First entry should be evicted
            await().atMost(2, SECONDS)
                    .untilAsserted(() -> assertTrue(
                            cachingReader.getEstimatedCacheSizeBytes() <= maxCacheSize,
                            "Cache size should not exceed max size"));

            assertEquals(2, cachingReader.getCacheEntryCount(), "Should have 2 cache entries after eviction");
        }
    }

    @Test
    void testClearCache() throws IOException {
        try (RangeReader baseReader = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(cacheDir)
                        .build()) {

            // Add some entries to cache
            cachingReader.readRange(0, 100);
            cachingReader.readRange(1000, 200);
            cachingReader.readRange(2000, 300);

            assertEquals(3, cachingReader.getCacheEntryCount(), "Should have 3 cache entries");

            // Clear the cache
            cachingReader.clearCache();

            assertEquals(0, cachingReader.getCacheEntryCount(), "Should have 0 cache entries after clearing");
            await().atMost(2, SECONDS).untilAsserted(() -> assertThat(cachingReader.getEstimatedCacheSizeBytes())
                    .isZero());

            // Verify no files in cache directory
            await().atMost(2, SECONDS)
                    .untilAsserted(() -> assertEquals(0, countCacheFiles(cacheDir), "Cache directory should be empty"));
        }
    }

    @Test
    void testMultipleReaders() throws IOException {
        // Create separate cache directories for each reader
        Path cacheDir1 = tempDir.resolve("cache1");
        Path cacheDir2 = tempDir.resolve("cache2");
        Files.createDirectories(cacheDir1);
        Files.createDirectories(cacheDir2);

        try (RangeReader baseReader = FileRangeReader.of(testFile)) {
            // Create two caching readers with different cache directories
            try (RangeReader baseReader2 = FileRangeReader.of(testFile);
                    DiskCachingRangeReader reader1 = DiskCachingRangeReader.builder(baseReader)
                            .cacheDirectory(cacheDir1)
                            .build();
                    DiskCachingRangeReader reader2 = DiskCachingRangeReader.builder(baseReader2)
                            .cacheDirectory(cacheDir2)
                            .build()) {

                // Add entries to both caches
                reader1.readRange(0, 100);
                reader2.readRange(0, 100);

                assertEquals(1, reader1.getCacheEntryCount(), "Reader 1 should have 1 entry");
                assertEquals(1, reader2.getCacheEntryCount(), "Reader 2 should have 1 entry");

                // Each cache directory should have 1 file
                assertEquals(1, countCacheFiles(cacheDir1), "Cache directory 1 should have 1 file");
                assertEquals(1, countCacheFiles(cacheDir2), "Cache directory 2 should have 1 file");
            }
        }
    }

    @Test
    void testMultipleReadersWithSharedCache() throws IOException {
        // Test that multiple DiskCachingRangeReader instances for the same source
        // can share the same cache directory and cache files correctly.
        // Note: Each instance maintains its own cache view, so cache entry counts may differ
        try (RangeReader baseReader1 = FileRangeReader.of(testFile);
                RangeReader baseReader2 = FileRangeReader.of(testFile);
                DiskCachingRangeReader reader1 = DiskCachingRangeReader.builder(baseReader1)
                        .cacheDirectory(cacheDir)
                        .build();
                DiskCachingRangeReader reader2 = DiskCachingRangeReader.builder(baseReader2)
                        .cacheDirectory(cacheDir)
                        .build()) {

            // Verify they have the same source identifier
            assertEquals(
                    reader1.getSourceIdentifier(),
                    reader2.getSourceIdentifier(),
                    "Both readers should have the same source identifier");

            // Reader 1 reads a range and caches it
            ByteBuffer data1 = reader1.readRange(1000, 500);
            assertEquals(500, data1.remaining(), "Should read 500 bytes");
            assertEquals(1, reader1.getCacheEntryCount(), "Reader 1 should have 1 cache entry");
            assertEquals(1, countCacheFiles(cacheDir), "Should have 1 cache file");

            // Reader 2 reads the same range - should find it on disk and add to its own cache
            ByteBuffer data2 = reader2.readRange(1000, 500);
            assertEquals(500, data2.remaining(), "Should read 500 bytes");
            assertEquals(1, countCacheFiles(cacheDir), "Should still have 1 cache file (shared)");

            // Verify the data is identical
            data1.rewind();
            data2.rewind();
            byte[] bytes1 = new byte[data1.remaining()];
            byte[] bytes2 = new byte[data2.remaining()];
            data1.get(bytes1);
            data2.get(bytes2);
            assertArrayEquals(bytes1, bytes2, "Data from both readers should be identical");

            // Reader 2 reads a different range
            ByteBuffer data3 = reader2.readRange(2000, 300);
            assertEquals(300, data3.remaining(), "Should read 300 bytes");
            assertEquals(2, countCacheFiles(cacheDir), "Should now have 2 cache files");

            // Reader 1 reads the range that reader 2 cached - should find it on disk
            ByteBuffer data4 = reader1.readRange(2000, 300);
            assertEquals(300, data4.remaining(), "Should read 300 bytes");
            assertEquals(2, countCacheFiles(cacheDir), "Should still have 2 cache files");

            // Verify the data is identical
            data3.rewind();
            data4.rewind();
            byte[] bytes3 = new byte[data3.remaining()];
            byte[] bytes4 = new byte[data4.remaining()];
            data3.get(bytes3);
            data4.get(bytes4);
            assertArrayEquals(bytes3, bytes4, "Data from both readers should be identical for second range");

            // Both readers should see both cache files on disk now
            assertTrue(reader1.getCacheEntryCount() >= 1, "Reader 1 should have at least 1 cache entry");
            assertTrue(reader2.getCacheEntryCount() >= 1, "Reader 2 should have at least 1 cache entry");
        }
    }

    @Test
    void testRangeReaderBuilderWithDiskCache() throws IOException {
        // Test using the builder pattern with disk caching
        try (RangeReader reader = DiskCachingRangeReader.builder(
                        FileRangeReader.builder().path(testFile).build())
                .cacheDirectory(cacheDir)
                .build()) {

            // Read some data
            ByteBuffer data1 = reader.readRange(1000, 200);
            assertEquals(200, data1.remaining(), "Should read 200 bytes");

            // Read again (should come from memory cache)
            ByteBuffer data2 = reader.readRange(1000, 200);
            assertEquals(200, data2.remaining(), "Should read 200 bytes again");

            // Verify cache files were created
            assertTrue(countCacheFiles(cacheDir) > 0, "Cache directory should contain files");
        }
    }

    @Test
    void testBuilderWithDefaultCacheDirectory() throws IOException {
        // Test using the builder pattern with default cache directory
        DiskCachingRangeReader reader = DiskCachingRangeReader.builder(
                        FileRangeReader.builder().path(testFile).build())
                .build();

        try {
            // Read some data
            ByteBuffer data1 = reader.readRange(1000, 200);
            assertEquals(200, data1.remaining(), "Should read 200 bytes");

            // Read again (should come from cache)
            ByteBuffer data2 = reader.readRange(1000, 200);
            assertEquals(200, data2.remaining(), "Should read 200 bytes again");

            // Verify the data is the same
            data1.rewind();
            data2.rewind();
            assertEquals(data1, data2, "Data should be identical");

            // Verify the cache directory is the expected default
            // We need to check that cache files are created in the expected location
            String expectedTmpDir = System.getProperty("java.io.tmpdir");
            Path expectedCacheRoot = Paths.get(expectedTmpDir, "tileverse-rangereader-cache");

            // Check that cache files exist under the expected directory structure
            assertTrue(
                    Files.exists(expectedCacheRoot), "Default cache directory should exist at: " + expectedCacheRoot);

            // Check that there are cache files in subdirectories under the expected root
            try (Stream<Path> paths = Files.walk(expectedCacheRoot)) {
                long cacheFileCount = paths.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".range"))
                        .count();
                assertTrue(
                        cacheFileCount > 0, "Should have cache files in default cache directory: " + expectedCacheRoot);
            }
        } finally {
            reader.close();
        }
    }

    @Test
    void testBasicCachingWithExplicitBuffer() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Read a range with explicit buffer
            ByteBuffer buffer1 = ByteBuffer.allocate(500);
            int bytesRead1 = cachingReader.readRange(1000, 500, buffer1);

            // Verify returned byte count
            assertEquals(500, bytesRead1, "Should read 500 bytes");

            // Verify buffer is ready for reading
            assertEquals(0, buffer1.position());
            assertEquals(500, buffer1.limit());
            assertEquals(500, buffer1.remaining());

            // Check cache stats
            assertEquals(1, cachingReader.getCacheEntryCount(), "Should have 1 cache entry");
            assertEquals(500, cachingReader.getEstimatedCacheSizeBytes(), "Cache size should be 500 bytes");

            // Read the same range again with a different buffer (should come from cache)
            ByteBuffer buffer2 = ByteBuffer.allocate(600); // Larger than needed
            int bytesRead2 = cachingReader.readRange(1000, 500, buffer2);

            // Verify returned byte count
            assertEquals(500, bytesRead2, "Should read 500 bytes again");

            // Verify buffer is ready for reading
            assertEquals(0, buffer2.position());
            assertEquals(500, buffer2.limit());
            assertEquals(500, buffer2.remaining());

            // Verify the data is the same
            byte[] bytes1 = new byte[buffer1.remaining()];
            buffer1.get(bytes1);

            byte[] bytes2 = new byte[buffer2.remaining()];
            buffer2.get(bytes2);

            assertTrue(Arrays.equals(bytes1, bytes2), "Data should be identical");

            // Read a different range with explicit buffer
            ByteBuffer buffer3 = ByteBuffer.allocate(300);
            int bytesRead3 = cachingReader.readRange(2000, 300, buffer3);

            // Verify returned byte count
            assertEquals(300, bytesRead3, "Should read 300 bytes");

            // Verify buffer is ready for reading
            assertEquals(0, buffer3.position());
            assertEquals(300, buffer3.limit());
            assertEquals(300, buffer3.remaining());

            // Cache should now have 2 entries
            assertEquals(2, cachingReader.getCacheEntryCount(), "Should have 2 cache entries");
            assertEquals(800, cachingReader.getEstimatedCacheSizeBytes(), "Cache size should be 800 bytes");
        }

        // Verify cache files persist after reader is closed
        assertTrue(countCacheFiles(cacheDir) > 0, "Cache directory should contain files");

        // Create a new reader to test using existing cache with explicit buffer
        try (RangeReader baseReader2 = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader2 = DiskCachingRangeReader.builder(baseReader2)
                        .cacheDirectory(cacheDir)
                        .build()) {

            // Should initialize with existing cache entries
            assertEquals(2, cachingReader2.getCacheEntryCount(), "Should have 2 cache entries from before");

            // Read from existing cache with explicit buffer
            ByteBuffer buffer = ByteBuffer.allocate(500);
            int bytesRead = cachingReader2.readRange(1000, 500, buffer);

            // Verify returned byte count
            assertEquals(500, bytesRead, "Should read 500 bytes from cache");

            // Verify buffer is ready for reading
            assertEquals(0, buffer.position());
            assertEquals(500, buffer.limit());
            assertEquals(500, buffer.remaining());
        }
    }

    @Test
    void testReadBeyondEndWithExplicitBuffer() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Try to read beyond the end of the file
            ByteBuffer buffer = ByteBuffer.allocate(200);
            int bytesRead = cachingReader.readRange(FILE_SIZE - 100, 200, buffer);

            // Verify returned byte count (should be truncated)
            assertEquals(100, bytesRead, "Should read only 100 bytes");

            // Verify buffer is ready for reading with correct limit
            assertEquals(0, buffer.position());
            assertEquals(100, buffer.limit());
            assertEquals(100, buffer.remaining());

            // Verify reading from offset beyond EOF
            ByteBuffer buffer2 = ByteBuffer.allocate(100);
            int bytesRead2 = cachingReader.readRange(FILE_SIZE + 100, 100, buffer2);

            // Should return 0 bytes read
            assertEquals(0, bytesRead2, "Should read 0 bytes");

            // Buffer should be flipped but with 0 bytes
            assertEquals(0, buffer2.position());
            assertEquals(0, buffer2.limit());
            assertEquals(0, buffer2.remaining());
        }
    }

    @Test
    void testReadWithOffsetInBuffer() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Create a buffer with an initial position
            ByteBuffer buffer = ByteBuffer.allocate(600);
            buffer.position(100); // Start at position 100

            // Read data into buffer at current position
            int bytesRead = cachingReader.readRange(1000, 400, buffer);

            // Verify returned byte count
            assertEquals(400, bytesRead, "Should read 400 bytes");

            // Verify buffer position and limit for reading
            assertEquals(100, buffer.position(), "Position should be at original position");
            assertEquals(500, buffer.limit(), "Limit should be original position + bytes read");
            assertEquals(400, buffer.remaining(), "Should have 400 bytes remaining");

            // Read same range again (should come from cache)
            buffer.clear(); // Reset buffer
            buffer.position(200); // Use a different position

            int bytesRead2 = cachingReader.readRange(1000, 400, buffer);

            // Verify returned byte count
            assertEquals(400, bytesRead2, "Should read 400 bytes again");

            // Verify buffer position and limit
            assertEquals(200, buffer.position(), "Position should be at new original position");
            assertEquals(600, buffer.limit(), "Limit should be original position + bytes read");
            assertEquals(400, buffer.remaining(), "Should have 400 bytes remaining");
        }
    }

    @Test
    void testInvalidBufferParameters() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Test with null buffer
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        cachingReader.readRange(0, 100, null);
                    },
                    "Should throw exception for null buffer");

            // Test with read-only buffer
            ByteBuffer readOnlyBuffer = ByteBuffer.allocate(100).asReadOnlyBuffer();
            assertThrows(
                    java.nio.ReadOnlyBufferException.class,
                    () -> {
                        cachingReader.readRange(0, 100, readOnlyBuffer);
                    },
                    "Should throw exception for read-only buffer");

            // Test with insufficient buffer capacity
            ByteBuffer smallBuffer = ByteBuffer.allocate(50);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        cachingReader.readRange(0, 100, smallBuffer);
                    },
                    "Should throw exception for insufficient buffer capacity");

            // Test with partially filled buffer
            ByteBuffer partialBuffer = ByteBuffer.allocate(150);
            partialBuffer.put(new byte[100]); // Fill first 100 bytes
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        cachingReader.readRange(0, 100, partialBuffer);
                    },
                    "Should throw exception for insufficient remaining capacity");
        }
    }

    @Test
    void testReadWithDirectBuffer() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Read using a direct ByteBuffer
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(500);
            int bytesRead = cachingReader.readRange(1000, 500, directBuffer);

            // Verify returned byte count
            assertEquals(500, bytesRead, "Should read 500 bytes");

            // Verify buffer is ready for reading
            assertEquals(0, directBuffer.position());
            assertEquals(500, directBuffer.limit());
            assertEquals(500, directBuffer.remaining());

            // Read again with heap buffer (should come from cache)
            ByteBuffer heapBuffer = ByteBuffer.allocate(500);
            int bytesRead2 = cachingReader.readRange(1000, 500, heapBuffer);

            // Verify returned byte count
            assertEquals(500, bytesRead2, "Should read 500 bytes again");

            // Verify both buffers have same content length
            assertEquals(directBuffer.remaining(), heapBuffer.remaining(), "Buffers should have same content length");
        }
    }

    @Test
    void testZeroLengthRead() throws IOException {
        // Create a file range reader
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Create a disk caching reader
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Read with zero length
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = cachingReader.readRange(1000, 0, buffer);

            // Should return 0 bytes read
            assertEquals(0, bytesRead, "Should read 0 bytes");

            // Buffer should not be modified
            assertEquals(0, buffer.position());
            assertEquals(0, buffer.limit());
        }
    }

    /**
     * Test that DiskCachingRangeReader is resilient to all cache files being deleted externally.
     * This simulates a scenario where a user manually deletes all files from the cache directory.
     */
    @Test
    void testResilienceToAllCacheFilesDeleted() throws IOException {
        // Create tracker to count delegate reads
        AtomicInteger delegateReadCount = new AtomicInteger(0);

        // Create a custom RangeReader that counts reads
        RangeReader countingReader = new RangeReader() {
            @SuppressWarnings("resource")
            private final RangeReader baseReader = FileRangeReader.of(testFile);

            @Override
            public ByteBuffer readRange(long offset, int length) throws IOException {
                delegateReadCount.incrementAndGet();
                return baseReader.readRange(offset, length);
            }

            @Override
            public int readRange(long offset, int length, ByteBuffer target) throws IOException {
                delegateReadCount.incrementAndGet();
                return baseReader.readRange(offset, length, target);
            }

            @Override
            public OptionalLong size() throws IOException {
                return baseReader.size();
            }

            @Override
            public void close() throws IOException {
                baseReader.close();
            }

            @Override
            public String getSourceIdentifier() {
                return sourceId;
            }
        };

        // Create a disk caching reader with the counting reader as delegate
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(countingReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Define test ranges
            long offset1 = 1000;
            int length1 = 500;
            long offset2 = 2000;
            int length2 = 600;

            // Read first range - should read from delegate
            ByteBuffer data1 = cachingReader.readRange(offset1, length1);
            assertEquals(length1, data1.remaining(), "Should read correct number of bytes");
            assertEquals(1, delegateReadCount.get(), "Should have read from delegate once");

            // Read first range again - should read from cache
            ByteBuffer data1Again = cachingReader.readRange(offset1, length1);
            assertEquals(length1, data1Again.remaining(), "Should read correct number of bytes");
            assertEquals(1, delegateReadCount.get(), "Should still have read from delegate only once");

            // Read second range - should read from delegate
            ByteBuffer data2 = cachingReader.readRange(offset2, length2);
            assertEquals(length2, data2.remaining(), "Should read correct number of bytes");
            assertEquals(2, delegateReadCount.get(), "Should have read from delegate twice");

            // Get all cache files
            Path[] cacheFiles = getAllCacheFiles(cacheDir);
            assertTrue(cacheFiles.length > 0, "Should have cache files");

            // Delete all cache files to simulate external deletion
            for (Path cacheFile : cacheFiles) {
                Files.delete(cacheFile);
            }
            // Also delete the source subdirectory to fully simulate external cleanup
            try (Stream<Path> paths = Files.walk(cacheDir)) {
                paths.filter(Files::isDirectory)
                        .filter(p -> !p.equals(cacheDir)) // Don't delete the root cache dir
                        .sorted((a, b) -> b.compareTo(a)) // Delete subdirectories
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                // Ignore cleanup errors in tests
                            }
                        });
            }

            // Verify cache directory is empty
            assertEquals(0, countCacheFiles(cacheDir), "Cache directory should be empty");

            // Force cache cleanup to happen immediately to avoid race conditions
            cachingReader.clearCache();

            // reset read count after cleanup
            delegateReadCount.set(0);

            // Read first range again - should re-read from delegate because cache is gone
            ByteBuffer data1AfterDelete = cachingReader.readRange(offset1, length1);
            assertEquals(length1, data1AfterDelete.remaining(), "Should read correct number of bytes");

            // Read second range again - should re-read from delegate because cache is gone
            ByteBuffer data2AfterDelete = cachingReader.readRange(offset2, length2);
            assertEquals(length2, data2AfterDelete.remaining(), "Should read correct number of bytes");

            // Both ranges should have been read from delegate (total should be at least 2)
            assertTrue(
                    delegateReadCount.get() >= 2,
                    String.format("Should have read from delegate at least twice, got %d", delegateReadCount.get()));

            // Verify that the cache files were recreated
            assertTrue(countCacheFiles(cacheDir) > 0, "Cache files should be recreated");

            // Read first range one more time - should now come from cache again
            int readCountBeforeCache = delegateReadCount.get();
            cachingReader.readRange(offset1, length1);
            assertEquals(readCountBeforeCache, delegateReadCount.get(), "Should not have read from delegate again");
        }
    }

    /**
     * Test that DiskCachingRangeReader is resilient to selective cache files being deleted externally.
     * This simulates a scenario where only some cache files are deleted from the cache directory.
     */
    @Test
    void testResilienceToSelectiveCacheFileDeletion() throws IOException {
        // Create tracker to count delegate reads
        AtomicInteger delegateReadCount = new AtomicInteger(0);

        // Create a custom RangeReader that counts reads
        RangeReader countingReader = new RangeReader() {
            @SuppressWarnings("resource")
            private final RangeReader baseReader = FileRangeReader.of(testFile);

            @Override
            public ByteBuffer readRange(long offset, int length) throws IOException {
                delegateReadCount.incrementAndGet();
                return baseReader.readRange(offset, length);
            }

            @Override
            public int readRange(long offset, int length, ByteBuffer target) throws IOException {
                delegateReadCount.incrementAndGet();
                return baseReader.readRange(offset, length, target);
            }

            @Override
            public OptionalLong size() throws IOException {
                return baseReader.size();
            }

            @Override
            public void close() throws IOException {
                baseReader.close();
            }

            @Override
            public String getSourceIdentifier() {
                return sourceId;
            }
        };

        // Create a disk caching reader with the counting reader as delegate
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(countingReader)
                .cacheDirectory(cacheDir)
                .build()) {
            // Define three different test ranges
            long offset1 = 1000;
            int length1 = 500;
            long offset2 = 2000;
            int length2 = 600;
            long offset3 = 3000;
            int length3 = 700;

            // Read all three ranges to populate cache
            ByteBuffer data1 = cachingReader.readRange(offset1, length1);
            ByteBuffer data2 = cachingReader.readRange(offset2, length2);
            ByteBuffer data3 = cachingReader.readRange(offset3, length3);

            assertEquals(3, delegateReadCount.get(), "Should have read from delegate three times");

            // Verify all ranges were read correctly
            assertEquals(length1, data1.remaining());
            assertEquals(length2, data2.remaining());
            assertEquals(length3, data3.remaining());

            // Get all cache files
            Path[] cacheFiles = getAllCacheFiles(cacheDir);
            assertEquals(3, cacheFiles.length, "Should have 3 cache files");

            // Delete just the second cache file
            // We need to identify which file corresponds to which range
            // For simplicity, we'll just delete one file and check which range is affected
            Files.delete(cacheFiles[1]);

            // Verify one cache file was deleted
            assertEquals(2, countCacheFiles(cacheDir), "Should have 2 remaining cache files");

            // Read all three ranges again
            ByteBuffer data1Again = cachingReader.readRange(offset1, length1);
            ByteBuffer data2Again = cachingReader.readRange(offset2, length2);
            ByteBuffer data3Again = cachingReader.readRange(offset3, length3);

            // One of these reads should have come from the delegate again
            // The other two should have come from cache
            assertTrue(
                    delegateReadCount.get() >= 4,
                    String.format(
                            "Should have read from delegate at least one more time, got %d total",
                            delegateReadCount.get()));

            // Verify all ranges were read correctly
            assertEquals(length1, data1Again.remaining());
            assertEquals(length2, data2Again.remaining());
            assertEquals(length3, data3Again.remaining());

            // Verify cache was repopulated (should have 3 files again)
            assertEquals(3, countCacheFiles(cacheDir), "Should have 3 cache files again");

            // Read all ranges one more time - all should come from cache now
            int readCountBeforeCache = delegateReadCount.get();
            cachingReader.readRange(offset1, length1);
            cachingReader.readRange(offset2, length2);
            cachingReader.readRange(offset3, length3);

            assertEquals(readCountBeforeCache, delegateReadCount.get(), "Should not have read from delegate again");
        }
    }

    /**
     * Test that DiskCachingRangeReader properly maintains its cache size tracking when
     * files are deleted externally. This ensures cache eviction works correctly even after
     * external file deletions.
     */
    @Test
    void testCacheSizeAccountingAfterFileDeletion() throws IOException {
        // Create a disk caching reader with a small max size
        final long maxCacheSize = 2000; // 2KB

        try (RangeReader baseReader = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(cacheDir)
                        .maxCacheSizeBytes(maxCacheSize)
                        .build()) {

            // Read several ranges to populate the cache
            // Each range is 500 bytes, so we should be able to store 4 ranges max
            for (int i = 0; i < 4; i++) {
                ByteBuffer data = cachingReader.readRange(i * 1000, 500);
                assertEquals(500, data.remaining(), "Should read 500 bytes");
            }

            // Verify cache size and entry count
            assertEquals(4, cachingReader.getCacheEntryCount(), "Should have 4 cache entries");
            assertEquals(2000, cachingReader.getEstimatedCacheSizeBytes(), "Cache size should be 2000 bytes");
            assertEquals(4, countCacheFiles(cacheDir), "Should have 4 cache files");

            // Get the cache files
            Path[] cacheFiles = getAllCacheFiles(cacheDir);

            // Delete 2 cache files
            Files.delete(cacheFiles[0]);
            Files.delete(cacheFiles[1]);

            // Verify files were deleted
            assertEquals(2, countCacheFiles(cacheDir), "Should have 2 cache files left");

            // Read a different range - this should not trigger eviction
            // because the actual cache size is now 1000 bytes (2 files of 500 bytes each were deleted)
            ByteBuffer data = cachingReader.readRange(5000, 500);
            assertEquals(500, data.remaining(), "Should read 500 bytes");

            // Read the ranges corresponding to the deleted files again
            // This will force the reader to discover the missing files and update its tracking
            cachingReader.readRange(0, 500);
            cachingReader.readRange(1000, 500);

            // Cache should have the entries (the 2 left after deletion + the new range + the 2 reloaded)
            // Note that total entries will be the number of unique CacheKeys
            await().atMost(2, SECONDS)
                    .untilAsserted(
                            () -> assertEquals(4, cachingReader.getCacheEntryCount(), "Should have 4 cache entries"));

            // Use Awaitility to wait for file count stabilization
            await().atMost(2, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
                // Count files in the cache directory
                long fileCount = countCacheFiles(cacheDir);
                // We expect at least 3 files (at least 1 that wasn't deleted + the new range + at least 1
                // reloaded)
                assertTrue(fileCount >= 3, String.format("Should have at least 3 cache files, found %d", fileCount));
            });

            // Calculate the actual size on disk
            long actualCacheSize = getAllCacheFiles(cacheDir).length > 0
                    ? Stream.of(getAllCacheFiles(cacheDir))
                            .mapToLong(p -> {
                                try {
                                    return Files.size(p);
                                } catch (IOException e) {
                                    return 0;
                                }
                            })
                            .sum()
                    : 0;

            // Use Awaitility to wait for the condition to be met
            await().atMost(2, SECONDS).pollInterval(100, MILLISECONDS).untilAsserted(() -> {
                // Force a synchronization by reading the cache size
                long reportedSize = cachingReader.getEstimatedCacheSizeBytes();

                // The reported size should be within a reasonable range of the actual size
                // We use a tolerance value since we've had timing issues
                assertTrue(
                        Math.abs(actualCacheSize - reportedSize) <= 500,
                        String.format(
                                "Reported cache size (%d) should be close to actual files on disk (%d)",
                                reportedSize, actualCacheSize));
            });

            // Now read one more range, which should trigger eviction
            // The cache is at capacity (5 files × 500 bytes = 2500 bytes > 2000 bytes max)
            cachingReader.readRange(6000, 500);

            // At least one entry should have been evicted
            assertTrue(cachingReader.getCacheEntryCount() < 6, "Should have fewer than 6 entries after eviction");
            await().atMost(2, SECONDS)
                    .untilAsserted(() -> assertTrue(
                            cachingReader.getEstimatedCacheSizeBytes() <= maxCacheSize,
                            "Cache size should not exceed max size"));
        }
    }

    @Test
    void testDeleteOnClose() throws IOException {
        // Create a temporary cache directory for this test
        Path testCacheDir = tempDir.resolve("test-delete-cache");
        Files.createDirectories(testCacheDir);

        try (RangeReader baseReader = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(testCacheDir)
                        .deleteOnClose()
                        .build()) {

            // Read some data to create cache files
            cachingReader.readRange(1000, 500);
            cachingReader.readRange(2000, 600);

            // Verify cache files exist
            assertTrue(countCacheFiles(testCacheDir) > 0, "Cache files should exist before close");
        }

        // After close(), cache files should be deleted
        assertEquals(0, countCacheFiles(testCacheDir), "Cache files should be deleted after close");
    }

    @Test
    void testRangeLargerThanMaxCacheSize() throws IOException {
        // Create a cache with very small max size (1KB)
        final long maxCacheSize = 1024;

        try (RangeReader baseReader = FileRangeReader.of(testFile);
                DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(cacheDir)
                        .maxCacheSizeBytes(maxCacheSize)
                        .build()) {

            // Try to read a range larger than max cache size (2KB)
            ByteBuffer buffer = cachingReader.readRange(0, 2048);
            assertEquals(2048, buffer.remaining(), "Should read 2048 bytes directly from delegate");

            // Verify no cache files were created for this large range
            assertEquals(0, countCacheFiles(cacheDir), "No cache files should be created for oversized range");

            // Read a smaller range that fits in cache
            ByteBuffer smallBuffer = cachingReader.readRange(5000, 500);
            assertEquals(500, smallBuffer.remaining(), "Should read 500 bytes and cache them");

            // Verify cache file was created for the smaller range
            assertEquals(1, countCacheFiles(cacheDir), "Cache file should be created for normal-sized range");
        }
    }

    @Test
    void testBuilderValidation() throws IOException {
        // Test null delegate
        assertThrows(
                NullPointerException.class,
                () -> {
                    DiskCachingRangeReader.builder(null);
                },
                "Should throw exception for null delegate");

        // Test zero max cache size
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    DiskCachingRangeReader.builder(FileRangeReader.of(testFile))
                            .maxCacheSizeBytes(0)
                            .build();
                },
                "Should throw exception for zero max cache size");

        // Test negative max cache size
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    DiskCachingRangeReader.builder(FileRangeReader.of(testFile))
                            .maxCacheSizeBytes(-1)
                            .build();
                },
                "Should throw exception for negative max cache size");
    }

    @Test
    void testBuilderMaxCacheSizeBytesDefault() throws IOException {
        // Test that builder uses default when maxCacheSizeBytes is not set
        try (DiskCachingRangeReader reader = DiskCachingRangeReader.builder(FileRangeReader.of(testFile))
                .cacheDirectory(cacheDir)
                .build()) {

            assertEquals(
                    DiskCachingRangeReader.DEFAULT_MAX_CACHE_SIZE,
                    reader.getMaxCacheSize(),
                    "Should use default max cache size when not specified");
        }
    }

    @Test
    void testConstructorMaxCacheSizeBytesValidation() throws IOException {
        try (RangeReader baseReader = FileRangeReader.of(testFile)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        DiskCachingRangeReader.builder(baseReader)
                                .cacheDirectory(cacheDir)
                                .maxCacheSizeBytes(0)
                                .build();
                    },
                    "Constructor should throw exception for zero max cache size");

            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        DiskCachingRangeReader.builder(baseReader)
                                .cacheDirectory(cacheDir)
                                .maxCacheSizeBytes(-1)
                                .build();
                    },
                    "Constructor should throw exception for negative max cache size");
        }
    }

    @Test
    void testVeryLargeRangeSize() throws IOException {
        // Test a very large range that's larger than cache size to verify fallback behavior

        // Create a custom RangeReader that can handle very large ranges for testing
        RangeReader mockReader = new RangeReader() {
            @Override
            public ByteBuffer readRange(long offset, int length) throws IOException {
                // For testing, just return a small buffer regardless of requested size
                return ByteBuffer.allocate(Math.min(length, 1000));
            }

            @Override
            public int readRange(long offset, int length, ByteBuffer target) throws IOException {
                int bytesToRead = Math.min(length, target.remaining());
                bytesToRead = Math.min(bytesToRead, 1000); // Simulate limited data available
                target.put(new byte[bytesToRead]);
                target.flip();
                return bytesToRead;
            }

            @Override
            public OptionalLong size() throws IOException {
                return OptionalLong.of(Long.MAX_VALUE); // Very large file
            }

            @Override
            public void close() throws IOException {
                // No-op
            }

            @Override
            public String getSourceIdentifier() {
                return "test-max-range";
            }
        };

        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(mockReader)
                .cacheDirectory(cacheDir)
                .maxCacheSizeBytes(1024 * 1024L)
                .build()) { // 1MB cache

            // Test with a very large range (500MB) that's larger than cache size - should use fallback
            int largeRangeSize = 500 * 1024 * 1024; // 500MB
            ByteBuffer buffer = cachingReader.readRange(0, largeRangeSize);
            assertNotNull(buffer, "Should successfully read using fallback");
            assertEquals(1000, buffer.remaining(), "Should return data from mock delegate");

            // Verify no cache files were created for this oversized range
            assertEquals(0, countCacheFiles(cacheDir), "No cache files should be created for oversized range");
        }
    }
}
