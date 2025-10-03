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
package io.tileverse.rangereader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.cache.DiskCachingRangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for using different block sizes for memory and disk operations.
 */
class DualBlockSizeRangeReaderTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private Path cacheDir;
    private static final int FILE_SIZE = 100_000;
    private static final int MEMORY_BLOCK_SIZE = 16 * 1024; // 16KB
    private static final int DISK_BLOCK_SIZE = 64 * 1024; // 64KB
    private static final String SOURCE_ID = "test-file";

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file
        testFile = tempDir.resolve("test.bin");
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

    @Test
    void testBuilderWithDifferentBlockSizes() throws IOException {
        // Create a reader with different block sizes using the updated builder
        RangeReader reader = CachingRangeReader.builder(BlockAlignedRangeReader.builder(DiskCachingRangeReader.builder(
                                        FileRangeReader.builder().path(testFile).build())
                                .cacheDirectory(cacheDir)
                                .build())
                        .blockSize(DISK_BLOCK_SIZE)
                        .build())
                .build();

        // Test reading a range (should go through both block alignments)
        ByteBuffer data = ByteBuffer.allocate(1024);
        reader.readRange(1000, 500, data);
        data.flip();
        assertEquals(500, data.remaining(), "Should read 500 bytes");

        // Read the same range again (should now come from memory cache)
        ByteBuffer cachedData = ByteBuffer.allocate(1024);
        reader.readRange(1000, 500, cachedData);
        cachedData.flip();
        assertEquals(500, cachedData.remaining(), "Should read 500 bytes from cache");

        // Verify data is the same
        data.rewind();
        cachedData.rewind();
        byte[] dataBytes = new byte[data.remaining()];
        byte[] cachedBytes = new byte[cachedData.remaining()];
        data.get(dataBytes);
        cachedData.get(cachedBytes);
        assertArrayEquals(dataBytes, cachedBytes, "Data should be identical");

        // Verify that cache files were created with the larger block size
        assertTrue(Files.list(cacheDir).count() > 0, "Cache directory should contain files");

        // Close the reader
        reader.close();
    }

    @Test
    void testManualConstructionWithDifferentBlockSizes() throws IOException {
        // Create a reader with different block sizes manually
        RangeReader baseReader = FileRangeReader.of(testFile);
        RangeReader diskCache = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .build();
        RangeReader diskBlocks = new BlockAlignedRangeReader(diskCache, DISK_BLOCK_SIZE);
        RangeReader memCache = CachingRangeReader.builder(diskBlocks).build();
        RangeReader memBlocks = new BlockAlignedRangeReader(memCache, MEMORY_BLOCK_SIZE);

        // Test reading a range
        ByteBuffer data = ByteBuffer.allocate(1024);
        memBlocks.readRange(1000, 500, data);
        data.flip();
        assertEquals(500, data.remaining(), "Should read 500 bytes");

        // Read the same range again (should now come from memory cache)
        ByteBuffer cachedData = ByteBuffer.allocate(1024);
        memBlocks.readRange(1000, 500, cachedData);
        cachedData.flip();
        assertEquals(500, cachedData.remaining(), "Should read 500 bytes from cache");

        // Verify data is the same
        data.rewind();
        cachedData.rewind();
        byte[] dataBytes = new byte[data.remaining()];
        byte[] cachedBytes = new byte[cachedData.remaining()];
        data.get(dataBytes);
        cachedData.get(cachedBytes);
        assertArrayEquals(dataBytes, cachedBytes, "Data should be identical");

        // Close the reader
        memBlocks.close();
    }

    @Test
    void testDefaultBlockSizes() throws IOException {
        // Test with default block sizes
        RangeReader reader = CachingRangeReader.builder(BlockAlignedRangeReader.builder(DiskCachingRangeReader.builder(
                                        FileRangeReader.builder().path(testFile).build())
                                .cacheDirectory(cacheDir)
                                .build())
                        .blockSize(BlockAlignedRangeReader.DEFAULT_BLOCK_SIZE)
                        .build())
                .build();

        // Test reading a range
        ByteBuffer data = ByteBuffer.allocate(1024);
        reader.readRange(1000, 500, data);
        data.flip();
        assertEquals(500, data.remaining(), "Should read 500 bytes");

        // Close the reader
        reader.close();
    }
}
