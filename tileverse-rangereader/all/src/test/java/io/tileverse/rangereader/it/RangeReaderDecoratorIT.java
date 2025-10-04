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
package io.tileverse.rangereader.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for RangeReader decorator implementations.
 * <p>
 * These tests verify the behavior of the BlockAlignedRangeReader and
 * CachingRangeReader implementations, as well as their combined usage.
 */
class RangeReaderDecoratorIT {

    private static final int FILE_SIZE = 128 * 1024; // 128KB test file
    private static final int DEFAULT_BLOCK_SIZE = 4096; // 4KB blocks
    private static final int CUSTOM_BLOCK_SIZE = 8192; // 8KB blocks

    @TempDir
    Path tempDir;

    private Path testFile;
    private byte[] expectedFileContent;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file with predictable content
        testFile = tempDir.resolve("test-data.bin");

        // Create data with a predictable pattern (index % 256 for each byte)
        expectedFileContent = new byte[FILE_SIZE];
        for (int i = 0; i < FILE_SIZE; i++) {
            expectedFileContent[i] = (byte) (i % 256);
        }

        // Write the test file
        java.nio.file.Files.write(testFile, expectedFileContent);
    }

    @Test
    void testBlockAlignedReaderBasicFunctionality() throws IOException {
        // Create a base reader
        FileRangeReader baseReader = FileRangeReader.of(testFile);

        // Create block-aligned reader with default block size
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(baseReader)) {
            // Check block size
            assertEquals(
                    BlockAlignedRangeReader.DEFAULT_BLOCK_SIZE,
                    reader.getBlockSize(),
                    "Default block size should be used");

            // Read data that crosses block boundaries
            int offset = DEFAULT_BLOCK_SIZE - 100; // 100 bytes before a block boundary
            int length = 200; // Crosses the boundary

            ByteBuffer buffer = reader.readRange(offset, length).flip();
            assertEquals(length, buffer.remaining(), "Should read the requested number of bytes");

            // Verify the content
            byte[] data = new byte[length];
            buffer.get(data);

            // Compare with the expected content
            byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
            assertArrayEquals(expected, data, "Data should match the expected content");
        }
    }

    @Test
    void testBlockAlignedReaderWithCustomBlockSize() throws IOException {
        // Create a base reader
        FileRangeReader baseReader = FileRangeReader.of(testFile);

        // Create block-aligned reader with custom block size
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(baseReader, CUSTOM_BLOCK_SIZE)) {
            // Check block size
            assertEquals(CUSTOM_BLOCK_SIZE, reader.getBlockSize(), "Custom block size should be used");

            // Read data at a custom block boundary
            int offset = CUSTOM_BLOCK_SIZE - 100; // 100 bytes before a block boundary
            int length = 200; // Crosses the boundary

            ByteBuffer buffer = reader.readRange(offset, length).flip();
            assertEquals(length, buffer.remaining(), "Should read the requested number of bytes");

            // Verify the content
            byte[] data = new byte[length];
            buffer.get(data);

            // Compare with the expected content
            byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
            assertArrayEquals(expected, data, "Data should match the expected content");
        }
    }

    @Test
    void testCachingReaderBasicFunctionality() throws IOException {
        // Create a base reader
        FileRangeReader baseReader = FileRangeReader.of(testFile);

        // Create caching reader
        try (CachingRangeReader reader = CachingRangeReader.builder(baseReader).build()) {
            // Read a range
            int offset = 1000;
            int length = 100;

            ByteBuffer buffer1 = reader.readRange(offset, length).flip();
            assertEquals(length, buffer1.remaining(), "Should read the requested number of bytes");

            // Read the same range again - should be cached
            ByteBuffer buffer2 = reader.readRange(offset, length).flip();
            assertEquals(length, buffer2.remaining(), "Should read the requested number of bytes");

            // Verify the content is the same
            byte[] data1 = new byte[length];
            byte[] data2 = new byte[length];
            buffer1.duplicate().get(data1);
            buffer2.duplicate().get(data2);

            assertArrayEquals(data1, data2, "Data from the cache should match the original");

            // Verify cache stats
            assertTrue(reader.getCacheStats().entryCount() > 0, "Cache should contain entries");
            assertTrue(reader.getCacheStats().hitCount() > 0, "Should have cache hits");
        }
    }

    @Test
    void testBlockAlignedAndCachingReader() throws IOException {

        try (RangeReader baseReader = FileRangeReader.of(testFile)) {
            RangeReader reader = CachingRangeReader.builder(baseReader)
                    .blockSize(CUSTOM_BLOCK_SIZE)
                    .build();
            // Read a range that crosses a block boundary
            int offset = CUSTOM_BLOCK_SIZE - 100;
            int length = 200;

            ByteBuffer buffer1 = reader.readRange(offset, length).flip();
            assertEquals(length, buffer1.remaining(), "Should read the requested number of bytes");

            // Read a subset of the previously read range - should be cached
            int subsetOffset = offset + 50;
            int subsetLength = 50;

            ByteBuffer buffer2 = reader.readRange(subsetOffset, subsetLength).flip();
            assertEquals(subsetLength, buffer2.remaining(), "Should read the requested number of bytes");

            // Verify the content
            byte[] data = new byte[subsetLength];
            buffer2.get(data);

            // Compare with the expected content
            byte[] expected = Arrays.copyOfRange(expectedFileContent, subsetOffset, subsetOffset + subsetLength);
            assertArrayEquals(expected, data, "Data should match the expected content");
        }
    }

    @Test
    void testRangeReaderBuilderWithDecorators() throws IOException {
        // Use the builder to create a reader with both decorators
        try (RangeReader reader = CachingRangeReader.builder(BlockAlignedRangeReader.builder(
                                FileRangeReader.builder().path(testFile).build())
                        .blockSize(CUSTOM_BLOCK_SIZE)
                        .build())
                .build()) {

            // Read data at block boundaries
            for (int i = 0; i < 3; i++) {
                int offset = i * CUSTOM_BLOCK_SIZE;
                int length = 1000;

                ByteBuffer buffer = reader.readRange(offset, length).flip();
                assertEquals(length, buffer.remaining(), "Should read the requested number of bytes");

                // Verify the content
                byte[] data = new byte[length];
                buffer.get(data);

                // Compare with the expected content
                byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
                assertArrayEquals(expected, data, "Data should match the expected content");
            }

            // Read some of the same ranges again - should be cached
            for (int i = 0; i < 3; i++) {
                int offset = i * CUSTOM_BLOCK_SIZE;
                int length = 1000;

                ByteBuffer buffer = reader.readRange(offset, length).flip();
                assertEquals(length, buffer.remaining(), "Should read the requested number of bytes");

                // Verify the content
                byte[] data = new byte[length];
                buffer.get(data);

                // Compare with the expected content
                byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
                assertArrayEquals(expected, data, "Data should match the expected content");
            }
        }
    }

    @Test
    void testLargeBlockAlignedReads() throws IOException {
        // Create a reader with a large block size
        int largeBlockSize = 32 * 1024; // 32KB

        try (RangeReader reader = BlockAlignedRangeReader.builder(
                        FileRangeReader.builder().path(testFile).build())
                .blockSize(largeBlockSize)
                .build()) {

            // Read data in various patterns
            int[] offsets = {0, 100, largeBlockSize - 1000, largeBlockSize, largeBlockSize + 100};
            int[] lengths = {100, 1000, 2000, largeBlockSize / 2, largeBlockSize};

            for (int offset : offsets) {
                for (int length : lengths) {
                    // Adjust if we're going beyond file size
                    if (offset + length > FILE_SIZE) {
                        length = FILE_SIZE - offset;
                    }

                    if (length <= 0) continue;

                    ByteBuffer buffer = reader.readRange(offset, length).flip();
                    assertEquals(length, buffer.remaining(), "Should read " + length + " bytes from offset " + offset);

                    // Verify the content
                    byte[] data = new byte[length];
                    buffer.get(data);

                    // Compare with the expected content
                    byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
                    assertArrayEquals(
                            expected, data, "Data from offset " + offset + " with length " + length + " should match");
                }
            }
        }
    }

    @Test
    void testRandomizedReads() throws IOException {
        // Create a reader with both caching and block alignment
        try (RangeReader reader = CachingRangeReader.builder(BlockAlignedRangeReader.builder(
                                FileRangeReader.builder().path(testFile).build())
                        .blockSize(DEFAULT_BLOCK_SIZE)
                        .build())
                .build()) {

            // Perform random reads
            Random random = new Random(42); // Fixed seed for reproducibility

            for (int i = 0; i < 100; i++) {
                // Random offset up to FILE_SIZE - 1000
                int offset = random.nextInt(FILE_SIZE - 1000);
                // Random length between 1 and 1000
                int length = random.nextInt(1000) + 1;

                ByteBuffer buffer = reader.readRange(offset, length).flip();
                assertEquals(length, buffer.remaining(), "Should read " + length + " bytes from offset " + offset);

                // Verify the content
                byte[] data = new byte[length];
                buffer.get(data);

                // Compare with the expected content
                byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
                assertArrayEquals(
                        expected, data, "Data from offset " + offset + " with length " + length + " should match");
            }
        }
    }
}
