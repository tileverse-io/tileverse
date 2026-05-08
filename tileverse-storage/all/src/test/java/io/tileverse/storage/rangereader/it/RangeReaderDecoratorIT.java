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
package io.tileverse.storage.rangereader.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.block.BlockAlignedRangeReader;
import io.tileverse.storage.cache.CachingRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for RangeReader decorator implementations.
 *
 * <p>These tests verify the behavior of the BlockAlignedRangeReader and CachingRangeReader implementations, as well as
 * their combined usage.
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
        testFile = tempDir.resolve("test-data.bin");
        // Predictable byte pattern so range reads can be verified by index arithmetic
        // rather than re-reading the file.
        expectedFileContent = new byte[FILE_SIZE];
        for (int i = 0; i < FILE_SIZE; i++) {
            expectedFileContent[i] = (byte) (i % 256);
        }
        Files.write(testFile, expectedFileContent);
    }

    @Test
    void testBlockAlignedReaderBasicFunctionality() throws IOException {
        RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(baseReader)) {
            assertEquals(
                    BlockAlignedRangeReader.DEFAULT_BLOCK_SIZE,
                    reader.getBlockSize(),
                    "Default block size should be used");
            // Read crosses a block boundary (100 bytes before, 100 after).
            assertReadMatchesExpected(reader, DEFAULT_BLOCK_SIZE - 100, 200);
        }
    }

    @Test
    void testBlockAlignedReaderWithCustomBlockSize() throws IOException {
        RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
        try (BlockAlignedRangeReader reader = new BlockAlignedRangeReader(baseReader, CUSTOM_BLOCK_SIZE)) {
            assertEquals(CUSTOM_BLOCK_SIZE, reader.getBlockSize(), "Custom block size should be used");
            assertReadMatchesExpected(reader, CUSTOM_BLOCK_SIZE - 100, 200);
        }
    }

    @Test
    void testCachingReaderBasicFunctionality() throws IOException {
        RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader = CachingRangeReader.builder(baseReader).build()) {
            int offset = 1000;
            int length = 100;
            ByteBuffer first = reader.readRange(offset, length).flip();
            assertEquals(length, first.remaining(), "Should read the requested number of bytes");
            // Same range twice exercises the cache hit path.
            ByteBuffer second = reader.readRange(offset, length).flip();
            assertEquals(length, second.remaining(), "Should read the requested number of bytes");

            byte[] firstBytes = new byte[length];
            byte[] secondBytes = new byte[length];
            first.duplicate().get(firstBytes);
            second.duplicate().get(secondBytes);
            assertArrayEquals(firstBytes, secondBytes, "Data from the cache should match the original");

            assertTrue(reader.getCacheStats().entryCount() > 0, "Cache should contain entries");
            assertTrue(reader.getCacheStats().hitCount() > 0, "Should have cache hits");
        }
    }

    @Test
    void testBlockAlignedAndCachingReader() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile)) {
            RangeReader reader = CachingRangeReader.builder(baseReader)
                    .blockSize(CUSTOM_BLOCK_SIZE)
                    .build();
            // Boundary-crossing read populates the cache for the underlying block(s).
            ByteBuffer initial = reader.readRange(CUSTOM_BLOCK_SIZE - 100, 200).flip();
            assertEquals(200, initial.remaining(), "Should read the requested number of bytes");
            // Subset read of the same block range should hit the cache.
            assertReadMatchesExpected(reader, CUSTOM_BLOCK_SIZE - 50, 50);
        }
    }

    @Test
    void testRangeReaderBuilderWithDecorators() throws IOException {
        try (RangeReader reader = CachingRangeReader.builder(
                        BlockAlignedRangeReader.builder(RangeReaderTestSupport.fileReader(testFile))
                                .blockSize(CUSTOM_BLOCK_SIZE)
                                .build())
                .build()) {
            // First pass: cold reads at block boundaries.
            for (int block = 0; block < 3; block++) {
                assertReadMatchesExpected(reader, block * CUSTOM_BLOCK_SIZE, 1000);
            }
            // Second pass: same ranges again to exercise the cache.
            for (int block = 0; block < 3; block++) {
                assertReadMatchesExpected(reader, block * CUSTOM_BLOCK_SIZE, 1000);
            }
        }
    }

    @Test
    void testLargeBlockAlignedReads() throws IOException {
        int largeBlockSize = 32 * 1024; // 32KB
        try (RangeReader reader = BlockAlignedRangeReader.builder(RangeReaderTestSupport.fileReader(testFile))
                .blockSize(largeBlockSize)
                .build()) {
            int[] offsets = {0, 100, largeBlockSize - 1000, largeBlockSize, largeBlockSize + 100};
            int[] lengths = {100, 1000, 2000, largeBlockSize / 2, largeBlockSize};
            for (int offset : offsets) {
                for (int length : lengths) {
                    int clamped = Math.min(length, FILE_SIZE - offset);
                    if (clamped <= 0) {
                        continue;
                    }
                    assertReadMatchesExpected(reader, offset, clamped);
                }
            }
        }
    }

    @Test
    void testRandomizedReads() throws IOException {
        try (RangeReader reader = CachingRangeReader.builder(
                        BlockAlignedRangeReader.builder(RangeReaderTestSupport.fileReader(testFile))
                                .blockSize(DEFAULT_BLOCK_SIZE)
                                .build())
                .build()) {
            Random random = new Random(42); // fixed seed for reproducibility
            for (int i = 0; i < 100; i++) {
                int offset = random.nextInt(FILE_SIZE - 1000);
                int length = random.nextInt(1000) + 1;
                assertReadMatchesExpected(reader, offset, length);
            }
        }
    }

    private void assertReadMatchesExpected(RangeReader reader, int offset, int length) {
        ByteBuffer buffer = reader.readRange(offset, length).flip();
        assertEquals(length, buffer.remaining(), "Should read " + length + " bytes from offset " + offset);
        byte[] actual = new byte[length];
        buffer.get(actual);
        byte[] expected = Arrays.copyOfRange(expectedFileContent, offset, offset + length);
        assertArrayEquals(expected, actual, "Data from offset " + offset + " with length " + length + " should match");
    }
}
