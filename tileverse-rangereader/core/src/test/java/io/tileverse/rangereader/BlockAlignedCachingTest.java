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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for combining BlockAlignedRangeReader with CachingRangeReader.
 */
class BlockAlignedCachingTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private String textContent;
    private FileRangeReader fileReader;
    private CountingRangeReader countingReader;
    private BlockAlignedRangeReader blockAlignedReader;
    private CachingRangeReader cachingReader;

    // Use a small block size for testing
    private static final int TEST_BLOCK_SIZE = 16;

    ByteBuffer buffer;

    @BeforeEach
    void setUp() throws IOException {
        buffer = ByteBuffer.allocate(2 * TEST_BLOCK_SIZE);
        // Create a test file with known content
        testFile = tempDir.resolve("block-cache-test.txt");
        textContent = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Files.writeString(testFile, textContent, StandardOpenOption.CREATE);

        // Initialize the readers - this shows the decorator pattern in action
        fileReader = FileRangeReader.of(testFile);
        countingReader = new CountingRangeReader(fileReader);
        blockAlignedReader = new BlockAlignedRangeReader(countingReader, TEST_BLOCK_SIZE);
        cachingReader = CachingRangeReader.builder(blockAlignedReader).build();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    @Test
    void testCachingOfBlockAlignedReads() throws IOException {
        // Read a range that's not aligned to a block boundary
        cachingReader.readRange(10, 10, buffer);
        assertEquals(10, buffer.flip().remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        assertEquals(textContent.substring(10, 20), new String(bytes, StandardCharsets.UTF_8));

        // The BlockAlignedRangeReader should have read the full blocks containing the range
        // which means the CountingRangeReader should have been called once or twice
        // (depending on whether the range crosses a block boundary)
        int initialCount = countingReader.getReadCount();
        assertTrue(initialCount == 1 || initialCount == 2, "Expected 1 or 2 reads, but got " + initialCount);

        // Read the same range again - it should come from cache
        cachingReader.readRange(10, 10, buffer.clear());
        buffer.flip();
        assertEquals(10, buffer.remaining());

        byte[] bytes2 = new byte[buffer.remaining()];
        buffer.get(bytes2);
        assertEquals(textContent.substring(10, 20), new String(bytes2, StandardCharsets.UTF_8));

        // CountingRangeReader should not have been called again
        assertEquals(initialCount, countingReader.getReadCount());
    }

    @Test
    void testCachingEfficiencyWithIdenticalReads() throws IOException {
        // Initial state
        assertEquals(0, countingReader.getReadCount());

        // First read: should result in BlockAlignedRangeReader accessing the underlying file
        cachingReader.readRange(0, 5, buffer.clear());
        int firstReadCount = countingReader.getReadCount();
        assertTrue(firstReadCount > 0, "Expected at least one read");

        // Same read again: should come from cache
        cachingReader.readRange(0, 5, buffer.clear());
        assertEquals(firstReadCount, countingReader.getReadCount(), "Should not have read from file again");

        // Different read: will hit the file again
        cachingReader.readRange(10, 5, buffer.clear());
        int secondReadCount = countingReader.getReadCount();
        assertTrue(secondReadCount > firstReadCount, "Expected additional reads for new range");

        // Repeat the second read: should come from cache
        cachingReader.readRange(10, 5, buffer.clear());
        assertEquals(secondReadCount, countingReader.getReadCount(), "Should not have read from file again");

        // Another new read
        cachingReader.readRange(20, 10, buffer.clear());
        int thirdReadCount = countingReader.getReadCount();
        assertTrue(thirdReadCount > secondReadCount, "Expected additional reads for new range");

        // Repeat third read: should come from cache
        cachingReader.readRange(20, 10, buffer.clear());
        assertEquals(thirdReadCount, countingReader.getReadCount(), "Should not have read from file again");
    }

    /**
     * A RangeReader that counts the number of reads for testing purposes.
     */
    private static class CountingRangeReader implements RangeReader {
        private final RangeReader delegate;
        private int readCount = 0;

        public CountingRangeReader(RangeReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) throws IOException {
            readCount++;
            return delegate.readRange(offset, length, target);
        }

        @Override
        public OptionalLong size() throws IOException {
            return delegate.size();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        public int getReadCount() {
            return readCount;
        }
    }
}
