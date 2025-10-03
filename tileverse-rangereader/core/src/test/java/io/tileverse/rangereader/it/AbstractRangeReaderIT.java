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

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Abstract base class for RangeReader integration tests.
 * <p>
 * This class provides a comprehensive set of tests for all RangeReader implementations,
 * along with their decorator combinations (caching, block alignment, etc.).
 * <p>
 * Subclasses only need to implement the createBaseReader() method to provide the
 * specific RangeReader implementation to test. This class handles creating all the
 * decorated versions (with caching, block alignment, etc.) using that base reader.
 */
public abstract class AbstractRangeReaderIT {

    protected static final int TEST_FILE_SIZE = 100 * 1024; // 100 KB
    protected static final int DEFAULT_BLOCK_SIZE = 4096; // 4KB blocks
    protected static final int LARGE_BLOCK_SIZE = 16384; // 16KB blocks

    protected Path testFile;

    /**
     * Creates a basic RangeReader implementation for the standard test file.
     * <p>
     * This is the base implementation without any decorators.
     * <p>
     * This is the only method that subclasses must implement. All other reader types
     * will be created by decorating this base reader.
     *
     * @return A basic RangeReader implementation
     * @throws IOException If an error occurs creating the reader
     */
    protected abstract RangeReader createBaseReader() throws IOException;

    /**
     * Creates a RangeReader with memory caching.
     *
     * @return A RangeReader with memory caching
     * @throws IOException If an error occurs creating the reader
     */
    protected RangeReader createCachingReader() throws IOException {
        return CachingRangeReader.builder(createBaseReader()).build();
    }

    /**
     * Creates a RangeReader with block alignment using the default block size.
     *
     * @return A RangeReader with block alignment
     * @throws IOException If an error occurs creating the reader
     */
    protected RangeReader createBlockAlignedReader() throws IOException {
        return new BlockAlignedRangeReader(createBaseReader(), DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a RangeReader with both block alignment and memory caching.
     * <p>
     * Note: Caching is applied first, then block alignment is applied on top.
     * This way the cache operates at the block aligned range level.
     *
     * @return A RangeReader with block alignment and caching
     * @throws IOException If an error occurs creating the reader
     */
    protected RangeReader createBlockAlignedCachingReader() throws IOException {
        CachingRangeReader cachingReader =
                CachingRangeReader.builder(createBaseReader()).build();
        return new BlockAlignedRangeReader(cachingReader, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Creates a RangeReader with custom block size.
     *
     * @param blockSize The block size to use
     * @return A RangeReader with the specified block size
     * @throws IOException If an error occurs creating the reader
     */
    protected RangeReader createCustomBlockSizeReader(int blockSize) throws IOException {
        return new BlockAlignedRangeReader(createBaseReader(), blockSize);
    }

    /**
     * Creates a RangeReader with custom block size and memory caching.
     * <p>
     * Note: Caching is applied first, then block alignment is applied on top.
     * This way the cache operates at the block aligned range level.
     *
     * @param blockSize The block size to use
     * @return A RangeReader with the specified block size and memory caching
     * @throws IOException If an error occurs creating the reader
     */
    protected RangeReader createCustomBlockSizeCachingReader(int blockSize) throws IOException {
        CachingRangeReader cachingReader =
                CachingRangeReader.builder(createBaseReader()).build();
        return new BlockAlignedRangeReader(cachingReader, blockSize);
    }

    /**
     * Setup that happens before each test.
     * Subclasses should implement this method to set up any necessary resources.
     *
     * @throws IOException If an I/O error occurs
     */
    @BeforeEach
    protected abstract void setUp() throws IOException;

    /**
     * Tests basic read operations of the base RangeReader.
     */
    @Test
    public void testBaseRangeReader() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            // Verify size
            assertEquals(TEST_FILE_SIZE, reader.size().getAsLong(), "File size should match");

            // Read the first 127 bytes (header)
            ByteBuffer headerBuffer = reader.readRange(0, 127).flip();
            assertEquals(127, headerBuffer.remaining(), "Header size should be 127 bytes");

            // Check if the first 7 bytes are "TstFile"
            byte[] headerBytes = new byte[7];
            headerBuffer.get(headerBytes);
            assertEquals("TstFile", new String(headerBytes), "File should begin with TstFile magic string");

            // Read a range from the middle of the file
            ByteBuffer middleBuffer = reader.readRange(1000, 100).flip();
            assertEquals(100, middleBuffer.remaining(), "Middle buffer should have 100 bytes");

            // Read to the end of the file
            ByteBuffer endBuffer = reader.readRange(TEST_FILE_SIZE - 50, 100).flip();
            assertEquals(50, endBuffer.remaining(), "End buffer should be truncated to file size");

            // Test with the new readRange method that takes a ByteBuffer
            ByteBuffer explicitBuffer = ByteBuffer.allocate(200);
            int bytesRead = reader.readRange(500, 200, explicitBuffer);
            explicitBuffer.flip();

            // Verify returned byte count
            assertEquals(200, bytesRead, "Should read 200 bytes");

            // Verify buffer is ready for reading
            assertEquals(0, explicitBuffer.position());
            assertEquals(200, explicitBuffer.limit());
            assertEquals(200, explicitBuffer.remaining());
        }
    }

    /**
     * Tests reading with direct ByteBuffer.
     */
    @Test
    public void testReadWithDirectBuffer() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            // Test with direct ByteBuffer
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(500);
            int bytesRead = reader.readRange(1000, 500, directBuffer);
            directBuffer.flip();

            // Verify returned byte count
            assertEquals(500, bytesRead, "Should read 500 bytes");

            // Verify buffer is ready for reading
            assertEquals(0, directBuffer.position());
            assertEquals(500, directBuffer.limit());
            assertEquals(500, directBuffer.remaining());

            // Read same range with heap buffer for comparison
            ByteBuffer heapBuffer = ByteBuffer.allocate(500);
            reader.readRange(1000, 500, heapBuffer);
            heapBuffer.flip();

            // Copy direct buffer to byte array
            byte[] directBytes = new byte[500];
            directBuffer.get(directBytes);

            // Copy heap buffer to byte array
            byte[] heapBytes = new byte[500];
            heapBuffer.get(heapBytes);

            // Compare contents
            assertArrayEquals(heapBytes, directBytes, "Direct buffer and heap buffer should have same content");
        }
    }

    /**
     * Tests reading with a buffer that has an offset position.
     */
    @Test
    public void testReadWithBufferOffset() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            // Create a buffer with position offset
            ByteBuffer buffer = ByteBuffer.allocate(700);
            buffer.position(100); // Start at position 100
            buffer.limit(600);

            // Read into buffer at position
            int bytesRead = reader.readRange(1000, 400, buffer);

            // Verify returned byte count
            assertEquals(400, bytesRead, "Should read 400 bytes");

            // Verify buffer position and limit
            assertEquals(500, buffer.position(), "Position should be position + bytes read");
            assertEquals(600, buffer.limit(), "Limit should be preserved");
            assertEquals(100, buffer.remaining(), "Remaining should be bytes read - capacity");
        }
    }

    /**
     * Tests edge cases like reading at EOF, zero-length reads.
     */
    @Test
    public void testEdgeCases() throws IOException {
        try (RangeReader reader = createBaseReader()) {
            // Test reading at EOF
            ByteBuffer eofBuffer = reader.readRange(TEST_FILE_SIZE, 100).flip();
            assertEquals(0, eofBuffer.remaining(), "Reading at EOF should return empty buffer");

            // Test reading with zero length
            ByteBuffer zeroBuffer = reader.readRange(1000, 0).flip();
            assertEquals(0, zeroBuffer.remaining(), "Reading zero bytes should return empty buffer");

            // Test with explicit buffer at EOF
            ByteBuffer explicitEofBuffer = ByteBuffer.allocate(100);
            int bytesRead = reader.readRange(TEST_FILE_SIZE, 100, explicitEofBuffer);
            explicitEofBuffer.flip();

            assertEquals(0, bytesRead, "Reading at EOF should return 0 bytes read");
            assertEquals(0, explicitEofBuffer.position(), "Position should be 0");
            assertEquals(0, explicitEofBuffer.limit(), "Limit should be 0");

            // Test with zero length explicit buffer
            ByteBuffer explicitZeroBuffer = ByteBuffer.allocate(100);
            int zeroBytesRead = reader.readRange(1000, 0, explicitZeroBuffer);

            assertEquals(0, zeroBytesRead, "Reading zero bytes should return 0 bytes read");
            assertEquals(0, explicitZeroBuffer.position(), "Position should remain unchanged");

            // Test with one byte remaining and request more than remaining
            ByteBuffer explicitOneBuffer = ByteBuffer.allocate(100);
            int oneBytesRead = reader.readRange(TEST_FILE_SIZE - 1, 100, explicitOneBuffer);

            assertEquals(1, oneBytesRead);
            assertEquals(1, explicitOneBuffer.position());
        }
    }

    /**
     * Tests memory caching operations.
     */
    @Test
    public void testCaching() throws IOException {
        try (RangeReader reader = createCachingReader()) {
            // Read the same range twice - second read should be from cache
            ByteBuffer buffer1 = reader.readRange(1000, 100).flip();
            assertEquals(100, buffer1.remaining(), "First read should return 100 bytes");

            // Read the exact same range again
            ByteBuffer buffer2 = reader.readRange(1000, 100).flip();
            assertEquals(100, buffer2.remaining(), "Second read should return 100 bytes");

            // Compare the data - should be identical
            byte[] data1 = new byte[buffer1.remaining()];
            byte[] data2 = new byte[buffer2.remaining()];
            buffer1.duplicate().get(data1);
            buffer2.duplicate().get(data2);

            // Check that the data is identical
            assertArrayEquals(data1, data2, "Cached data should match original");

            // Test with explicit buffer
            ByteBuffer explicitBuffer = ByteBuffer.allocate(100);
            int bytesRead = reader.readRange(1000, 100, explicitBuffer);
            explicitBuffer.flip();

            assertEquals(100, bytesRead, "Should read 100 bytes");

            byte[] explicitData = new byte[100];
            explicitBuffer.get(explicitData);

            assertArrayEquals(data1, explicitData, "Explicit buffer data should match original");
        }
    }

    /**
     * Tests block-aligned reading operations.
     */
    @Test
    public void testBlockAlignment() throws IOException {
        try (RangeReader reader = createBlockAlignedReader()) {
            // Read ranges that cross block boundaries
            ByteBuffer buffer1 = reader.readRange(DEFAULT_BLOCK_SIZE - 100, 200); // Crosses block boundary
            assertEquals(200, buffer1.flip().remaining(), "Should read 200 bytes");

            // Read a range within a block
            ByteBuffer buffer2 = reader.readRange(DEFAULT_BLOCK_SIZE, 1000); // Starts at block boundary
            assertEquals(1000, buffer2.flip().remaining(), "Should read 1000 bytes");

            // Read a range at the end of the file
            ByteBuffer buffer3 = reader.readRange(TEST_FILE_SIZE - 100, 200);
            assertEquals(100, buffer3.remaining(), "Should read 100 bytes (truncated at EOF)");

            // Test with explicit buffer
            ByteBuffer explicitBuffer = ByteBuffer.allocate(200);
            int bytesRead = reader.readRange(DEFAULT_BLOCK_SIZE - 100, 200, explicitBuffer);
            explicitBuffer.flip();

            assertEquals(200, bytesRead, "Should read 200 bytes");
            assertEquals(0, explicitBuffer.position(), "Position should be 0");
            assertEquals(200, explicitBuffer.limit(), "Limit should be 200");
        }
    }

    /**
     * Tests combined block alignment and caching.
     */
    @Test
    public void testBlockAlignmentAndCaching() throws IOException {
        try (RangeReader reader = createBlockAlignedCachingReader()) {
            // Read a range that crosses a block boundary
            ByteBuffer buffer1 = reader.readRange(DEFAULT_BLOCK_SIZE - 100, 200).flip();
            assertEquals(200, buffer1.remaining(), "Should read 200 bytes");

            // Read a subset of the previously read range - should be cached
            ByteBuffer buffer2 = reader.readRange(DEFAULT_BLOCK_SIZE - 50, 100).flip();
            assertEquals(100, buffer2.remaining(), "Should read 100 bytes");

            // Read the exact same range again
            ByteBuffer buffer3 = reader.readRange(DEFAULT_BLOCK_SIZE - 50, 100).flip();
            assertEquals(100, buffer3.remaining(), "Should read 100 bytes again");

            // Test with explicit buffer
            ByteBuffer explicitBuffer = ByteBuffer.allocate(100);
            int bytesRead = reader.readRange(DEFAULT_BLOCK_SIZE - 50, 100, explicitBuffer);
            explicitBuffer.flip();
            assertEquals(100, bytesRead, "Should read 100 bytes");

            // Get data from third buffer
            byte[] data3 = new byte[100];
            buffer3.duplicate().get(data3);

            // Get data from explicit buffer
            byte[] explicitData = new byte[100];
            explicitBuffer.get(explicitData);

            assertArrayEquals(data3, explicitData, "Explicit buffer data should match cached data");
        }
    }

    /**
     * Tests reading operations with a custom block size.
     */
    @Test
    public void testCustomBlockSize() throws IOException {
        try (RangeReader reader = createCustomBlockSizeReader(LARGE_BLOCK_SIZE)) {
            // Read a range that crosses a custom block boundary
            ByteBuffer buffer1 = reader.readRange(LARGE_BLOCK_SIZE - 100, 200).flip();
            assertEquals(200, buffer1.remaining(), "Should read 200 bytes");

            // Read a range within a block
            ByteBuffer buffer2 = reader.readRange(LARGE_BLOCK_SIZE, 1000).flip(); // Starts at block boundary
            assertEquals(1000, buffer2.remaining(), "Should read 1000 bytes");

            // Test with explicit buffer
            ByteBuffer explicitBuffer = ByteBuffer.allocate(200);
            int bytesRead = reader.readRange(LARGE_BLOCK_SIZE - 100, 200, explicitBuffer);
            explicitBuffer.flip();

            assertEquals(200, bytesRead, "Should read 200 bytes");
            assertEquals(0, explicitBuffer.position(), "Position should be 0");
            assertEquals(200, explicitBuffer.limit(), "Limit should be 200");
        }
    }

    /**
     * Tests combined custom block size and caching.
     */
    @Test
    public void testCustomBlockSizeAndCaching() throws IOException {
        try (RangeReader reader = createCustomBlockSizeCachingReader(LARGE_BLOCK_SIZE)) {
            // Read a range that crosses a block boundary
            ByteBuffer buffer1 = reader.readRange(LARGE_BLOCK_SIZE - 100, 200).flip();
            assertEquals(200, buffer1.remaining(), "Should read 200 bytes");

            // Read a subset of the previously read range - should be cached
            ByteBuffer buffer2 = reader.readRange(LARGE_BLOCK_SIZE - 50, 100).flip();
            assertEquals(100, buffer2.remaining(), "Should read 100 bytes");

            // Read the exact same range again
            ByteBuffer buffer3 = reader.readRange(LARGE_BLOCK_SIZE - 50, 100).flip();
            assertEquals(100, buffer3.remaining(), "Should read 100 bytes again");

            // Test with explicit buffer
            ByteBuffer explicitBuffer = ByteBuffer.allocate(100);
            int bytesRead = reader.readRange(LARGE_BLOCK_SIZE - 50, 100, explicitBuffer);
            explicitBuffer.flip();
            assertEquals(100, bytesRead, "Should read 100 bytes");

            // Get data from third buffer
            byte[] data3 = new byte[100];
            buffer3.duplicate().get(data3);

            // Get data from explicit buffer
            byte[] explicitData = new byte[100];
            explicitBuffer.get(explicitData);

            assertArrayEquals(data3, explicitData, "Explicit buffer data should match cached data");
        }
    }

    /**
     * Tests randomized reads to ensure general correctness.
     */
    @Test
    public void testRandomizedReads() throws IOException {
        try (RangeReader reader = createBlockAlignedCachingReader()) {
            // Perform random reads
            Random random = new Random(42); // Fixed seed for reproducibility

            // First, get reference data directly from the base reader
            try (RangeReader baseReader = createBaseReader()) {
                for (int i = 0; i < 20; i++) {
                    // Random offset up to FILE_SIZE - 1000
                    int offset = random.nextInt(TEST_FILE_SIZE - 1000);
                    // Random length between 1 and 1000
                    int length = random.nextInt(1000) + 1;

                    // Read from decorated reader
                    ByteBuffer decoratedBuffer =
                            reader.readRange(offset, length).flip();
                    assertEquals(
                            length,
                            decoratedBuffer.remaining(),
                            "Should read " + length + " bytes from offset " + offset);

                    // Read from base reader for comparison
                    ByteBuffer baseBuffer = baseReader.readRange(offset, length).flip();
                    assertEquals(
                            length,
                            baseBuffer.remaining(),
                            "Base reader should read " + length + " bytes from offset " + offset);

                    // Compare data
                    byte[] decoratedData = new byte[length];
                    byte[] baseData = new byte[length];
                    decoratedBuffer.get(decoratedData);
                    baseBuffer.get(baseData);

                    assertArrayEquals(
                            baseData,
                            decoratedData,
                            "Data from decorated reader should match base reader at offset " + offset);

                    // Test with explicit buffer too
                    ByteBuffer explicitBuffer = ByteBuffer.allocate(length);
                    int bytesRead = reader.readRange(offset, length, explicitBuffer);

                    assertEquals(length, bytesRead, "Should read " + length + " bytes");

                    byte[] explicitData = new byte[length];
                    explicitBuffer.flip().get(explicitData);

                    assertArrayEquals(
                            baseData,
                            explicitData,
                            "Explicit buffer data should match base reader at offset " + offset);
                }
            }
        }
    }
}
