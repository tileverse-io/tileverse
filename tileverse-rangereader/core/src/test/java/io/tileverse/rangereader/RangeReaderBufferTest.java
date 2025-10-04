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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.cache.DiskCachingRangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the ByteBuffer-reusing API of RangeReader implementations.
 * <p>
 * These tests verify that the {@code int readRange(long offset, int length, ByteBuffer target)}
 * method correctly returns the number of bytes read and leaves the target buffer in a readable
 * state without requiring the caller to flip it.
 */
class RangeReaderBufferTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private String textContent;
    private byte[] binaryContent;
    private static final int BINARY_SIZE = 100_000; // 100 KB

    @BeforeEach
    void setUp() throws IOException {
        // Create a text file for simple tests
        textContent = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        testFile = tempDir.resolve("test.txt");
        Files.writeString(testFile, textContent, StandardOpenOption.CREATE);

        // Create a binary file for more realistic testing
        Path binaryFile = tempDir.resolve("test.bin");
        binaryContent = new byte[BINARY_SIZE];
        new Random(42).nextBytes(binaryContent); // Use fixed seed for reproducibility
        Files.write(binaryFile, binaryContent);
    }

    /**
     * Helper method to verify that a ByteBuffer is ready for reading and contains the expected data.
     *
     * @param buffer The buffer to check
     * @param expectedBytes The expected bytes
     * @param bytesRead The expected number of bytes read
     */
    private void verifyBuffer(ByteBuffer buffer, byte[] expectedBytes, int bytesRead) {
        // Verify the buffer state
        assertEquals(0, buffer.position(), "Buffer position should be 0");
        assertEquals(bytesRead, buffer.limit(), "Buffer limit should equal bytesRead");
        assertEquals(bytesRead, buffer.remaining(), "Buffer remaining should equal bytesRead");

        // Verify the buffer contents
        byte[] actualBytes = new byte[buffer.remaining()];
        buffer.get(actualBytes);
        assertArrayEquals(expectedBytes, actualBytes, "Buffer contents should match expected data");

        // Verify buffer is fully read
        assertEquals(bytesRead, buffer.position(), "Buffer position should be at the end after reading");
        assertEquals(0, buffer.remaining(), "Buffer should have no remaining bytes after reading");
    }

    @Test
    void testFileRangeReader_TargetBuffer() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test a full read
            ByteBuffer target = ByteBuffer.allocate(textContent.length());
            int bytesRead = reader.readRange(0, textContent.length(), target);
            target.flip();

            // Verify read count
            assertEquals(textContent.length(), bytesRead, "Should have read the full content length");

            // Verify buffer state and contents
            verifyBuffer(target, textContent.getBytes(StandardCharsets.UTF_8), bytesRead);

            // Test a partial read
            ByteBuffer partialTarget = ByteBuffer.allocate(20);
            int partialOffset = 10;
            int partialLength = 10;
            int partialBytesRead = reader.readRange(partialOffset, partialLength, partialTarget);
            partialTarget.flip();

            // Verify read count
            assertEquals(partialLength, partialBytesRead, "Should have read the requested length");

            // Verify buffer state and contents
            byte[] expectedBytes = textContent
                    .substring(partialOffset, partialOffset + partialLength)
                    .getBytes(StandardCharsets.UTF_8);
            verifyBuffer(partialTarget, expectedBytes, partialBytesRead);
        }
    }

    @Test
    void testFileRangeReader_TargetBuffer_EOFHandling() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test a read that goes beyond EOF
            int beyondEOFOffset = textContent.length() - 5;
            int beyondEOFLength = 10; // Only 5 bytes are available
            ByteBuffer beyondEOFTarget = ByteBuffer.allocate(beyondEOFLength);

            int beyondEOFBytesRead = reader.readRange(beyondEOFOffset, beyondEOFLength, beyondEOFTarget);
            beyondEOFTarget.flip();

            // Verify read count (should be truncated to available bytes)
            assertEquals(5, beyondEOFBytesRead, "Should have read only available bytes");

            // Verify buffer state and contents
            byte[] expectedBytes = textContent.substring(beyondEOFOffset).getBytes(StandardCharsets.UTF_8);
            verifyBuffer(beyondEOFTarget, expectedBytes, beyondEOFBytesRead);

            // Test a read entirely beyond EOF
            ByteBuffer pastEOFTarget = ByteBuffer.allocate(10);
            int pastEOFBytesRead = reader.readRange(textContent.length() + 10, 10, pastEOFTarget);
            pastEOFTarget.flip();

            // Verify read count (should be 0)
            assertEquals(0, pastEOFBytesRead, "Should have read 0 bytes");

            // Verify buffer state
            assertEquals(0, pastEOFTarget.position(), "Buffer position should be 0");
            assertEquals(0, pastEOFTarget.limit(), "Buffer limit should have changed to read zero bytes");
        }
    }

    @Test
    void testFileRangeReader_TargetBuffer_LargerBuffer() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test with a buffer larger than needed
            ByteBuffer largeTarget = ByteBuffer.allocate(textContent.length() * 2);
            int offset = 5;
            int length = 10;

            int bytesRead = reader.readRange(offset, length, largeTarget);
            largeTarget.flip();

            // Verify read count
            assertEquals(length, bytesRead, "Should have read requested length");

            // Verify buffer state and contents
            byte[] expectedBytes =
                    textContent.substring(offset, offset + length).getBytes(StandardCharsets.UTF_8);

            // Buffer should be positioned at 0 and limited to bytesRead
            assertEquals(0, largeTarget.position(), "Buffer position should be 0");
            assertEquals(bytesRead, largeTarget.limit(), "Buffer limit should be bytesRead");

            // Read the data
            byte[] actualBytes = new byte[largeTarget.remaining()];
            largeTarget.get(actualBytes);
            assertArrayEquals(expectedBytes, actualBytes, "Buffer contents should match expected data");
        }
    }

    @Test
    void testFileRangeReader_TargetBuffer_ExistingPosition() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test with a buffer that already has data (position > 0)
            ByteBuffer target = ByteBuffer.allocate(textContent.length());

            // First, put some data in the buffer
            String prefix = "PREFIX_";
            target.put(prefix.getBytes(StandardCharsets.UTF_8));
            int initialPosition = target.position();

            // Now read into the same buffer at current position
            int offset = 5;
            int length = 10;

            int bytesRead = reader.readRange(offset, length, target);

            // Verify read count
            assertEquals(length, bytesRead, "Should have read requested length");

            // With NIO conventions: position advances, limit stays at capacity
            assertEquals(
                    initialPosition + bytesRead,
                    target.position(),
                    "Buffer position should be initial position + bytes read");
            assertEquals(target.capacity(), target.limit(), "Buffer limit should remain at capacity");

            // Now flip to prepare for reading
            target.flip();

            // After flip: position becomes 0, limit becomes the total data written
            assertEquals(0, target.position(), "After flip, position should be 0");
            assertEquals(
                    initialPosition + bytesRead,
                    target.limit(),
                    "After flip, limit should be initial position + bytes read");

            // Read and verify the content (prefix + read data)
            byte[] expectedContent =
                    (prefix + textContent.substring(offset, offset + length)).getBytes(StandardCharsets.UTF_8);
            byte[] actualContent = new byte[target.remaining()];
            target.get(actualContent);
            assertArrayEquals(expectedContent, actualContent, "Buffer should contain prefix + read data");
        }
    }

    @Test
    void testBlockAlignedRangeReader_TargetBuffer() throws IOException {
        try (FileRangeReader baseReader = FileRangeReader.of(testFile);
                BlockAlignedRangeReader reader = new BlockAlignedRangeReader(baseReader, 16)) {

            // Test a read that spans multiple blocks
            int offset = 8; // In the middle of the first block
            int length = 24; // Spans into the third block
            ByteBuffer target = ByteBuffer.allocate(length);

            int bytesRead = reader.readRange(offset, length, target);
            target.flip(); // Flip to prepare for reading

            // Verify read count
            assertEquals(length, bytesRead, "Should have read requested length");

            // Verify buffer state and contents
            byte[] expectedBytes =
                    textContent.substring(offset, offset + length).getBytes(StandardCharsets.UTF_8);
            verifyBuffer(target, expectedBytes, bytesRead);
        }
    }

    @Test
    void testCachingRangeReader_TargetBuffer() throws IOException {
        try (FileRangeReader baseReader = FileRangeReader.of(testFile);
                CachingRangeReader reader =
                        CachingRangeReader.builder(baseReader).build()) {

            // First read to populate cache
            int offset = 10;
            int length = 15;
            ByteBuffer target1 = ByteBuffer.allocate(length);

            int bytesRead1 = reader.readRange(offset, length, target1);
            target1.flip();

            // Verify first read
            assertEquals(length, bytesRead1, "First read should return requested length");
            byte[] expectedBytes =
                    textContent.substring(offset, offset + length).getBytes(StandardCharsets.UTF_8);
            verifyBuffer(target1, expectedBytes, bytesRead1);

            // Second read of same range (from cache)
            ByteBuffer target2 = ByteBuffer.allocate(length);
            int bytesRead2 = reader.readRange(offset, length, target2);
            target2.flip();

            // Verify second read
            assertEquals(length, bytesRead2, "Second read should return same length");
            verifyBuffer(target2, expectedBytes, bytesRead2);
        }
    }

    @Test
    void testDiskCachingRangeReader_TargetBuffer() throws IOException {
        Path cacheDir = tempDir.resolve("cache");
        Files.createDirectories(cacheDir);

        try (FileRangeReader baseReader = FileRangeReader.of(testFile);
                DiskCachingRangeReader reader = DiskCachingRangeReader.builder(baseReader)
                        .cacheDirectory(cacheDir)
                        .build()) {

            // First read to populate cache
            int offset = 15;
            int length = 20;
            ByteBuffer target1 = ByteBuffer.allocate(length);

            int bytesRead1 = reader.readRange(offset, length, target1);
            target1.flip();

            // Verify first read
            assertEquals(length, bytesRead1, "First read should return requested length");
            byte[] expectedBytes =
                    textContent.substring(offset, offset + length).getBytes(StandardCharsets.UTF_8);
            verifyBuffer(target1, expectedBytes, bytesRead1);

            // Second read of same range (from cache)
            ByteBuffer target2 = ByteBuffer.allocate(length);
            int bytesRead2 = reader.readRange(offset, length, target2);
            target2.flip();

            // Verify second read
            assertEquals(length, bytesRead2, "Second read should return same length");
            verifyBuffer(target2, expectedBytes, bytesRead2);
        }
    }

    @Test
    void testDirectBuffer() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test with a direct buffer
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(textContent.length());

            int bytesRead = reader.readRange(0, textContent.length(), directBuffer);
            directBuffer.flip();

            // Verify read count
            assertEquals(textContent.length(), bytesRead, "Should have read the full content length");

            // Verify buffer state
            assertEquals(0, directBuffer.position(), "Buffer position should be 0");
            assertEquals(bytesRead, directBuffer.limit(), "Buffer limit should be bytes read");

            // Read and verify content
            byte[] actualBytes = new byte[directBuffer.remaining()];
            directBuffer.get(actualBytes);
            assertArrayEquals(
                    textContent.getBytes(StandardCharsets.UTF_8),
                    actualBytes,
                    "Direct buffer contents should match expected data");
        }
    }

    @Test
    void testMultipleConsecutiveReads() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Single buffer for multiple reads
            ByteBuffer target = ByteBuffer.allocate(textContent.length());

            // First read
            int offset1 = 0;
            int length1 = 10;
            int bytesRead1 = reader.readRange(offset1, length1, target);
            target.flip();

            // Verify first read
            assertEquals(length1, bytesRead1, "First read should return requested length");
            assertEquals(0, target.position(), "Buffer position should be 0 after first read");
            assertEquals(bytesRead1, target.limit(), "Buffer limit should be bytes read after first read");

            // Read and store the first content
            byte[] content1 = new byte[target.remaining()];
            target.get(content1);

            // Clear buffer for second read
            target.clear();

            // Second read
            int offset2 = 10;
            int length2 = 15;
            int bytesRead2 = reader.readRange(offset2, length2, target);
            target.flip();

            // Verify second read
            assertEquals(length2, bytesRead2, "Second read should return requested length");
            assertEquals(0, target.position(), "Buffer position should be 0 after second read");
            assertEquals(bytesRead2, target.limit(), "Buffer limit should be bytes read after second read");

            // Read and verify the second content
            byte[] content2 = new byte[target.remaining()];
            target.get(content2);

            // Verify content
            assertArrayEquals(
                    textContent.substring(offset1, offset1 + length1).getBytes(StandardCharsets.UTF_8),
                    content1,
                    "First read content should match expected");
            assertArrayEquals(
                    textContent.substring(offset2, offset2 + length2).getBytes(StandardCharsets.UTF_8),
                    content2,
                    "Second read content should match expected");
        }
    }

    @Test
    void testMultiLevelNestedReaders_TargetBuffer() throws IOException {
        // Create a multi-level nested reader setup
        FileRangeReader baseReader = FileRangeReader.of(testFile);
        BlockAlignedRangeReader blockReader = new BlockAlignedRangeReader(baseReader, 16);
        CachingRangeReader cachingReader =
                CachingRangeReader.builder(blockReader).build();

        try (RangeReader reader = cachingReader) {
            // Test with nested readers
            int offset = 12;
            int length = 20;
            ByteBuffer target = ByteBuffer.allocate(length);

            int bytesRead = reader.readRange(offset, length, target);
            target.flip();

            // Verify read count
            assertEquals(length, bytesRead, "Should have read requested length");

            // Verify buffer state and contents
            byte[] expectedBytes =
                    textContent.substring(offset, offset + length).getBytes(StandardCharsets.UTF_8);
            verifyBuffer(target, expectedBytes, bytesRead);
        }
    }

    @Test
    void testZeroLengthRead() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test a zero-length read
            ByteBuffer target = ByteBuffer.allocate(10);

            // Position buffer at index 2 to test that position is maintained
            target.position(2);
            int originalPosition = target.position();
            int originalLimit = target.limit();

            int bytesRead = reader.readRange(5, 0, target);

            // Verify read count
            assertEquals(0, bytesRead, "Zero-length read should return 0 bytes");

            // With NIO conventions: for zero-length reads, position and limit should remain unchanged
            assertEquals(
                    originalPosition, target.position(), "Buffer position should be unchanged for zero-length read");
            assertEquals(originalLimit, target.limit(), "Buffer limit should be unchanged for zero-length read");

            // Now flip to see the effect
            target.flip();

            // After flip with zero bytes written: position becomes 0, limit becomes original position
            assertEquals(0, target.position(), "After flip, position should be 0");
            assertEquals(originalPosition, target.limit(), "After flip, limit should be original position");
        }
    }

    @Test
    void testReadIntoExistingData() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Create a buffer and pre-fill it with some data
            ByteBuffer target = ByteBuffer.allocate(100);
            String prefix = "PREFIX_DATA_";
            target.put(prefix.getBytes(StandardCharsets.UTF_8));
            int initialPosition = target.position();

            // Read into the buffer at current position
            int offset = 5;
            int length = 15;
            int bytesRead = reader.readRange(offset, length, target);

            // Verify read count
            assertEquals(length, bytesRead, "Should have read requested length");

            // With NIO conventions: position should be advanced by bytes written, limit unchanged
            assertEquals(
                    initialPosition + bytesRead, target.position(), "Position should be initial position + bytes read");
            assertEquals(target.capacity(), target.limit(), "Limit should remain at capacity");

            // Now flip to prepare for reading
            target.flip();

            // After flip: position should be 0, limit should be the total data length
            assertEquals(0, target.position(), "After flip, position should be 0");
            assertEquals(
                    initialPosition + bytesRead,
                    target.limit(),
                    "After flip, limit should be initial position + bytes read");

            // Read and verify the content
            byte[] expectedContent =
                    (prefix + textContent.substring(offset, offset + length)).getBytes(StandardCharsets.UTF_8);
            byte[] actualContent = new byte[target.remaining()];
            target.get(actualContent);
            assertArrayEquals(expectedContent, actualContent, "Buffer should contain prefix + read data");
        }
    }

    @Test
    void testLargeBufferReads() throws IOException {
        // Read from the binary file for large data testing
        Path binaryFile = tempDir.resolve("test.bin");

        try (FileRangeReader reader = FileRangeReader.of(binaryFile)) {
            // Test reading in chunks of various sizes
            int[] chunkSizes = {1024, 4096, 8192, 16384, 32768};

            for (int chunkSize : chunkSizes) {
                // Read a chunk from the middle of the file
                int offset = BINARY_SIZE / 2 - chunkSize / 2;
                ByteBuffer target = ByteBuffer.allocate(chunkSize);

                int bytesRead = reader.readRange(offset, chunkSize, target);
                target.flip();

                // Verify read count
                assertEquals(chunkSize, bytesRead, String.format("Should have read %d bytes", chunkSize));

                // Verify buffer state
                assertEquals(0, target.position(), "Buffer position should be 0");
                assertEquals(bytesRead, target.limit(), "Buffer limit should be bytes read");

                // Read and verify content
                byte[] expectedChunk = new byte[chunkSize];
                System.arraycopy(binaryContent, offset, expectedChunk, 0, chunkSize);

                byte[] actualChunk = new byte[target.remaining()];
                target.get(actualChunk);
                assertArrayEquals(
                        expectedChunk,
                        actualChunk,
                        String.format("Buffer for chunk size %d should match expected data", chunkSize));
            }
        }
    }

    @Test
    void testExceptionHandlingForInvalidParameters() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile)) {
            // Test with negative offset
            ByteBuffer target = ByteBuffer.allocate(10);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        reader.readRange(-1, 10, target);
                    },
                    "Should throw exception for negative offset");

            // Test with negative length
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        reader.readRange(0, -5, target);
                    },
                    "Should throw exception for negative length");

            // Test with null buffer
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        reader.readRange(0, 10, null);
                    },
                    "Should throw exception for null buffer");

            // Test with read-only buffer
            ByteBuffer readOnlyBuffer = ByteBuffer.allocate(10).asReadOnlyBuffer();
            assertThrows(
                    java.nio.ReadOnlyBufferException.class,
                    () -> {
                        reader.readRange(0, 10, readOnlyBuffer);
                    },
                    "Should throw exception for read-only buffer");

            // Test with insufficient buffer capacity
            ByteBuffer smallBuffer = ByteBuffer.allocate(5);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        reader.readRange(0, 10, smallBuffer);
                    },
                    "Should throw exception for insufficient buffer capacity");
        }
    }
}
