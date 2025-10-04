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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tileverse.rangereader.block.BlockAlignedRangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for BlockAlignedRangeReader.
 */
class BlockAlignedRangeReaderTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private String textContent;
    private FileRangeReader fileReader;
    private BlockAlignedRangeReader reader;

    // Use a small block size for testing
    private static final int TEST_BLOCK_SIZE = 16;

    ByteBuffer buffer;

    @BeforeEach
    void setUp() throws IOException {
        buffer = ByteBuffer.allocate(2 * TEST_BLOCK_SIZE);
        // Create a test file with known content
        testFile = tempDir.resolve("block-test.txt");
        textContent = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        Files.writeString(testFile, textContent, StandardOpenOption.CREATE);

        // Initialize the readers
        fileReader = FileRangeReader.of(testFile);
        reader = new BlockAlignedRangeReader(fileReader, TEST_BLOCK_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    @Test
    void testConstructorWithNegativeBlockSize() {
        assertThrows(IllegalArgumentException.class, () -> new BlockAlignedRangeReader(fileReader, -1));
    }

    @Test
    void testConstructorWithZeroBlockSize() {
        assertThrows(IllegalArgumentException.class, () -> new BlockAlignedRangeReader(fileReader, 0));
    }

    @Test
    void testConstructorWithNonPowerOfTwoBlockSize() {
        assertThrows(IllegalArgumentException.class, () -> new BlockAlignedRangeReader(fileReader, 15));
    }

    @Test
    void testReadAlignedRange() throws IOException {
        // Read a range that's already aligned to block boundaries
        reader.readRange(16, 16, buffer);
        buffer.flip();
        assertEquals(16, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(16, 32), result);
    }

    @Test
    void testReadUnalignedOffset() throws IOException {
        // Read from an offset that's not aligned
        reader.readRange(10, 10, buffer);
        buffer.flip();

        assertEquals(10, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(10, 20), result);
    }

    @Test
    void testReadAcrossMultipleBlocks() throws IOException {
        // Read a range that spans multiple blocks
        reader.readRange(10, 30, buffer);
        buffer.flip();
        assertEquals(30, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(10, 40), result);
    }

    @Test
    void testReadPartialBlock() throws IOException {
        // Read a small part from the middle of a block
        reader.readRange(20, 5, buffer);
        buffer.flip();
        assertEquals(5, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(20, 25), result);
    }

    @Test
    void testReadBeyondEnd() throws IOException {
        // Try to read beyond the end of the file
        reader.readRange(textContent.length() - 5, 10, buffer);
        buffer.flip();
        assertEquals(5, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(textContent.length() - 5), result);
    }

    @Test
    void testReadStartingBeyondEnd() throws IOException {
        // Try to read from an offset beyond the end of the file
        reader.readRange(textContent.length() + 10, 5, buffer);
        buffer.flip();
        assertEquals(0, buffer.remaining());
    }

    @Test
    void testReadWithZeroLength() throws IOException {
        // Try to read with a length of 0
        reader.readRange(10, 0, buffer);
        buffer.flip();
        assertEquals(0, buffer.remaining());
    }

    @Test
    void testReadWithNegativeOffset() {
        // Try to read with a negative offset
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(-1, 10, buffer));
    }

    @Test
    void testReadWithNegativeLength() {
        // Try to read with a negative length
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, -1, buffer));
    }

    @Test
    void testDefaultBlockSize() throws IOException {
        // Check that the default constructor sets the correct block size
        try (BlockAlignedRangeReader defaultReader = new BlockAlignedRangeReader(fileReader)) {
            assertEquals(BlockAlignedRangeReader.DEFAULT_BLOCK_SIZE, defaultReader.getBlockSize());
        }
    }

    @Test
    void testGetBlockSize() {
        // Check that getBlockSize returns the correct value
        assertEquals(TEST_BLOCK_SIZE, reader.getBlockSize());
    }

    @Test
    void testDelegateSize() throws IOException {
        // Check that size() delegates to the underlying reader
        assertEquals(textContent.length(), reader.size().getAsLong());
    }

    @Test
    void testReadWithOffsetInBuffer() throws IOException {
        // Test reading with offset in the buffer
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.position(5); // Start at position 5

        int bytesRead = reader.readRange(16, 10, buffer);

        // Verify returned byte count
        assertEquals(10, bytesRead);

        // Verify buffer is ready for reading
        assertEquals(15, buffer.position());
        assertEquals(20, buffer.limit());

        buffer.flip().position(5);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(16, 26), result);
    }

    @Test
    void testReadWithNegativeExplicitBuffer() {
        // Try to read with a negative length with explicit buffer
        ByteBuffer buffer = ByteBuffer.allocate(10);
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, -1, buffer));
    }

    @Test
    void testReadWithNullBuffer() {
        // Try to read with a null buffer
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, 10, null));
    }

    @Test
    void testReadWithInsufficientBufferCapacity() {
        // Try to read with a buffer that has insufficient capacity
        ByteBuffer buffer = ByteBuffer.allocate(5);
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, 10, buffer));
    }
}
