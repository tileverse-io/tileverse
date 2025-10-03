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
package io.tileverse.rangereader.adapters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.rangereader.file.FileRangeReader;
import io.tileverse.rangereader.it.TestUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeReaderReadableByteChannelTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private byte[] testData;
    private static final int TEST_FILE_SIZE = 2048; // 2KB test file

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-data.bin");
        TestUtil.createMockTestFile(testFile, TEST_FILE_SIZE);
        testData = Files.readAllBytes(testFile);
    }

    @Test
    void of_withNullRangeReader_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> RangeReaderReadableByteChannel.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("rangeReader cannot be null");
    }

    @Test
    void isOpen_initiallyTrue() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    void position_initiallyZero() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            assertThat(channel.position()).isEqualTo(0L);
        }
    }

    @Test
    void size_returnsCorrectFileSize() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            assertThat(channel.size()).isEqualTo(TEST_FILE_SIZE);
        }
    }

    @Test
    void read_withValidBuffer_readsSequentially() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            // Read first 100 bytes
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = channel.read(buffer);

            assertThat(bytesRead).isEqualTo(100);
            assertThat(channel.position()).isEqualTo(100L);

            // Verify data integrity - buffer is already positioned correctly after readRange
            byte[] readData = new byte[bytesRead];
            buffer.flip().get(readData);

            byte[] expectedData = new byte[100];
            System.arraycopy(testData, 0, expectedData, 0, 100);
            assertThat(readData).isEqualTo(expectedData);
        }
    }

    @Test
    void read_withNullBuffer_throwsNullPointerException() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            assertThatThrownBy(() -> channel.read(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("destination buffer cannot be null");
        }
    }

    @Test
    void read_withEmptyBuffer_returnsZero() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
            int bytesRead = channel.read(emptyBuffer);
            assertThat(bytesRead).isEqualTo(0);
            assertThat(channel.position()).isEqualTo(0L);
        }
    }

    @Test
    void read_atEndOfFile_returnsMinusOne() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            // Read entire file first
            ByteBuffer largeBuffer = ByteBuffer.allocate(TEST_FILE_SIZE);
            int totalBytesRead = channel.read(largeBuffer);
            assertThat(totalBytesRead).isEqualTo(TEST_FILE_SIZE);
            assertThat(channel.position()).isEqualTo(TEST_FILE_SIZE);

            // Try to read beyond end
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = channel.read(buffer);
            assertThat(bytesRead).isEqualTo(-1);
        }
    }

    @Test
    void read_partialReadAtEndOfFile_readsAvailableBytes() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            // Read almost to the end, leaving 50 bytes
            ByteBuffer largeBuffer = ByteBuffer.allocate(TEST_FILE_SIZE - 50);
            int bytesRead1 = channel.read(largeBuffer);
            assertThat(bytesRead1).isEqualTo(TEST_FILE_SIZE - 50);
            assertThat(channel.position()).isEqualTo(TEST_FILE_SIZE - 50);

            // Try to read 100 bytes when only 50 are available
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead2 = channel.read(buffer);
            assertThat(bytesRead2).isEqualTo(50); // Only 50 bytes available
            assertThat(channel.position()).isEqualTo(TEST_FILE_SIZE);
        }
    }

    @Test
    void read_sequentialReads_advancePosition() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            // Read in chunks
            int chunkSize = 256;
            int totalBytesRead = 0;
            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);

            while (true) {
                buffer.clear();
                int bytesRead = channel.read(buffer);

                if (bytesRead == -1) {
                    break; // End of file
                }

                totalBytesRead += bytesRead;
                assertThat(channel.position()).isEqualTo(totalBytesRead);

                // Verify data integrity for this chunk - buffer is already positioned correctly
                byte[] chunkData = new byte[bytesRead];
                buffer.flip().get(chunkData);

                byte[] expectedChunk = new byte[bytesRead];
                System.arraycopy(testData, totalBytesRead - bytesRead, expectedChunk, 0, bytesRead);
                assertThat(chunkData).isEqualTo(expectedChunk);
            }

            assertThat(totalBytesRead).isEqualTo(TEST_FILE_SIZE);
        }
    }

    @Test
    void close_closesChannel() throws IOException {
        FileRangeReader reader = FileRangeReader.of(testFile);
        RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader);

        assertThat(channel.isOpen()).isTrue();

        channel.close();
        assertThat(channel.isOpen()).isFalse();

        // Note: Channel doesn't close the underlying reader based on our design
        // The reader should still be usable
        assertThat(reader.size().getAsLong()).isEqualTo(TEST_FILE_SIZE);
        reader.close(); // Clean up
    }

    @Test
    void close_multipleCalls_doesNotThrowException() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            channel.close();
            channel.close(); // Should not throw exception
            assertThat(channel.isOpen()).isFalse();
        }
    }

    @Test
    void operationsAfterClose_throwClosedChannelException() throws IOException {
        FileRangeReader reader = FileRangeReader.of(testFile);
        RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader);
        channel.close();

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(10))).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.position()).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.size()).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.getRangeReader()).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.getSourceIdentifier()).isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void getRangeReader_returnsUnderlyingReader() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            assertThat(channel.getRangeReader()).isSameAs(reader);
        }
    }

    @Test
    void getSourceIdentifier_returnsFileIdentifier() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            String identifier = channel.getSourceIdentifier();
            assertThat(identifier).contains(testFile.toString());
            assertThat(identifier).isEqualTo(reader.getSourceIdentifier());
        }
    }

    @Test
    void toString_openChannel_includesDetails() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            String result = channel.toString();
            assertThat(result)
                    .contains("RangeReaderReadableByteChannel")
                    .contains("position=0")
                    .contains("size=" + TEST_FILE_SIZE);
        }
    }

    @Test
    void toString_closedChannel_indicatesClosed() throws IOException {
        FileRangeReader reader = FileRangeReader.of(testFile);
        RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader);
        channel.close();

        String result = channel.toString();
        assertThat(result).contains("RangeReaderReadableByteChannel[closed]");
    }

    @Test
    void integrationWithJavaNIOAPIs() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            // Use as ReadableByteChannel interface
            ReadableByteChannel readableChannel = channel;
            ByteBuffer buffer = ByteBuffer.allocate(127); // Read the header

            int bytesRead = readableChannel.read(buffer);
            assertThat(bytesRead).isEqualTo(127);

            // Verify we read the magic header - buffer is already positioned correctly
            byte[] headerBytes = new byte[7];
            buffer.flip().get(headerBytes);
            assertThat(new String(headerBytes)).isEqualTo("TstFile");
        }
    }

    @Test
    void largeFileReading_performance() throws IOException {
        // Create a larger test file (1MB)
        Path largeFile = tempDir.resolve("large-test-data.bin");
        TestUtil.createMockTestFile(largeFile, 1024 * 1024);

        try (FileRangeReader reader = FileRangeReader.of(largeFile);
                RangeReaderReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {

            assertThat(channel.size()).isEqualTo(1024 * 1024L);

            // Read in 64KB chunks
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            long totalBytesRead = 0;
            int readCount = 0;

            while (true) {
                buffer.clear();
                int bytesRead = channel.read(buffer);

                if (bytesRead == -1) {
                    break;
                }

                totalBytesRead += bytesRead;
                readCount++;
                assertThat(channel.position()).isEqualTo(totalBytesRead);
            }

            assertThat(totalBytesRead).isEqualTo(1024 * 1024L);
            assertThat(readCount).isEqualTo(16); // 1MB / 64KB = 16 chunks
        }
    }
}
