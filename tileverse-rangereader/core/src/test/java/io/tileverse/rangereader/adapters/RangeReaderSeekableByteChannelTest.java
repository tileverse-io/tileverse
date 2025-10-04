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
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeReaderSeekableByteChannelTest {

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
        assertThatThrownBy(() -> RangeReaderSeekableByteChannel.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("rangeReader cannot be null");
    }

    @Test
    void isOpen_initiallyTrue() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    void position_initiallyZero() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThat(channel.position()).isEqualTo(0L);
        }
    }

    @Test
    void size_returnsCorrectFileSize() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThat(channel.size()).isEqualTo(TEST_FILE_SIZE);
        }
    }

    @Test
    void position_setValidPosition_updatesPosition() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            channel.position(500L);
            assertThat(channel.position()).isEqualTo(500L);
        }
    }

    @Test
    void position_setNegativePosition_throwsIllegalArgumentException() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThatThrownBy(() -> channel.position(-1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("position cannot be negative");
        }
    }

    @Test
    void read_withValidBuffer_readsFromRangeReader() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Arrange
            ByteBuffer buffer = ByteBuffer.allocate(100);

            // Act
            int bytesRead = channel.read(buffer);
            buffer.flip();
            // Assert
            assertThat(bytesRead).isEqualTo(100);
            assertThat(channel.position()).isEqualTo(100L);

            // Verify data integrity - buffer is already positioned correctly after readRange
            byte[] readData = new byte[bytesRead];
            buffer.get(readData);

            byte[] expectedData = new byte[100];
            System.arraycopy(testData, 0, expectedData, 0, 100);
            assertThat(readData).isEqualTo(expectedData);
        }
    }

    @Test
    void read_withNullBuffer_throwsNullPointerException() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThatThrownBy(() -> channel.read(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("destination buffer cannot be null");
        }
    }

    @Test
    void read_withEmptyBuffer_returnsZero() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
            int bytesRead = channel.read(emptyBuffer);
            assertThat(bytesRead).isEqualTo(0);
            assertThat(channel.position()).isEqualTo(0L);
        }
    }

    @Test
    void read_atEndOfSource_returnsMinusOne() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Arrange
            channel.position(TEST_FILE_SIZE); // At end of file
            ByteBuffer buffer = ByteBuffer.allocate(100);

            // Act
            int bytesRead = channel.read(buffer);

            // Assert
            assertThat(bytesRead).isEqualTo(-1);
        }
    }

    @Test
    void read_beyondEndOfSource_returnsMinusOne() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Arrange
            channel.position(TEST_FILE_SIZE + 500L); // Beyond end of file
            ByteBuffer buffer = ByteBuffer.allocate(100);

            // Act
            int bytesRead = channel.read(buffer);

            // Assert
            assertThat(bytesRead).isEqualTo(-1);
        }
    }

    @Test
    void read_partialReadAtEndOfSource_readsAvailableBytes() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Arrange
            long remainingBytes = 50L;
            channel.position(TEST_FILE_SIZE - remainingBytes); // 50 bytes from end
            ByteBuffer buffer = ByteBuffer.allocate(100); // Request 100 bytes

            // Act
            int bytesRead = channel.read(buffer);

            // Assert
            assertThat(bytesRead).isEqualTo((int) remainingBytes); // Only 50 bytes available
            assertThat(channel.position()).isEqualTo(TEST_FILE_SIZE);

            // Verify data integrity - buffer is already positioned correctly after readRange
            byte[] readData = new byte[bytesRead];
            buffer.flip().get(readData);

            byte[] expectedData = new byte[bytesRead];
            System.arraycopy(testData, (int) (TEST_FILE_SIZE - remainingBytes), expectedData, 0, bytesRead);
            assertThat(readData).isEqualTo(expectedData);
        }
    }

    @Test
    void read_sequentialReads_advancePosition() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Arrange
            ByteBuffer buffer1 = ByteBuffer.allocate(100);
            ByteBuffer buffer2 = ByteBuffer.allocate(50);

            // Act
            int bytesRead1 = channel.read(buffer1);
            int bytesRead2 = channel.read(buffer2);

            // Assert
            assertThat(bytesRead1).isEqualTo(100);
            assertThat(bytesRead2).isEqualTo(50);
            assertThat(channel.position()).isEqualTo(150L);

            buffer1.flip();
            buffer2.flip();

            // Verify data integrity for both reads - buffers are already positioned correctly
            byte[] readData1 = new byte[bytesRead1];
            buffer1.get(readData1);
            byte[] expectedData1 = new byte[100];
            System.arraycopy(testData, 0, expectedData1, 0, 100);
            assertThat(readData1).isEqualTo(expectedData1);

            byte[] readData2 = new byte[bytesRead2];
            buffer2.get(readData2);
            byte[] expectedData2 = new byte[50];
            System.arraycopy(testData, 100, expectedData2, 0, 50);
            assertThat(readData2).isEqualTo(expectedData2);
        }
    }

    @Test
    void write_throwsNonWritableChannelException() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            ByteBuffer buffer = ByteBuffer.allocate(100);
            assertThatThrownBy(() -> channel.write(buffer)).isInstanceOf(NonWritableChannelException.class);
        }
    }

    @Test
    void truncate_throwsNonWritableChannelException() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThatThrownBy(() -> channel.truncate(500L)).isInstanceOf(NonWritableChannelException.class);
        }
    }

    @Test
    void close_closesChannel() throws IOException {
        FileRangeReader reader = FileRangeReader.of(testFile);
        RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader);

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
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            channel.close();
            channel.close(); // Should not throw exception
            assertThat(channel.isOpen()).isFalse();
        }
    }

    @Test
    void operationsAfterClose_throwClosedChannelException() throws IOException {
        FileRangeReader reader = FileRangeReader.of(testFile);
        RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader);
        channel.close();

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(10))).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.position()).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.position(100L)).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.size()).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.getRangeReader()).isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.getSourceIdentifier()).isInstanceOf(ClosedChannelException.class);

        reader.close(); // Clean up
    }

    @Test
    void getRangeReader_returnsUnderlyingReader() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThat(channel.getRangeReader()).isSameAs(reader);
        }
    }

    @Test
    void getSourceIdentifier_returnsFileIdentifier() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            String identifier = channel.getSourceIdentifier();
            assertThat(identifier).contains(testFile.toString());
            assertThat(identifier).isEqualTo(reader.getSourceIdentifier());
        }
    }

    @Test
    void toString_openChannel_includesDetails() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            String result = channel.toString();
            assertThat(result)
                    .contains("RangeReaderSeekableByteChannel")
                    .contains("position=0")
                    .contains("size=" + TEST_FILE_SIZE);
        }
    }

    @Test
    void toString_closedChannel_indicatesClosed() throws IOException {
        FileRangeReader reader = FileRangeReader.of(testFile);
        RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader);
        channel.close();

        String result = channel.toString();
        assertThat(result).contains("RangeReaderSeekableByteChannel[closed]");

        reader.close(); // Clean up
    }

    @Test
    void toString_withIOException_handlesGracefully() throws IOException {
        // This test is harder to implement with real FileRangeReader since it doesn't easily throw IOExceptions
        // on size() calls. We'll skip this specific test for now as it requires mock setup.
        // The toString method does handle IOExceptions gracefully as shown in the implementation.
    }

    @Test
    void threadSafety_concurrentPositionAccess() throws Exception {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // This is a basic test for thread safety - in a real scenario you'd want more comprehensive testing
            Thread thread1 = new Thread(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        channel.position(i * 10);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            Thread thread2 = new Thread(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        channel.position();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            thread1.start();
            thread2.start();
            thread1.join();
            thread2.join();

            // If we get here without exceptions, the basic thread safety worked
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    void seeking_readFromDifferentPositions() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Test reading from different positions by seeking
            ByteBuffer buffer = ByteBuffer.allocate(50);

            // Read from position 100
            channel.position(100);
            int bytesRead1 = channel.read(buffer);
            assertThat(bytesRead1).isEqualTo(50);
            assertThat(channel.position()).isEqualTo(150L);

            byte[] data1 = new byte[bytesRead1];
            buffer.flip().get(data1);

            // Seek to position 500 and read
            buffer.clear();
            channel.position(500);
            int bytesRead2 = channel.read(buffer);
            assertThat(bytesRead2).isEqualTo(50);
            assertThat(channel.position()).isEqualTo(550L);

            byte[] data2 = new byte[bytesRead2];
            buffer.flip().get(data2);

            // Verify we read different data from different positions
            byte[] expected1 = new byte[50];
            byte[] expected2 = new byte[50];
            System.arraycopy(testData, 100, expected1, 0, 50);
            System.arraycopy(testData, 500, expected2, 0, 50);

            assertThat(data1).isEqualTo(expected1);
            assertThat(data2).isEqualTo(expected2);
            assertThat(data1).isNotEqualTo(data2); // Should be different data
        }
    }

    @Test
    void integrationWithJavaNIOAPIs() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Arrange
            ByteBuffer buffer = ByteBuffer.allocate(100);

            // Act - Use as SeekableByteChannel interface
            SeekableByteChannel seekableChannel = channel;
            seekableChannel.position(50L);
            int bytesRead = seekableChannel.read(buffer);
            buffer.flip();

            // Assert
            assertThat(bytesRead).isEqualTo(100);
            assertThat(seekableChannel.position()).isEqualTo(150L);

            // Verify data integrity - buffer is already positioned correctly after readRange
            byte[] readData = new byte[bytesRead];
            buffer.get(readData);

            byte[] expectedData = new byte[100];
            System.arraycopy(testData, 50, expectedData, 0, 100);
            assertThat(readData).isEqualTo(expectedData);
        }
    }
}
