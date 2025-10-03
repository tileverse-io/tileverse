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

import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test for RangeReaderSeekableByteChannel using a real FileRangeReader.
 */
class RangeReaderSeekableByteChannelIntegrationTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private byte[] testData;

    @BeforeEach
    void setUp() throws IOException {
        // Create test data: 1KB of sequential bytes
        testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }

        // Write test data to file
        testFile = tempDir.resolve("test-data.bin");
        Files.write(testFile, testData, StandardOpenOption.CREATE);
    }

    @Test
    void completeWorkflow_fileRangeReader() throws IOException {
        // Create FileRangeReader and wrap it as SeekableByteChannel
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                SeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Verify initial state
            assertThat(channel.isOpen()).isTrue();
            assertThat(channel.position()).isEqualTo(0L);
            assertThat(channel.size()).isEqualTo(1024L);

            // Read first 100 bytes
            ByteBuffer buffer = ByteBuffer.allocate(100);
            int bytesRead = channel.read(buffer);
            assertThat(bytesRead).isEqualTo(100);
            assertThat(channel.position()).isEqualTo(100L);

            // Verify data integrity - buffer is already positioned correctly after readRange
            byte[] readData = new byte[bytesRead];
            buffer.flip().get(readData);
            assertThat(readData).isEqualTo(Arrays.copyOfRange(testData, 0, bytesRead));

            // Seek to middle of file
            channel.position(500L);
            assertThat(channel.position()).isEqualTo(500L);

            // Read 200 bytes from middle
            buffer = ByteBuffer.allocate(200);
            bytesRead = channel.read(buffer);
            assertThat(bytesRead).isEqualTo(200);
            assertThat(channel.position()).isEqualTo(700L);

            // Verify data integrity from middle - buffer is already positioned correctly
            readData = new byte[bytesRead];
            buffer.flip().get(readData);
            assertThat(readData).isEqualTo(Arrays.copyOfRange(testData, 500, 500 + bytesRead));

            // Seek near end and read remaining bytes
            channel.position(1000L);
            buffer = ByteBuffer.allocate(100); // Request more than available
            bytesRead = channel.read(buffer);
            assertThat(bytesRead).isEqualTo(24); // Only 24 bytes remaining
            assertThat(channel.position()).isEqualTo(1024L);

            // Try to read beyond end
            buffer = ByteBuffer.allocate(100);
            bytesRead = channel.read(buffer);
            assertThat(bytesRead).isEqualTo(-1); // End of file
        }
    }

    @Test
    void sequentialReading_compareWithDirectFileAccess() throws IOException {
        // Read entire file using SeekableByteChannel
        byte[] channelData = new byte[1024];
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                SeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            ByteBuffer buffer = ByteBuffer.allocate(256);
            int totalBytesRead = 0;
            int bytesRead;

            while ((bytesRead = channel.read(buffer)) != -1) {
                // Buffer is already positioned correctly after readRange
                buffer.flip().get(channelData, totalBytesRead, bytesRead);
                totalBytesRead += bytesRead;
                buffer.clear();
            }

            assertThat(totalBytesRead).isEqualTo(1024);
        }

        // Compare with original test data
        assertThat(channelData).isEqualTo(testData);
    }

    @Test
    void randomAccess_multipleSeeksAndReads() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                SeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            // Test multiple random accesses
            int[][] accessPatterns = {
                {0, 50}, // Read first 50 bytes
                {512, 100}, // Read 100 bytes from middle
                {900, 124}, // Read 124 bytes near end
                {50, 200}, // Read 200 bytes from offset 50
                {1020, 4} // Read last 4 bytes
            };

            for (int[] pattern : accessPatterns) {
                long offset = pattern[0];
                int length = pattern[1];

                // Seek and read
                channel.position(offset);
                ByteBuffer buffer = ByteBuffer.allocate(length);
                int bytesRead = channel.read(buffer);

                // Verify position and data
                assertThat(channel.position()).isEqualTo(offset + bytesRead);
                assertThat(bytesRead).isEqualTo(Math.min(length, (int) (1024 - offset)));

                // Verify data integrity - buffer is already positioned correctly
                if (bytesRead > 0) {
                    byte[] readData = new byte[bytesRead];
                    buffer.flip().get(readData);
                    byte[] expectedData = Arrays.copyOfRange(testData, (int) offset, (int) offset + bytesRead);
                    assertThat(readData).isEqualTo(expectedData);
                }
            }
        }
    }

    @Test
    void largeFile_performance() throws IOException {
        // Create a larger test file (10MB)
        Path largeFile = tempDir.resolve("large-test-data.bin");
        byte[] largeData = new byte[10 * 1024 * 1024]; // 10MB

        // Fill with pattern
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) ((i / 1024) % 256); // Change every 1KB
        }

        Files.write(largeFile, largeData, StandardOpenOption.CREATE);

        try (FileRangeReader reader = FileRangeReader.of(largeFile);
                SeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            assertThat(channel.size()).isEqualTo(10 * 1024 * 1024L);

            // Test random access performance with larger chunks
            long[] positions = {0L, 1024 * 1024L, 5 * 1024 * 1024L, 9 * 1024 * 1024L};

            for (long position : positions) {
                channel.position(position);
                ByteBuffer buffer = ByteBuffer.allocate(64 * 1024); // 64KB chunks
                int bytesRead = channel.read(buffer);

                assertThat(bytesRead).isGreaterThan(0);
                assertThat(channel.position()).isEqualTo(position + bytesRead);

                // Verify data pattern - buffer is already positioned correctly
                if (bytesRead > 0) {
                    byte expectedValue = (byte) ((position / 1024) % 256);
                    assertThat(buffer.get(0)).isEqualTo(expectedValue);
                }
            }
        }
    }

    @Test
    void sourceIdentifier_preserved() throws IOException {
        try (FileRangeReader reader = FileRangeReader.of(testFile);
                RangeReaderSeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {

            String identifier = channel.getSourceIdentifier();
            assertThat(identifier).contains(testFile.toString());
            assertThat(identifier).isEqualTo(reader.getSourceIdentifier());
        }
    }
}
