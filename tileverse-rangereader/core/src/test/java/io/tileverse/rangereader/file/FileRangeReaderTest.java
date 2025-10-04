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
package io.tileverse.rangereader.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Comprehensive tests for FileRangeReader.
 */
class FileRangeReaderTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private String textContent;
    private FileRangeReader reader;

    @BeforeEach
    void setUp() throws IOException {
        // Create a text file with known content
        testFile = tempDir.resolve("test-file.txt");
        textContent =
                """
                The quick brown fox jumps over the lazy dog. \
                ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789 \
                abcdefghijklmnopqrstuvwxyz""";
        Files.writeString(testFile, textContent, StandardOpenOption.CREATE);

        // Initialize the reader
        reader = FileRangeReader.of(testFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Close the reader after each test
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    void testBuilder() throws IOException {
        assertThat(FileRangeReader.builder().path(testFile).build())
                .isNotNull()
                .hasFieldOrPropertyWithValue("path", testFile);
        assertThat(FileRangeReader.builder().path(testFile.toString()).build())
                .isNotNull()
                .hasFieldOrPropertyWithValue("path", testFile);
        assertThat(FileRangeReader.builder().uri(testFile.toUri()).build())
                .isNotNull()
                .hasFieldOrPropertyWithValue("path", testFile);

        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> FileRangeReader.builder()
                .uri(URI.create("http://test.com/file")));
        assertThat(iae)
                .hasRootCauseInstanceOf(FileSystemNotFoundException.class)
                .hasMessageContainingAll(
                        "Unable to create reader for URI http://test.com/file", "Provider \"http\" not installed");

        assertThrows(
                IllegalStateException.class, () -> FileRangeReader.builder().build());
    }

    @Test
    void testConstructorWithNullPath() {
        assertThrows(NullPointerException.class, () -> FileRangeReader.of(null));
    }

    @Test
    void testNonExistentFile() {
        Path nonExistentFile = tempDir.resolve("non-existent-file.txt");
        assertThrows(NoSuchFileException.class, () -> FileRangeReader.of(nonExistentFile));
    }

    @Test
    void testGetSize() throws IOException {
        assertEquals(textContent.length(), reader.size().getAsLong());
    }

    @Test
    void testReadEntireFile() throws IOException {
        ByteBuffer buffer = reader.readRange(0, textContent.length()).flip();

        assertEquals(textContent.length(), buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent, result);
    }

    @Test
    void testReadRangeFromStart() throws IOException {
        int length = 10;
        ByteBuffer buffer = reader.readRange(0, length).flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(0, length), result);
    }

    @Test
    void testReadRangeFromMiddle() throws IOException {
        int offset = 20;
        int length = 15;
        ByteBuffer buffer = reader.readRange(offset, length).flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(offset, offset + length), result);
    }

    @Test
    void testReadRangeAtEnd() throws IOException {
        int offset = textContent.length() - 10;
        int length = 10;
        ByteBuffer buffer = reader.readRange(offset, length);

        assertEquals(length, buffer.flip().remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(offset), result);
    }

    @Test
    void testReadBeyondEnd() throws IOException {
        int offset = textContent.length() - 5;
        int length = 20; // Trying to read 20 bytes, but only 5 are available
        ByteBuffer buffer = reader.readRange(offset, length);

        // Only 5 bytes should be read
        assertEquals(5, buffer.flip().remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(offset), result);
    }

    @Test
    void testReadStartingBeyondEnd() throws IOException {
        int offset = textContent.length() + 10; // Start beyond the end of the file
        int length = 10;
        ByteBuffer buffer = reader.readRange(offset, length).flip();

        // Should return an empty buffer
        assertEquals(0, buffer.remaining());
    }

    @Test
    void testReadMultipleRanges() throws IOException {
        // Read multiple ranges and verify they're correct
        ByteBuffer buffer1 = reader.readRange(5, 10).flip();
        ByteBuffer buffer2 = reader.readRange(20, 15).flip();
        ByteBuffer buffer3 = reader.readRange(50, 20).flip();

        byte[] bytes1 = new byte[buffer1.remaining()];
        buffer1.get(bytes1);
        String result1 = new String(bytes1, StandardCharsets.UTF_8);

        byte[] bytes2 = new byte[buffer2.remaining()];
        buffer2.get(bytes2);
        String result2 = new String(bytes2, StandardCharsets.UTF_8);

        byte[] bytes3 = new byte[buffer3.remaining()];
        buffer3.get(bytes3);
        String result3 = new String(bytes3, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(5, 15), result1);
        assertEquals(textContent.substring(20, 35), result2);
        assertEquals(textContent.substring(50, 70), result3);
    }

    @Test
    void testReadVeryLargeRange() throws IOException {
        // Try to read a range larger than the file (but not unreasonably large)
        int largeSize = textContent.length() * 10; // 10 times the actual file size
        ByteBuffer buffer = reader.readRange(0, largeSize).flip();

        assertEquals(textContent.length(), buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent, result);
    }

    @Test
    void testReadWithZeroLength() throws IOException {
        // Test reading with length = 0
        ByteBuffer buffer = reader.readRange(10, 0);

        assertEquals(0, buffer.remaining());
    }

    @Test
    void testReadWithNegativeOffset() {
        // Test reading with negative offset - should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(-1, 10));
    }

    @Test
    void testReadWithNegativeLength() {
        // Test reading with negative length - should throw exception
        assertThrows(IllegalArgumentException.class, () -> reader.readRange(0, -1));
    }

    @Test
    void testBinaryData() throws IOException {
        // Create a binary file with random data
        Path binaryFile = tempDir.resolve("binary-data.bin");
        byte[] binaryContent = new byte[8192]; // 8KB
        new Random(42).nextBytes(binaryContent); // Use seed 42 for reproducibility
        Files.write(binaryFile, binaryContent, StandardOpenOption.CREATE);

        try (FileRangeReader binaryReader = FileRangeReader.of(binaryFile)) {
            // Check size
            assertEquals(binaryContent.length, binaryReader.size().getAsLong());

            // Read full content
            ByteBuffer fullBuffer =
                    binaryReader.readRange(0, binaryContent.length).flip();
            byte[] fullBytes = new byte[fullBuffer.remaining()];
            fullBuffer.get(fullBytes);
            assertArrayEquals(binaryContent, fullBytes);

            // Read a chunk from the middle
            int offset = 1024;
            int length = 2048;
            ByteBuffer partialBuffer = binaryReader.readRange(offset, length).flip();
            byte[] partialBytes = new byte[partialBuffer.remaining()];
            partialBuffer.get(partialBytes);

            byte[] expectedPartial = Arrays.copyOfRange(binaryContent, offset, offset + length);
            assertArrayEquals(expectedPartial, partialBytes);
        }
    }

    @Test
    void testLargeFile() throws IOException {
        // Create a larger file (1MB)
        Path largeFile = tempDir.resolve("large-file.bin");
        int size = 1024 * 1024; // 1MB
        byte[] largeContent = new byte[size];

        // Fill with pattern data for verification
        for (int i = 0; i < size; i++) {
            largeContent[i] = (byte) (i % 256);
        }

        Files.write(largeFile, largeContent, StandardOpenOption.CREATE);

        try (FileRangeReader largeReader = FileRangeReader.of(largeFile)) {
            // Check size
            assertEquals(size, largeReader.size().getAsLong());

            // Read 100KB from the middle
            int offset = 400 * 1024; // 400KB offset
            int length = 100 * 1024; // 100KB length
            ByteBuffer buffer = largeReader.readRange(offset, length).flip();

            assertEquals(length, buffer.remaining());

            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            // Verify the data
            for (int i = 0; i < length; i++) {
                assertEquals((byte) ((offset + i) % 256), bytes[i]);
            }
        }
    }

    @Test
    void testEmptyFile() throws IOException {
        // Create an empty file
        Path emptyFile = tempDir.resolve("empty.txt");
        Files.createFile(emptyFile);

        try (FileRangeReader emptyReader = FileRangeReader.of(emptyFile)) {
            // Size should be 0
            assertEquals(0, emptyReader.size().getAsLong());

            // Reading should return empty buffer
            ByteBuffer buffer = emptyReader.readRange(0, 10).flip();
            assertEquals(0, buffer.remaining());
        }
    }

    @Test
    void testCloseAndReuseAttempt() throws IOException {
        // Test closing and attempting to use after close
        reader.close();

        // Operations after close should throw
        assertThrows(IOException.class, () -> reader.size());
        assertThrows(IOException.class, () -> reader.readRange(0, 10));
    }

    @Test
    void testConcurrentReads() throws Exception {
        // Create a file with predictable content for concurrent reads
        Path concurrentFile = tempDir.resolve("concurrent-test.txt");
        int fileSize = 100_000; // 100KB
        byte[] data = new byte[fileSize];

        // Fill with deterministic pattern for verification
        for (int i = 0; i < fileSize; i++) {
            data[i] = (byte) (i % 256);
        }
        Files.write(concurrentFile, data, StandardOpenOption.CREATE);

        // Create a shared reader - our implementation should be thread-safe
        FileRangeReader sharedReader = FileRangeReader.of(concurrentFile);

        try {
            // Define regions to read concurrently
            // We'll create 10 threads, each reading a different region
            int numThreads = 10;
            int regionSize = fileSize / numThreads;
            CountDownLatch startLatch = new CountDownLatch(1); // To make threads start simultaneously

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<Boolean>> futures = new ArrayList<>();

            // Submit tasks for concurrent execution
            for (int i = 0; i < numThreads; i++) {
                final int threadIndex = i;
                final int regionStart = threadIndex * regionSize;

                // Each thread will read its own region and verify the data
                futures.add(executor.submit(() -> {
                    try {
                        // Wait for all threads to be ready
                        startLatch.await();

                        // Read the region
                        ByteBuffer buffer = sharedReader.readRange(regionStart, regionSize);
                        byte[] readData = new byte[buffer.remaining()];
                        buffer.get(readData);

                        // Verify each byte
                        for (int j = 0; j < readData.length; j++) {
                            byte expected = (byte) ((regionStart + j) % 256);
                            if (readData[j] != expected) {
                                System.err.printf(
                                        "Thread %d: Data mismatch at index %d, expected %d but got %d%n",
                                        threadIndex, j, expected & 0xFF, readData[j] & 0xFF);
                                return false;
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }));
            }

            // Start all threads simultaneously
            startLatch.countDown();

            // Wait for all threads to complete and check results
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Verify all threads succeeded
            for (Future<Boolean> future : futures) {
                assertTrue(future.get(), "One or more threads failed to read correctly");
            }
        } finally {
            sharedReader.close();
        }
    }

    @Test
    void testConcurrentOverlappingReads() throws Exception {
        // Create a file with predictable content
        Path concurrentFile = tempDir.resolve("overlap-test.txt");
        int fileSize = 50_000; // 50KB
        byte[] data = new byte[fileSize];

        // Fill with deterministic pattern
        for (int i = 0; i < fileSize; i++) {
            data[i] = (byte) (i % 256);
        }
        Files.write(concurrentFile, data, StandardOpenOption.CREATE);

        // Create a shared reader
        FileRangeReader sharedReader = FileRangeReader.of(concurrentFile);

        try {
            // Multiple threads will read overlapping regions
            int numThreads = 20;
            int regionSize = 5000; // 5KB
            int regionStep = 2000; // Regions will overlap by 3KB
            CountDownLatch startLatch = new CountDownLatch(1);

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<Boolean>> futures = new ArrayList<>();

            // Submit tasks for concurrent execution
            for (int i = 0; i < numThreads; i++) {
                final int regionStart = i * regionStep;
                if (regionStart + regionSize > fileSize) {
                    break; // Don't go beyond file size
                }

                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Read the overlapping region
                        ByteBuffer buffer = sharedReader.readRange(regionStart, regionSize);
                        byte[] readData = new byte[buffer.remaining()];
                        buffer.get(readData);

                        // Verify each byte
                        for (int j = 0; j < readData.length; j++) {
                            byte expected = (byte) ((regionStart + j) % 256);
                            if (readData[j] != expected) {
                                return false;
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }));
            }

            // Start all threads simultaneously
            startLatch.countDown();

            // Wait for all threads to complete and check results
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // Verify all threads succeeded
            for (Future<Boolean> future : futures) {
                assertTrue(future.get(), "One or more threads failed to read correctly");
            }
        } finally {
            sharedReader.close();
        }
    }

    @Test
    void of_convenienceMethod_equivalentToBuilder() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("convenience-test.txt");
        String content = "Test content for convenience method";
        Files.write(testFile, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);

        // Test convenience method
        try (FileRangeReader reader1 = FileRangeReader.of(testFile);
                FileRangeReader reader2 =
                        FileRangeReader.builder().path(testFile).build()) {

            // Both should have same size
            assertEquals(reader1.size(), reader2.size());

            // Both should have same source identifier
            assertEquals(reader1.getSourceIdentifier(), reader2.getSourceIdentifier());

            // Both should read the same content
            ByteBuffer buffer1 = ByteBuffer.allocate(content.length());
            ByteBuffer buffer2 = ByteBuffer.allocate(content.length());

            int bytesRead1 = reader1.readRange(0, content.length(), buffer1);
            int bytesRead2 = reader2.readRange(0, content.length(), buffer2);

            assertEquals(bytesRead1, bytesRead2);
            assertEquals(buffer1.position(), buffer2.position());

            // Flip buffers to prepare for reading (NIO contract for ByteBuffer version)
            buffer1.flip();
            buffer2.flip();

            byte[] data1 = new byte[bytesRead1];
            byte[] data2 = new byte[bytesRead2];
            buffer1.get(data1);
            buffer2.get(data2);

            assertArrayEquals(data1, data2);
            assertEquals(content, new String(data1, StandardCharsets.UTF_8));
        }
    }

    @Test
    void of_withNullPath_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> FileRangeReader.of(null));
    }
}
