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
package io.tileverse.storage.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Comprehensive tests for FileRangeReader. */
@Slf4j
class FileRangeReaderTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private String textContent;
    private FileRangeReader reader;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-file.txt");
        textContent = """
                The quick brown fox jumps over the lazy dog. \
                ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789 \
                abcdefghijklmnopqrstuvwxyz""";
        Files.writeString(testFile, textContent, StandardOpenOption.CREATE);

        reader = new FileRangeReader(testFile);
    }

    @AfterEach
    void tearDown() {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    void testFactory() throws IOException {
        assertThat(new FileRangeReader(testFile)).isNotNull().hasFieldOrPropertyWithValue("path", testFile);
        assertThat(new FileRangeReader(testFile)).isNotNull().hasFieldOrPropertyWithValue("path", testFile);
    }

    @Test
    void testConstructorWithNullPath() {
        assertThatThrownBy(() -> new FileRangeReader(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testNonExistentFile() {
        Path nonExistentFile = tempDir.resolve("non-existent-file.txt");
        assertThatThrownBy(() -> new FileRangeReader(nonExistentFile)).isInstanceOf(NoSuchFileException.class);
    }

    @Test
    void testGetSize() {
        assertEquals(textContent.length(), reader.size().getAsLong());
    }

    @Test
    void testReadEntireFile() {
        ByteBuffer buffer = reader.readRange(0, textContent.length()).flip();

        assertEquals(textContent.length(), buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent, result);
    }

    @Test
    void testReadRangeFromStart() {
        int length = 10;
        ByteBuffer buffer = reader.readRange(0, length).flip();

        assertEquals(length, buffer.remaining());

        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        assertEquals(textContent.substring(0, length), result);
    }

    @Test
    void testReadRangeFromMiddle() {
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
    void testReadRangeAtEnd() {
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
    void testReadBeyondEnd() {
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
    void testReadStartingBeyondEnd() {
        int offset = textContent.length() + 10; // Start beyond the end of the file
        int length = 10;
        ByteBuffer buffer = reader.readRange(offset, length).flip();

        // Should return an empty buffer
        assertEquals(0, buffer.remaining());
    }

    @Test
    void testReadMultipleRanges() {
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
    void testReadVeryLargeRange() {
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
    void testReadWithZeroLength() {
        ByteBuffer buffer = reader.readRange(10, 0);

        assertEquals(0, buffer.remaining());
    }

    @Test
    void testReadWithNegativeOffset() {
        assertThatThrownBy(() -> reader.readRange(-1, 10)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadWithNegativeLength() {
        assertThatThrownBy(() -> reader.readRange(0, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBinaryData() throws IOException {
        // Create a binary file with random data
        Path binaryFile = tempDir.resolve("binary-data.bin");
        byte[] binaryContent = new byte[8192]; // 8KB
        new Random(42).nextBytes(binaryContent); // Use seed 42 for reproducibility
        Files.write(binaryFile, binaryContent, StandardOpenOption.CREATE);

        try (FileRangeReader binaryReader = new FileRangeReader(binaryFile)) {
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

        try (FileRangeReader largeReader = new FileRangeReader(largeFile)) {
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

        try (FileRangeReader emptyReader = new FileRangeReader(emptyFile)) {
            // Size should be 0
            assertEquals(0, emptyReader.size().getAsLong());

            // Reading should return empty buffer
            ByteBuffer buffer = emptyReader.readRange(0, 10).flip();
            assertEquals(0, buffer.remaining());
        }
    }

    @Test
    void testCloseAndReuseAttempt() {
        // Test closing and attempting to use after close
        reader.close();

        // Operations after close should throw IllegalStateException (now RuntimeException, not IOException)
        assertThatThrownBy(() -> reader.size()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> reader.readRange(0, 10)).isInstanceOf(IllegalStateException.class);
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
        FileRangeReader sharedReader = new FileRangeReader(concurrentFile);

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
                                log.debug(
                                        "Thread {}: Data mismatch at index {}, expected {} but got {}",
                                        threadIndex,
                                        j,
                                        expected & 0xFF,
                                        readData[j] & 0xFF);
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
        FileRangeReader sharedReader = new FileRangeReader(concurrentFile);

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

            startLatch.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            for (Future<Boolean> future : futures) {
                assertTrue(future.get(), "One or more threads failed to read correctly");
            }
        } finally {
            sharedReader.close();
        }
    }

    @Test
    void of_convenienceMethod_equivalentToBuilder() throws IOException {
        String content = "Test content for convenience method";
        Files.write(testFile, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);

        try (FileRangeReader reader1 = new FileRangeReader(testFile);
                FileRangeReader reader2 = new FileRangeReader(testFile)) {

            assertEquals(reader1.size(), reader2.size());
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
        assertThatThrownBy(() -> new FileRangeReader(null)).isInstanceOf(NullPointerException.class);
    }

    @Nested
    class NfsResilienceTests {

        @Test
        void idleTimeoutClosesChannel() throws Exception {
            Duration idleTimeout = Duration.ofMillis(200);
            try (FileRangeReader r = new FileRangeReader(testFile, idleTimeout)) {

                // Trigger a read to open the channel
                ByteBuffer buf = r.readRange(0, 10).flip();
                assertThat(buf.remaining()).isEqualTo(10);

                // Channel should be open after the read
                assertThat(r.channel()).isNotNull();
                assertThat(r.channel().isOpen()).isTrue();

                // Wait for idle timeout to close the channel
                await().atMost(Duration.ofSeconds(2))
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> {
                            FileChannel ch = r.channel();
                            assertThat(ch).isNull();
                        });
            }
        }

        @Test
        void readReopensAfterIdleClose() throws Exception {
            Duration idleTimeout = Duration.ofMillis(200);
            try (FileRangeReader r = new FileRangeReader(testFile, idleTimeout)) {

                // First read
                String firstRead = readAsString(r, 0, textContent.length());
                assertThat(firstRead).isEqualTo(textContent);

                // Wait for idle close
                await().atMost(Duration.ofSeconds(2))
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> assertThat(r.channel()).isNull());

                // Read again - should transparently reopen
                String secondRead = readAsString(r, 0, textContent.length());
                assertThat(secondRead).isEqualTo(textContent);

                // Channel should be open again
                assertThat(r.channel()).isNotNull();
                assertThat(r.channel().isOpen()).isTrue();
            }
        }

        @Test
        void retryOnClosedChannel() throws Exception {
            // Use no idle timeout so we control the channel lifecycle
            try (FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO)) {

                // First read opens the channel
                String firstRead = readAsString(r, 0, 10);
                assertThat(firstRead).isEqualTo(textContent.substring(0, 10));

                // Simulate stale channel by closing it directly
                FileChannel ch = r.channel();
                assertThat(ch).isNotNull();
                ch.close();

                // Next read should detect the closed channel and retry with a fresh one
                String retryRead = readAsString(r, 0, textContent.length());
                assertThat(retryRead).isEqualTo(textContent);
            }
        }

        @Test
        void retryOnNulledChannel() throws Exception {
            // Simulate the idle closer having nulled the channel between ensureOpen and read
            try (FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO)) {

                // First read opens the channel
                readAsString(r, 0, 10);

                // Close and null the channel (simulating idleClose)
                r.closeChannel();

                // Next read should reopen
                String result = readAsString(r, 0, textContent.length());
                assertThat(result).isEqualTo(textContent);
            }
        }

        @Test
        void retryOnStaleFileHandle() throws Exception {
            // Subclass that returns a channel throwing "Stale file handle" on the first openChannel() call, then a real
            // channel on the second (retry) call.
            java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    if (callCount.incrementAndGet() == 1) {
                        // close immediately so read() throws ClosedChannelException (a recoverable error), simulating a
                        // stale handle.
                        // we can't throw "Stale file handle" from FileChannel.open itself because ensureOpen would
                        // propagate it before doRead runs. Instead, return a closed channel so ch.read() inside doRead
                        // throws.
                        FileChannel ch = super.openChannel(path);
                        ch.close();
                        return ch;
                    }
                    return super.openChannel(path);
                }
            };
            try (r) {
                // triggers openChannel twice: first returns closed channel (doRead throws ClosedChannelException),
                // retry path calls closeStaleChannel() then ensureOpen() again which calls openChannel() a second time
                // with a real channel.
                String result = readAsString(r, 0, textContent.length());
                assertThat(result).isEqualTo(textContent);
                assertThat(callCount.get()).isEqualTo(2);
            }
        }

        @Test
        void retryOnStaleFileHandleIOException() throws Exception {
            // Same as above but uses a proxy channel that ("Stale file handle") on read(), exercising
            // the message-based detection path.
            java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    FileChannel real = super.openChannel(path);
                    if (callCount.incrementAndGet() == 1) {
                        return new ForwardingFileChannel(real) {
                            @Override
                            public int read(ByteBuffer dst, long position) throws IOException {
                                throw new IOException("Stale file handle");
                            }
                        };
                    }
                    return real;
                }
            };
            try (r) {
                String result = readAsString(r, 0, textContent.length());
                assertThat(result).isEqualTo(textContent);
                assertThat(callCount.get()).isEqualTo(2);
            }
        }

        @Test
        void nonRecoverableErrorIsNotRetried() throws Exception {
            java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    FileChannel real = super.openChannel(path);
                    callCount.incrementAndGet();
                    return new ForwardingFileChannel(real) {
                        @Override
                        public int read(ByteBuffer dst, long position) throws IOException {
                            throw new IOException("Permission denied");
                        }
                    };
                }
            };
            try (r) {
                assertThatThrownBy(() -> r.readRange(0, 10)).isInstanceOf(io.tileverse.storage.StorageException.class);
                // Should NOT have retried -- only one openChannel call
                assertThat(callCount.get()).isEqualTo(1);
            }
        }

        @Test
        void permanentCloseSkipsRetryOnRecoverableError() throws Exception {
            // after close(), readRange raises the closed-state error eagerly via size() which throws
            // IllegalStateException before reaching readRangeNoFlip.
            try (FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO)) {

                r.readRange(0, 10);
                // permanently close
                r.close();

                assertThatThrownBy(() -> r.readRange(0, 10))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("closed");
            }
        }

        @Test
        void sizeWorksAfterIdleClose() throws Exception {
            Duration idleTimeout = Duration.ofMillis(200);
            try (FileRangeReader r = new FileRangeReader(testFile, idleTimeout)) {

                // trigger a read to open channel
                r.readRange(0, 1);

                // wait for idle close
                await().atMost(Duration.ofSeconds(2))
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> assertThat(r.channel()).isNull());

                // size() should still work even with channel closed
                assertThat(r.size()).hasValue(textContent.length());
            }
        }

        @Test
        void sizeThrowsAfterPermanentClose() {
            reader.close();
            assertThatThrownBy(() -> reader.size()).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void closeIsIdempotent() {
            assertThatNoException().isThrownBy(() -> {
                reader.close();
                reader.close();
                reader.close();
            });
        }

        @Test
        void permanentClosePreventChannelReopen() {
            reader.readRange(0, 10);
            reader.close();
            // After close, size() throws IllegalStateException eagerly from the readRange entry path.
            assertThatThrownBy(() -> reader.readRange(0, 10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        }

        @Test
        void isRecoverableError_staleFileHandle() {
            assertThat(FileRangeReader.isRecoverableError(new IOException("Stale file handle")))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverableError(new IOException("stale file handle")))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverableError(new IOException("NFS: Stale file handle (errno 116)")))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverableError(new ClosedChannelException()))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverableError(new IOException("Permission denied")))
                    .isFalse();
            assertThat(FileRangeReader.isRecoverableError(new IOException((String) null)))
                    .isFalse();
        }

        @Test
        void constructorIdleTimeoutValidation() {
            assertThatNoException().isThrownBy(() -> new FileRangeReader(testFile, Duration.ofSeconds(30)));
            assertThatNoException().isThrownBy(() -> new FileRangeReader(testFile, Duration.ZERO));
            Duration negativeDuration = Duration.ofSeconds(-1);
            assertThatThrownBy(() -> new FileRangeReader(testFile, negativeDuration))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new FileRangeReader(testFile, null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void disabledIdleTimeoutKeepsChannelOpen() throws Exception {
            try (FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO)) {

                // read to open channel
                r.readRange(0, 10);
                FileChannel ch = r.channel();
                assertThat(ch).isNotNull();

                // wait a bit, channel should remain open
                Thread.sleep(300);
                assertThat(r.channel()).isSameAs(ch);
                assertThat(ch.isOpen()).isTrue();
            }
        }

        @Test
        void concurrentReadsWithIdleTimeout() throws Exception {
            Path concurrentFile = tempDir.resolve("concurrent-idle-test.bin");
            int fileSize = 100_000;
            byte[] data = new byte[fileSize];
            for (int i = 0; i < fileSize; i++) {
                data[i] = (byte) (i % 256);
            }
            Files.write(concurrentFile, data, StandardOpenOption.CREATE);

            try (FileRangeReader r = new FileRangeReader(concurrentFile, Duration.ofMillis(300))) {

                int numThreads = 10;
                int regionSize = fileSize / numThreads;
                CountDownLatch startLatch = new CountDownLatch(1);
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                List<Future<Boolean>> futures = new ArrayList<>();

                for (int i = 0; i < numThreads; i++) {
                    final int regionStart = i * regionSize;
                    futures.add(executor.submit(() -> {
                        try {
                            startLatch.await();
                            ByteBuffer buffer = r.readRange(regionStart, regionSize);
                            byte[] readData = new byte[buffer.remaining()];
                            buffer.get(readData);
                            for (int j = 0; j < readData.length; j++) {
                                if (readData[j] != (byte) ((regionStart + j) % 256)) {
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

                startLatch.countDown();
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);

                for (Future<Boolean> future : futures) {
                    assertTrue(future.get(), "One or more threads failed to read correctly");
                }
            }
        }

        private String readAsString(FileRangeReader r, int offset, int length) {
            ByteBuffer buf = r.readRange(offset, length).flip();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        /**
         * FileChannel wrapper that delegates all operations to a real channel. Tests override specific methods to
         * inject failures.
         */
        private static class ForwardingFileChannel extends FileChannel {
            private final FileChannel delegate;

            ForwardingFileChannel(FileChannel delegate) {
                this.delegate = delegate;
            }

            @Override
            public int read(ByteBuffer dst) throws IOException {
                return delegate.read(dst);
            }

            @Override
            public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                return delegate.read(dsts, offset, length);
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                return delegate.write(src);
            }

            @Override
            public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                return delegate.write(srcs, offset, length);
            }

            @Override
            public long position() throws IOException {
                return delegate.position();
            }

            @Override
            public FileChannel position(long newPosition) throws IOException {
                return delegate.position(newPosition);
            }

            @Override
            public long size() throws IOException {
                return delegate.size();
            }

            @Override
            public FileChannel truncate(long size) throws IOException {
                return delegate.truncate(size);
            }

            @Override
            public void force(boolean metaData) throws IOException {
                delegate.force(metaData);
            }

            @Override
            public long transferTo(long position, long count, java.nio.channels.WritableByteChannel target)
                    throws IOException {
                return delegate.transferTo(position, count, target);
            }

            @Override
            public long transferFrom(java.nio.channels.ReadableByteChannel src, long position, long count)
                    throws IOException {
                return delegate.transferFrom(src, position, count);
            }

            @Override
            public int read(ByteBuffer dst, long position) throws IOException {
                return delegate.read(dst, position);
            }

            @Override
            public int write(ByteBuffer src, long position) throws IOException {
                return delegate.write(src, position);
            }

            @Override
            public java.nio.MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
                return delegate.map(mode, position, size);
            }

            @Override
            public java.nio.channels.FileLock lock(long position, long size, boolean shared) throws IOException {
                return delegate.lock(position, size, shared);
            }

            @Override
            public java.nio.channels.FileLock tryLock(long position, long size, boolean shared) throws IOException {
                return delegate.tryLock(position, size, shared);
            }

            @Override
            protected void implCloseChannel() throws IOException {
                delegate.close();
            }
        }
    }
}
