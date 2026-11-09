/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.file;

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
        Path realPath = testFile.toRealPath();
        assertThat(new FileRangeReader(testFile)).isNotNull().hasFieldOrPropertyWithValue("path", realPath);
        assertThat(new FileRangeReader(testFile)).isNotNull().hasFieldOrPropertyWithValue("path", realPath);
    }

    @Test
    void constructorRejectsADirectory() {
        assertThatThrownBy(() -> new FileRangeReader(tempDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(tempDir.toString());
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
                        ByteBuffer buffer =
                                sharedReader.readRange(regionStart, regionSize).flip();
                        byte[] readData = new byte[buffer.remaining()];
                        buffer.get(readData);
                        if (readData.length != regionSize) {
                            log.debug("Thread {}: short read {} of {}", threadIndex, readData.length, regionSize);
                            return false;
                        }

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
                        ByteBuffer buffer =
                                sharedReader.readRange(regionStart, regionSize).flip();
                        byte[] readData = new byte[buffer.remaining()];
                        buffer.get(readData);
                        if (readData.length != regionSize) {
                            return false;
                        }

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

    /**
     * The identifier is the file's real path, resolved once at construction, and every channel opens by it. The three
     * aliases below are platform-bound, hence the assumptions.
     */
    @Nested
    class IdentityTests {

        @Test
        void identifierIsTheRealPathBehindASymlink() throws IOException {
            Path link = symlinkOrSkip(tempDir.resolve("link.txt"), testFile);

            try (FileRangeReader throughLink = new FileRangeReader(link);
                    FileRangeReader direct = new FileRangeReader(testFile)) {
                assertThat(throughLink.getSourceIdentifier())
                        .isEqualTo(testFile.toRealPath().toString())
                        .isEqualTo(direct.getSourceIdentifier());
            }
        }

        @Test
        void identifierIgnoresTheSpellingOnACaseInsensitiveFilesystem() throws IOException {
            Path upperCase = tempDir.resolve("TEST-FILE.TXT");
            assumeTrue(isSameFileQuietly(testFile, upperCase), "case-insensitive filesystem only");

            try (FileRangeReader spelledUpper = new FileRangeReader(upperCase);
                    FileRangeReader spelledLower = new FileRangeReader(testFile)) {
                assertThat(spelledUpper.getSourceIdentifier()).isEqualTo(spelledLower.getSourceIdentifier());
            }
        }

        @Test
        void identifierResolvesTmpToPrivateTmpOnMacos() throws IOException {
            Path tmp = Path.of("/tmp");
            assumeTrue(
                    System.getProperty("os.name", "").contains("Mac") && Files.isSymbolicLink(tmp),
                    "macOS /tmp symlink only");

            Path file = Files.createTempFile(tmp, "tileverse-identity", ".txt");
            try (FileRangeReader r = new FileRangeReader(file)) {
                assertThat(r.getSourceIdentifier())
                        .startsWith("/private/tmp/")
                        .isEqualTo(file.toRealPath().toString());
            } finally {
                Files.deleteIfExists(file);
            }
        }

        @Test
        void channelOpensByTheRealPathResolvedAtConstruction() throws IOException {
            Path original = tempDir.resolve("original.txt");
            Path replacement = tempDir.resolve("replacement.txt");
            Files.writeString(original, "original content");
            Files.writeString(replacement, "replacement text");
            Path link = symlinkOrSkip(tempDir.resolve("moving-link.txt"), original);

            try (FileRangeReader r = new FileRangeReader(link, Duration.ZERO)) {
                Files.delete(link);
                Files.createSymbolicLink(link, replacement);

                ByteBuffer read = r.readRange(0, "original content".length()).flip();
                String content = StandardCharsets.UTF_8.decode(read).toString();
                assertThat(content).isEqualTo("original content");
            }
        }

        private Path symlinkOrSkip(Path link, Path target) {
            try {
                return Files.createSymbolicLink(link, target);
            } catch (UnsupportedOperationException | IOException e) {
                return abort("symbolic links are not available here: " + e);
            }
        }

        private boolean isSameFileQuietly(Path a, Path b) {
            try {
                return Files.isSameFile(a, b);
            } catch (IOException notFound) {
                return false;
            }
        }
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
            // A permanent close must reject the read immediately, unlike an idle close, which is
            // transient and triggers the recoverable-error retry with a fresh channel instead.
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
            // A permanent close must reject the read immediately, never silently reopening the channel.
            assertThatThrownBy(() -> reader.readRange(0, 10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        }

        @Test
        void isRecoverable_staleFileHandle() {
            assertThat(FileRangeReader.isRecoverable(new IOException("Stale file handle")))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverable(new IOException("stale file handle")))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverable(new IOException("NFS: Stale file handle (errno 116)")))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverable(new ClosedChannelException()))
                    .isTrue();
            assertThat(FileRangeReader.isRecoverable(new IOException("Permission denied")))
                    .isFalse();
            assertThat(FileRangeReader.isRecoverable(new IOException((String) null)))
                    .isFalse();
        }

        @Test
        void isRecoverable_matchesTheMacosAndBsdWording() {
            assertThat(FileRangeReader.isRecoverable(new IOException("Stale NFS file handle")))
                    .isTrue();
        }

        @Test
        void isRecoverable_ignoresTheDefaultLocale() {
            Locale saved = Locale.getDefault();
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            try {
                assertThat(FileRangeReader.isRecoverable(new IOException("STALE FILE HANDLE")))
                        .isTrue();
            } finally {
                Locale.setDefault(saved);
            }
        }

        @Test
        void isRecoverable_neverRetriesAnInterruptedRead() {
            assertThat(FileRangeReader.isRecoverable(new ClosedByInterruptException()))
                    .isFalse();
        }

        @Test
        void recoveryClosesOnlyTheFailedChannel() throws Exception {
            AtomicInteger opens = new AtomicInteger();
            AtomicInteger staleReads = new AtomicInteger();
            CyclicBarrier bothHoldTheStaleChannel = new CyclicBarrier(2);
            CountDownLatch firstReaderRecovered = new CountDownLatch(1);
            List<FileChannel> realChannels = Collections.synchronizedList(new ArrayList<>());
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    FileChannel real = super.openChannel(path);
                    realChannels.add(real);
                    if (opens.incrementAndGet() > 1) {
                        return real;
                    }
                    return new ForwardingFileChannel(real) {
                        @Override
                        public int read(ByteBuffer dst, long position) throws IOException {
                            int call = staleReads.incrementAndGet();
                            awaitQuietly(bothHoldTheStaleChannel);
                            if (call == 2) {
                                // the second reader fails only after the first has recovered on a replacement
                                awaitQuietly(firstReaderRecovered);
                            }
                            throw new IOException("Stale file handle");
                        }
                    };
                }
            };
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (r) {
                Callable<String> read = () -> {
                    String content = readAsString(r, 0, textContent.length());
                    firstReaderRecovered.countDown();
                    return content;
                };
                Future<String> first = pool.submit(read);
                Future<String> second = pool.submit(read);

                assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(textContent);
                assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(textContent);
                assertThat(opens)
                        .as("the replacement channel serves both recoveries")
                        .hasValue(2);
                assertThat(realChannels.get(1).isOpen())
                        .as("the second recovery must not close the first one's replacement")
                        .isTrue();
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void interruptedThreadFailsWithoutTouchingTheChannel() throws Exception {
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return super.openChannel(path);
                }
            };
            try (r) {
                readAsString(r, 0, 10);
                FileChannel before = r.channel();

                Thread.currentThread().interrupt();
                try {
                    assertThatThrownBy(() -> r.readRange(0, 10))
                            .isInstanceOf(StorageException.class)
                            .hasCauseInstanceOf(InterruptedIOException.class);
                    assertThat(Thread.currentThread().isInterrupted())
                            .as("the interrupt flag stays set")
                            .isTrue();
                } finally {
                    Thread.interrupted();
                }
                assertThat(opens).hasValue(1);
                assertThat(r.channel()).isSameAs(before);
                assertThat(before.isOpen()).isTrue();
            }
        }

        @Test
        void readSlowerThanTheIdleTimeoutCompletes() throws Exception {
            Duration idleTimeout = Duration.ofMillis(100);
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, idleTimeout) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        @Override
                        public int read(ByteBuffer dst, long position) throws IOException {
                            sleepQuietly(4 * idleTimeout.toMillis());
                            return delegate.read(dst, position);
                        }
                    };
                }
            };
            try (r) {
                assertThat(readAsString(r, 0, textContent.length())).isEqualTo(textContent);
                assertThat(opens).hasValue(1);

                await().atMost(Duration.ofSeconds(2))
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> assertThat(r.channel()).isNull());
            }
        }

        @Test
        void batchLongerThanTheIdleTimeoutKeepsOneChannel() throws Exception {
            Duration idleTimeout = Duration.ofMillis(100);
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, idleTimeout) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        @Override
                        public int read(ByteBuffer dst, long position) throws IOException {
                            sleepQuietly(idleTimeout.toMillis() + 50);
                            return delegate.read(dst, position);
                        }
                    };
                }
            };
            try (r) {
                List<RangeRequest> requests = List.of(
                        RangeRequest.of(0, 10, ByteBuffer.allocate(10)),
                        RangeRequest.of(20, 10, ByteBuffer.allocate(10)),
                        RangeRequest.of(40, 10, ByteBuffer.allocate(10)));

                BatchReadResult result = r.readRanges(requests);
                int[] read = counts(result);

                assertThat(read).containsExactly(10, 10, 10);
                assertThat(result.fetches())
                        .as("requests with a gap between them read one by one")
                        .isEqualTo(3);
                assertThat(result.bytesTransferred()).isEqualTo(30);
                assertThat(result.bytesRequested()).isEqualTo(30);
                for (int i = 0; i < requests.size(); i++) {
                    ByteBuffer target = requests.get(i).target().flip();
                    String content = StandardCharsets.UTF_8.decode(target).toString();
                    assertThat(content).isEqualTo(textContent.substring(20 * i, 20 * i + 10));
                }
                assertThat(opens).hasValue(1);
            }
        }

        @Test
        void retryResumesWhereTheFailedChunkStopped() throws Exception {
            int partialChunk = 10;
            AtomicInteger opens = new AtomicInteger();
            List<Long> readPositions = Collections.synchronizedList(new ArrayList<>());
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    FileChannel real = super.openChannel(path);
                    if (opens.incrementAndGet() > 1) {
                        return new ForwardingFileChannel(real) {
                            @Override
                            public int read(ByteBuffer dst, long position) throws IOException {
                                readPositions.add(position);
                                return delegate.read(dst, position);
                            }
                        };
                    }
                    return new ForwardingFileChannel(real) {
                        private int calls;

                        @Override
                        public int read(ByteBuffer dst, long position) throws IOException {
                            readPositions.add(position);
                            if (++calls > 1) {
                                throw new IOException("Stale file handle");
                            }
                            // land a partial chunk, then fail on the next one
                            int limit = dst.limit();
                            dst.limit(dst.position() + partialChunk);
                            try {
                                return delegate.read(dst, position);
                            } finally {
                                dst.limit(limit);
                            }
                        }
                    };
                }
            };
            try (r) {
                assertThat(readAsString(r, 0, textContent.length())).isEqualTo(textContent);
                assertThat(opens).hasValue(2);
                assertThat(readPositions)
                        .as("the retry resumes after the bytes already landed")
                        .containsExactly(0L, (long) partialChunk, (long) partialChunk);
            }
        }

        @Test
        @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
        void zeroByteChunkOnAnOpenChannelFailsInsteadOfSpinning() throws Exception {
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(testFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        @Override
                        public int read(ByteBuffer dst, long position) {
                            return 0;
                        }
                    };
                }
            };
            try (r) {
                assertThatThrownBy(() -> r.readRange(0, 10))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("3 attempts");
                assertThat(opens).hasValue(3);
            }
        }

        @Test
        void idleCloserDropsCancelledChecksAndRunsOnADaemonThread() throws Exception {
            ScheduledThreadPoolExecutor closer = (ScheduledThreadPoolExecutor) FileRangeReader.IDLE_CLOSER;
            assertThat(closer.getRemoveOnCancelPolicy()).isTrue();

            Thread closerThread = closer.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            assertThat(closerThread.getName()).isEqualTo("FileRangeReader-idle-closer");
            assertThat(closerThread.isDaemon()).isTrue();
            assertThat(closerThread.getContextClassLoader()).isSameAs(ClassLoader.getSystemClassLoader());
        }

        @Test
        void constructorIdleTimeoutValidation() {
            assertThatNoException().isThrownBy(() -> new FileRangeReader(testFile, Duration.ofSeconds(30)));
            assertThatNoException().isThrownBy(() -> new FileRangeReader(testFile, Duration.ofMillis(1)));
            assertThatNoException().isThrownBy(() -> new FileRangeReader(testFile, Duration.ZERO));
            Duration negativeDuration = Duration.ofSeconds(-1);
            assertThatThrownBy(() -> new FileRangeReader(testFile, negativeDuration))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new FileRangeReader(testFile, null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void constructorRejectsASubMillisecondIdleTimeout() {
            Duration halfAMillisecond = Duration.ofNanos(500_000);
            assertThatThrownBy(() -> new FileRangeReader(testFile, halfAMillisecond))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("millisecond");
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
                            ByteBuffer buffer =
                                    r.readRange(regionStart, regionSize).flip();
                            byte[] readData = new byte[buffer.remaining()];
                            buffer.get(readData);
                            if (readData.length != regionSize) {
                                return false;
                            }
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
    }

    /**
     * Batches of abutting requests read as one run. Vectored reads fill direct targets, on the shared channel when no
     * other vectored read holds it, else on an extra channel. A run holding a heap target reads request by request.
     */
    @Nested
    class VectoredReadTests {

        private static final int FILE_SIZE = 64 * 1024;

        private Path binaryFile;
        private byte[] fileBytes;

        @BeforeEach
        void writeBinaryFile() throws IOException {
            fileBytes = new byte[FILE_SIZE];
            for (int i = 0; i < FILE_SIZE; i++) {
                fileBytes[i] = (byte) (i * 31 + 7);
            }
            binaryFile = tempDir.resolve("vectored.bin");
            Files.write(binaryFile, fileBytes);
        }

        /**
         * {@code count} abutting requests of {@code length} bytes from {@code offset}, into slices of one direct
         * buffer: vectored reads take direct buffers only.
         */
        private List<RangeRequest> abutting(long offset, int count, int length) {
            ByteBuffer backing = ByteBuffer.allocateDirect(count * length);
            List<RangeRequest> requests = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ByteBuffer target = backing.slice(i * length, length);
                requests.add(RangeRequest.of(offset + (long) i * length, length, target));
            }
            return requests;
        }

        /**
         * {@code count} abutting requests of {@code length} bytes from {@code offset}, each into a heap buffer of its
         * own.
         */
        private List<RangeRequest> abuttingOnHeap(long offset, int count, int length) {
            List<RangeRequest> requests = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                requests.add(RangeRequest.of(offset + (long) i * length, length, ByteBuffer.allocate(length)));
            }
            return requests;
        }

        /**
         * Checks the counts, and that the targets hold the file bytes of their requests from 0 up to their positions.
         */
        private void assertLanded(List<RangeRequest> requests, int[] counts) {
            for (int i = 0; i < requests.size(); i++) {
                RangeRequest request = requests.get(i);
                int offset = (int) request.range().offset();
                int expected = Math.max(0, Math.min(request.range().length(), FILE_SIZE - offset));
                assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(expected);
                ByteBuffer target = request.target().duplicate().flip();
                byte[] landed = new byte[target.remaining()];
                target.get(landed);
                byte[] wanted = expected == 0 ? new byte[0] : Arrays.copyOfRange(fileBytes, offset, offset + expected);
                assertThat(landed).as("entry " + i).containsExactly(wanted);
            }
        }

        @Test
        void abuttingRequestsReadAsOneRun() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = abutting(100, 3, 10);

                BatchReadResult result = r.readRanges(requests);

                assertLanded(requests, counts(result));
                assertThat(result.fetches()).as("one run").isEqualTo(1);
                assertThat(result.bytesTransferred()).isEqualTo(30);
                assertThat(r.vectoredReads).hasValue(1);
                assertThat(r.positionalReads).hasValue(0);
            }
        }

        @Test
        void longRunTakes1024ViewsPerRead() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = abutting(0, 2_500, 8);

                BatchReadResult result = r.readRanges(requests);

                assertLanded(requests, counts(result));
                assertThat(result.fetches()).isEqualTo(1);
                assertThat(r.viewsPerRead).allMatch(views -> views <= FileRangeReader.MAX_VIEWS_PER_READ);
                assertThat(r.vectoredReads.get()).isGreaterThanOrEqualTo(3);
            }
        }

        @Test
        void requestsWithGapsReadOneByOne() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = List.of(
                        RangeRequest.of(0, 10, ByteBuffer.allocate(10)),
                        RangeRequest.of(20, 10, ByteBuffer.allocate(10)));

                BatchReadResult result = r.readRanges(requests);

                assertLanded(requests, counts(result));
                assertThat(result.fetches()).isEqualTo(2);
                assertThat(r.vectoredReads).hasValue(0);
            }
        }

        @Test
        void overlappingRequestsReadOnce() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = List.of(
                        RangeRequest.of(0, 20, ByteBuffer.allocate(20)),
                        RangeRequest.of(10, 20, ByteBuffer.allocate(20)));

                BatchReadResult result = r.readRanges(requests);

                assertLanded(requests, counts(result));
                assertThat(result.fetches()).isEqualTo(1);
                assertThat(r.vectoredReads).hasValue(0);
            }
        }

        @Test
        void runEndingPastEofStopsThere() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = abutting(FILE_SIZE - 20, 3, 15);

                int[] counts = counts(r.readRanges(requests));

                assertThat(counts).containsExactly(15, 5, 0);
                assertLanded(requests, counts);
            }
        }

        @Test
        void landsAtTheTargetPositions() throws IOException {
            ByteBuffer first = ByteBuffer.allocateDirect(40);
            first.position(5);
            ByteBuffer second = ByteBuffer.allocateDirect(40);
            second.position(3);
            List<RangeRequest> requests = List.of(RangeRequest.of(1000, 20, first), RangeRequest.of(1020, 20, second));
            try (CountingReader r = new CountingReader()) {
                int[] counts = counts(r.readRanges(requests));

                assertThat(counts).containsExactly(20, 20);
                assertThat(first.position()).isEqualTo(25);
                assertThat(second.position()).isEqualTo(23);
                assertThat(first.get(5)).isEqualTo(fileBytes[1000]);
                assertThat(second.get(3)).isEqualTo(fileBytes[1020]);
                assertThat(r.vectoredReads).hasValue(1);
            }
        }

        @Test
        void heapTargetsReadOneByOne() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = abuttingOnHeap(100, 3, 10);

                BatchReadResult result = r.readRanges(requests);

                assertLanded(requests, counts(result));
                assertThat(result.fetches()).as("one run").isEqualTo(1);
                assertThat(r.vectoredReads).hasValue(0);
                assertThat(r.positionalReads.get()).isGreaterThanOrEqualTo(requests.size());
                assertThat(r.opens).as("the shared channel only").hasValue(1);
            }
        }

        @Test
        void mixedTargetsReadOneByOne() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = List.of(
                        RangeRequest.of(200, 10, ByteBuffer.allocateDirect(10)),
                        RangeRequest.of(210, 10, ByteBuffer.allocate(10)));

                BatchReadResult result = r.readRanges(requests);

                assertLanded(requests, counts(result));
                assertThat(result.fetches()).as("one run").isEqualTo(1);
                assertThat(r.vectoredReads).hasValue(0);
                assertThat(r.positionalReads.get()).isGreaterThanOrEqualTo(requests.size());
            }
        }

        @Test
        void heapRunEndingPastEofStops() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = abuttingOnHeap(FILE_SIZE - 20, 3, 15);

                int[] counts = counts(r.readRanges(requests));

                assertThat(counts).containsExactly(15, 5, 0);
                assertLanded(requests, counts);
                assertThat(r.vectoredReads).hasValue(0);
            }
        }

        @Test
        void retryResumesAfterTheBytesLanded() throws IOException {
            AtomicInteger opens = new AtomicInteger();
            List<Long> positions = Collections.synchronizedList(new ArrayList<>());
            FileRangeReader r = new FileRangeReader(binaryFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    boolean stale = opens.incrementAndGet() == 1;
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        private int calls;

                        @Override
                        public FileChannel position(long newPosition) throws IOException {
                            positions.add(newPosition);
                            return delegate.position(newPosition);
                        }

                        @Override
                        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                            if (stale && ++calls > 1) {
                                throw new IOException("Stale file handle");
                            }
                            // the stale channel lands the first view, then fails
                            return delegate.read(dsts, offset, stale ? 1 : length);
                        }
                    };
                }
            };
            try (r) {
                List<RangeRequest> requests = abutting(0, 3, 10);

                assertLanded(requests, counts(r.readRanges(requests)));
                assertThat(opens).hasValue(2);
                assertThat(positions)
                        .as("the retry resumes after the bytes already landed")
                        .containsExactly(0L, 10L, 10L);
            }
        }

        @Test
        void unrecoverableFailureIsNotRetried() throws IOException {
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(binaryFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        @Override
                        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                            throw new IOException("Permission denied");
                        }
                    };
                }
            };
            try (r) {
                List<RangeRequest> requests = abutting(0, 3, 10);

                assertThatThrownBy(() -> r.readRanges(requests)).isInstanceOf(StorageException.class);
                assertThat(opens).hasValue(1);
            }
        }

        @Test
        @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
        void zeroByteReadFailsAfterThreeTries() throws IOException {
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(binaryFile, Duration.ZERO) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        @Override
                        public long read(ByteBuffer[] dsts, int offset, int length) {
                            return 0;
                        }
                    };
                }
            };
            try (r) {
                List<RangeRequest> requests = abutting(0, 3, 10);

                assertThatThrownBy(() -> r.readRanges(requests))
                        .isInstanceOf(StorageException.class)
                        .hasMessageContaining("3 attempts");
                assertThat(opens).hasValue(3);
            }
        }

        @Test
        void interruptedThreadReadsNothing() throws IOException {
            try (CountingReader r = new CountingReader()) {
                List<RangeRequest> requests = abutting(0, 3, 10);

                Thread.currentThread().interrupt();
                try {
                    assertThatThrownBy(() -> r.readRanges(requests))
                            .isInstanceOf(StorageException.class)
                            .hasCauseInstanceOf(InterruptedIOException.class);
                    assertThat(Thread.currentThread().isInterrupted())
                            .as("the interrupt flag stays set")
                            .isTrue();
                } finally {
                    Thread.interrupted();
                }
                assertThat(r.opens).hasValue(0);
            }
        }

        @Test
        void overlappingReadTakesAnExtraChannel() throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HoldingReader r = new HoldingReader(Duration.ZERO)) {
                List<RangeRequest> held = abutting(0, 4, 100);
                List<RangeRequest> overlapping = abutting(1000, 4, 100);
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(held));
                awaitQuietly(r.sharedReadStarted);

                BatchReadResult overlappingResult =
                        pool.submit(() -> r.readRanges(overlapping)).get(10, TimeUnit.SECONDS);
                r.releaseSharedRead.countDown();

                assertLanded(overlapping, counts(overlappingResult));
                assertLanded(held, counts(heldRead.get(10, TimeUnit.SECONDS)));
                assertThat(r.opens).as("the shared channel and one extra").hasValue(2);
                assertThat(r.extraChannels.get(0).isOpen())
                        .as("kept for the next vectored read")
                        .isTrue();
                assertThat(r.idleExtraChannelCount()).isEqualTo(1);
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void extraChannelIsReused() throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HoldingReader r = new HoldingReader(Duration.ZERO)) {
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(abutting(0, 4, 100)));
                awaitQuietly(r.sharedReadStarted);

                pool.submit(() -> r.readRanges(abutting(1000, 4, 100))).get(10, TimeUnit.SECONDS);
                List<RangeRequest> third = abutting(2000, 4, 100);
                assertLanded(
                        third, counts(pool.submit(() -> r.readRanges(third)).get(10, TimeUnit.SECONDS)));
                r.releaseSharedRead.countDown();
                heldRead.get(10, TimeUnit.SECONDS);

                assertThat(r.opens)
                        .as("the third read took the idle extra channel")
                        .hasValue(2);
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void atMostFourExtraChannelsStayIdle() throws Exception {
            int overlapping = FileRangeReader.MAX_IDLE_EXTRA_CHANNELS + 1;
            ExecutorService pool = Executors.newFixedThreadPool(overlapping + 1);
            try (HoldingReader r = new HoldingReader(Duration.ZERO)) {
                r.holdExtraReads(overlapping);
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(abutting(0, 4, 100)));
                awaitQuietly(r.sharedReadStarted);
                List<Future<BatchReadResult>> reads = new ArrayList<>();
                for (int i = 0; i < overlapping; i++) {
                    long offset = 1000L * (i + 1);
                    reads.add(pool.submit(() -> r.readRanges(abutting(offset, 4, 100))));
                }
                awaitQuietly(r.extraReadsStarted);

                r.releaseExtraReads.countDown();
                for (Future<BatchReadResult> read : reads) {
                    read.get(10, TimeUnit.SECONDS);
                }
                r.releaseSharedRead.countDown();
                heldRead.get(10, TimeUnit.SECONDS);

                assertThat(r.extraChannels).hasSize(overlapping);
                assertThat(r.idleExtraChannelCount()).isEqualTo(FileRangeReader.MAX_IDLE_EXTRA_CHANNELS);
                assertThat(r.extraChannels.stream().filter(FileChannel::isOpen))
                        .as("the extra channel given back to a full pool is closed")
                        .hasSize(FileRangeReader.MAX_IDLE_EXTRA_CHANNELS);
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void idleCheckClosesExtraChannels() throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HoldingReader r = new HoldingReader(Duration.ofMillis(200))) {
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(abutting(0, 4, 100)));
                awaitQuietly(r.sharedReadStarted);
                pool.submit(() -> r.readRanges(abutting(1000, 4, 100))).get(10, TimeUnit.SECONDS);
                r.releaseSharedRead.countDown();
                heldRead.get(10, TimeUnit.SECONDS);

                await().atMost(Duration.ofSeconds(5))
                        .pollInterval(Duration.ofMillis(50))
                        .untilAsserted(() -> {
                            assertThat(r.channel()).isNull();
                            assertThat(r.idleExtraChannelCount()).isZero();
                            assertThat(r.extraChannels.get(0).isOpen()).isFalse();
                        });
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void closeClosesExtraChannels() throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HoldingReader r = new HoldingReader(Duration.ZERO)) {
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(abutting(0, 4, 100)));
                awaitQuietly(r.sharedReadStarted);
                pool.submit(() -> r.readRanges(abutting(1000, 4, 100))).get(10, TimeUnit.SECONDS);
                r.releaseSharedRead.countDown();
                heldRead.get(10, TimeUnit.SECONDS);

                r.close();

                assertThat(r.idleExtraChannelCount()).isZero();
                assertThat(r.extraChannels.get(0).isOpen()).isFalse();
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void channelBackAfterCloseIsClosed() throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HoldingReader r = new HoldingReader(Duration.ZERO)) {
                r.holdExtraReads(1);
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(abutting(0, 4, 100)));
                awaitQuietly(r.sharedReadStarted);
                Future<BatchReadResult> extraRead = pool.submit(() -> r.readRanges(abutting(1000, 4, 100)));
                awaitQuietly(r.extraReadsStarted);

                r.close();
                r.releaseExtraReads.countDown();
                extraRead.get(10, TimeUnit.SECONDS);

                assertThat(r.idleExtraChannelCount()).isZero();
                assertThat(r.extraChannels.get(0).isOpen()).isFalse();
                r.releaseSharedRead.countDown();
                assertThatThrownBy(() -> heldRead.get(10, TimeUnit.SECONDS))
                        .as("the shared channel closed under the held read")
                        .isInstanceOf(ExecutionException.class);
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void interruptOnAnExtraChannelSparesShared() throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (HoldingReader r = new HoldingReader(Duration.ZERO)) {
                r.interruptExtraReads = true;
                Future<BatchReadResult> heldRead = pool.submit(() -> r.readRanges(abutting(0, 4, 100)));
                awaitQuietly(r.sharedReadStarted);
                FileChannel shared = r.channel();

                Future<BatchReadResult> interrupted = pool.submit(() -> r.readRanges(abutting(1000, 4, 100)));

                assertThatThrownBy(() -> interrupted.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .isInstanceOf(StorageException.class)
                        .hasCauseInstanceOf(ClosedByInterruptException.class);
                assertThat(shared.isOpen()).as("the shared channel").isTrue();
                assertThat(r.idleExtraChannelCount())
                        .as("a failed channel is never kept")
                        .isZero();
                r.releaseSharedRead.countDown();
                heldRead.get(10, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }
        }

        // Single reads keep their positional reads while vectored reads of the same file move a channel's position
        @Test
        void singleReadsBesideVectoredReads() throws Exception {
            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try (FileRangeReader r = new FileRangeReader(binaryFile, Duration.ZERO)) {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Void>> tasks = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    long base = t * 8_000L;
                    boolean batches = t % 2 == 0;
                    tasks.add(pool.submit(() -> {
                        start.await();
                        for (int round = 0; round < 50; round++) {
                            List<RangeRequest> requests = abutting(base + round * 13L, batches ? 16 : 1, 64);
                            int[] counts = batches
                                    ? counts(r.readRanges(requests))
                                    : new int[] {
                                        r.readRange(
                                                requests.get(0).range(),
                                                requests.get(0).target())
                                    };
                            assertLanded(requests, counts);
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<Void> task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }
        }

        @Test
        void slowRunKeepsItsChannel() throws IOException {
            Duration idleTimeout = Duration.ofMillis(100);
            AtomicInteger opens = new AtomicInteger();
            FileRangeReader r = new FileRangeReader(binaryFile, idleTimeout) {
                @Override
                FileChannel openChannel(Path path) throws IOException {
                    opens.incrementAndGet();
                    return new ForwardingFileChannel(super.openChannel(path)) {
                        @Override
                        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                            sleepQuietly(3 * idleTimeout.toMillis());
                            return delegate.read(dsts, offset, length);
                        }
                    };
                }
            };
            try (r) {
                List<RangeRequest> requests = abutting(0, 3, 10);

                assertLanded(requests, counts(r.readRanges(requests)));
                assertThat(opens).hasValue(1);
            }
        }

        @Test
        void eightThreadsReadBatchesAtOnce() throws Exception {
            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try (FileRangeReader r = new FileRangeReader(binaryFile, Duration.ZERO)) {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Void>> tasks = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    long base = t * 8_000L;
                    tasks.add(pool.submit(() -> {
                        start.await();
                        for (int round = 0; round < 20; round++) {
                            List<RangeRequest> requests = abutting(base + round * 7L, 16, 64);
                            assertLanded(requests, counts(r.readRanges(requests)));
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<Void> task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
                assertThat(r.idleExtraChannelCount()).isLessThanOrEqualTo(FileRangeReader.MAX_IDLE_EXTRA_CHANNELS);
            } finally {
                pool.shutdownNow();
            }
        }

        /** Counts the vectored and the positional reads of its channels. */
        private final class CountingReader extends FileRangeReader {
            final AtomicInteger opens = new AtomicInteger();
            final AtomicInteger vectoredReads = new AtomicInteger();
            final AtomicInteger positionalReads = new AtomicInteger();
            final List<Integer> viewsPerRead = Collections.synchronizedList(new ArrayList<>());

            CountingReader() throws IOException {
                super(binaryFile, Duration.ZERO);
            }

            @Override
            FileChannel openChannel(Path path) throws IOException {
                opens.incrementAndGet();
                return new ForwardingFileChannel(super.openChannel(path)) {
                    @Override
                    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                        vectoredReads.incrementAndGet();
                        viewsPerRead.add(length);
                        return delegate.read(dsts, offset, length);
                    }

                    @Override
                    public int read(ByteBuffer dst, long position) throws IOException {
                        positionalReads.incrementAndGet();
                        return delegate.read(dst, position);
                    }
                };
            }
        }

        /**
         * Holds the vectored reads of its first channel, the shared one, until released: a batch overlapping the held
         * one finds the shared channel taken and reads on an extra channel. Extra channels can hold their reads too, or
         * interrupt them.
         */
        private final class HoldingReader extends FileRangeReader {
            final AtomicInteger opens = new AtomicInteger();
            final List<FileChannel> extraChannels = Collections.synchronizedList(new ArrayList<>());
            final CountDownLatch sharedReadStarted = new CountDownLatch(1);
            final CountDownLatch releaseSharedRead = new CountDownLatch(1);
            final CountDownLatch releaseExtraReads = new CountDownLatch(1);
            volatile CountDownLatch extraReadsStarted;
            volatile boolean interruptExtraReads;

            HoldingReader(Duration idleTimeout) throws IOException {
                super(binaryFile, idleTimeout);
            }

            /**
             * Holds the reads of extra channels until {@code count} of them have started and the test releases them.
             */
            void holdExtraReads(int count) {
                extraReadsStarted = new CountDownLatch(count);
            }

            @Override
            FileChannel openChannel(Path path) throws IOException {
                FileChannel real = super.openChannel(path);
                if (opens.incrementAndGet() == 1) {
                    return sharedChannel(real);
                }
                extraChannels.add(real);
                return extraChannel(real);
            }

            private FileChannel sharedChannel(FileChannel real) {
                return new ForwardingFileChannel(real) {
                    @Override
                    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                        sharedReadStarted.countDown();
                        awaitQuietly(releaseSharedRead);
                        return delegate.read(dsts, offset, length);
                    }
                };
            }

            private FileChannel extraChannel(FileChannel real) {
                return new ForwardingFileChannel(real) {
                    @Override
                    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                        CountDownLatch started = extraReadsStarted;
                        if (started != null) {
                            started.countDown();
                            awaitQuietly(releaseExtraReads);
                        }
                        if (interruptExtraReads) {
                            Thread.currentThread().interrupt();
                        }
                        return delegate.read(dsts, offset, length);
                    }
                };
            }
        }
    }

    private static void awaitQuietly(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("java:S2925") // a forwarding channel whose read outlasts the idle timeout needs a real delay
    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * FileChannel wrapper that delegates all operations to a real channel. Tests override specific methods to inject
     * failures.
     */
    private static class ForwardingFileChannel extends FileChannel {
        final FileChannel delegate;

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
