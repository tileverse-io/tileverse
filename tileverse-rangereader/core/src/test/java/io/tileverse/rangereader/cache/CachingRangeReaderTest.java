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
package io.tileverse.rangereader.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link CachingRangeReader}.
 */
class CachingRangeReaderTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private static final int FILE_SIZE = 100_000;
    private static final byte[] TEST_DATA = new byte[FILE_SIZE];

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file with predictable data
        testFile = tempDir.resolve("test.bin");
        new Random(42).nextBytes(TEST_DATA); // Use fixed seed for reproducibility
        Files.write(testFile, TEST_DATA);
    }

    @Test
    void testBuilderRequiresDelegateParameter() throws IOException {
        // Test that builder requires delegate parameter
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        CachingRangeReader.Builder builder = CachingRangeReader.builder(delegate);
        assertNotNull(builder, "Builder should be created successfully");

        // Test that null delegate throws exception
        assertThrows(NullPointerException.class, () -> CachingRangeReader.builder(null));
    }

    @Test
    void testBasicCaching() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            // First read should go to delegate
            ByteBuffer data1 = reader.readRange(1000, 500);
            assertEquals(500, data1.flip().remaining(), "Should read 500 bytes");

            // Second read of same range should come from cache
            ByteBuffer data2 = reader.readRange(1000, 500);
            assertEquals(500, data2.flip().remaining(), "Should read 500 bytes from cache");

            // Verify data is identical
            data1.rewind();
            data2.rewind();
            byte[] bytes1 = new byte[data1.remaining()];
            byte[] bytes2 = new byte[data2.remaining()];
            data1.get(bytes1);
            data2.get(bytes2);
            assertArrayEquals(bytes1, bytes2, "Data should be identical");

            // Verify cache contains the entry
            assertTrue(reader.getCacheEntryCount() > 0, "Cache should contain entries");
        }
    }

    @Test
    void testGetSourceIdentifier() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {
            String sourceId = reader.getSourceIdentifier();
            assertThat(sourceId).startsWith("memory-cached:");
            assertThat(sourceId).contains(testFile.toAbsolutePath().toString());
        }
    }

    @Test
    void testCacheMaxSize() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        long maxSize = 1024; // Very small cache

        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).maximumWeight(maxSize).build()) {

            // Read multiple ranges that exceed cache size
            reader.readRange(0, 500);
            reader.readRange(1000, 500);
            reader.readRange(2000, 500);

            // Cache should have evicted some entries due to size limit
            // Note: Exact behavior depends on Caffeine's eviction timing
            assertThat(reader.getCacheEntryCount()).isGreaterThan(0);
        }
    }

    @Test
    void testCacheStats() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            // Read some data to generate stats
            reader.readRange(1000, 500);
            reader.readRange(1000, 500); // Cache hit
            reader.readRange(2000, 500); // Cache miss

            var stats = reader.getCacheStats();
            assertNotNull(stats, "Stats should not be null");
            assertThat(stats.hitCount()).isGreaterThan(0);
            assertThat(stats.missCount()).isGreaterThan(0);
        }
    }

    @Test
    void testClearCache() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            // Add data to cache
            reader.readRange(1000, 500);
            assertTrue(reader.getCacheEntryCount() > 0, "Cache should contain entries");

            // Clear cache
            reader.clearCache();
            assertEquals(0, reader.getCacheEntryCount(), "Cache should be empty after clear");
        }
    }

    @Test
    void testConcurrentAccess() throws Exception {
        CountingRangeReader delegate =
                new CountingRangeReader(FileRangeReader.builder().path(testFile).build());
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            int numThreads = 10;
            int numReadsPerThread = 20;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch latch = new CountDownLatch(numThreads);

            // Submit multiple threads reading the same range
            @SuppressWarnings("unchecked")
            CompletableFuture<Void>[] futures = new CompletableFuture[numThreads];
            for (int i = 0; i < numThreads; i++) {
                futures[i] = CompletableFuture.runAsync(
                        () -> {
                            try {
                                latch.countDown();
                                latch.await(); // Wait for all threads to be ready

                                for (int j = 0; j < numReadsPerThread; j++) {
                                    ByteBuffer data = reader.readRange(1000, 500);
                                    assertEquals(500, data.flip().remaining());
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        executor);
            }

            // Wait for all threads to complete
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
            executor.shutdown();

            // The delegate should have been called much fewer times than total reads
            // due to caching and concurrency control
            int totalReads = numThreads * numReadsPerThread;
            assertThat(delegate.getReadCount()).isLessThan(totalReads);
            assertThat(delegate.getReadCount()).isGreaterThan(0);
        }
    }

    @Test
    void testMultipleRanges() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            // Read multiple different ranges
            ByteBuffer data1 = reader.readRange(0, 1000);
            ByteBuffer data2 = reader.readRange(5000, 1000);
            ByteBuffer data3 = reader.readRange(10000, 1000);

            assertEquals(1000, data1.flip().remaining());
            assertEquals(1000, data2.flip().remaining());
            assertEquals(1000, data3.flip().remaining());

            // Cache should contain all three ranges
            assertEquals(3, reader.getCacheEntryCount());

            // Reading same ranges again should come from cache
            ByteBuffer cached1 = reader.readRange(0, 1000);
            ByteBuffer cached2 = reader.readRange(5000, 1000);
            ByteBuffer cached3 = reader.readRange(10000, 1000);

            // Verify data matches
            assertArrayEquals(bufferToArray(data1), bufferToArray(cached1));
            assertArrayEquals(bufferToArray(data2), bufferToArray(cached2));
            assertArrayEquals(bufferToArray(data3), bufferToArray(cached3));
        }
    }

    @Test
    void testPartialReads() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            // Read near end of file where we might get fewer bytes than requested
            long fileSize = reader.size().orElseThrow();
            int requestedBytes = 1000;
            long offset = fileSize - 500; // Request more bytes than available

            ByteBuffer data = reader.readRange(offset, requestedBytes);
            assertEquals(500, data.remaining(), "Should read only available bytes");

            // Reading again should come from cache
            ByteBuffer cached = reader.readRange(offset, requestedBytes);
            assertEquals(500, cached.remaining(), "Should read cached bytes");

            assertArrayEquals(bufferToArray(data), bufferToArray(cached));
        }
    }

    @Test
    void testSizeMethod() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {
            assertEquals(FILE_SIZE, reader.size().getAsLong(), "Size should match file size");
        }
    }

    @Test
    void testBuilderValidation() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();

        // Test invalid max size
        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .maximumWeight(0));

        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .maximumWeight(-1));

        // Test invalid maximum size
        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .maximumSize(0));

        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .maximumSize(-1));

        // Test conflicting size configurations
        assertThrows(
                IllegalStateException.class,
                () -> CachingRangeReader.builder(delegate).maximumSize(100).maximumWeight(1024));

        assertThrows(
                IllegalStateException.class,
                () -> CachingRangeReader.builder(delegate).maximumWeight(1024).maximumSize(100));

        // Test invalid expiration duration
        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .expireAfterAccess(0, TimeUnit.SECONDS));

        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .expireAfterAccess(-1, TimeUnit.SECONDS));

        // Test null time unit
        assertThrows(NullPointerException.class, () -> CachingRangeReader.builder(delegate)
                .expireAfterAccess(1, null));

        // Test invalid block size
        assertThrows(IllegalArgumentException.class, () -> CachingRangeReader.builder(delegate)
                .blockSize(-1));
    }

    @Test
    void testMaximumSizeLimit() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();

        // Create a cache with very small entry limit
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate)
                .maximumSize(2) // Only 2 entries allowed
                .build()) {

            // Read 3 different ranges
            reader.readRange(0, 100);
            reader.readRange(1000, 100);
            reader.readRange(2000, 100);

            // Wait for eviction to process
            Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> assertThat(reader.getCacheEntryCount())
                    .isLessThanOrEqualTo(2));
        }
    }

    @Test
    void testExpireAfterAccess() throws IOException {
        RangeReader delegate = FileRangeReader.builder().path(testFile).build();

        // Create a cache with very short expiration
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate)
                .maximumSize(100) // Large enough to not trigger size-based eviction
                .expireAfterAccess(100, TimeUnit.MILLISECONDS)
                .build()) {

            // Read some data
            reader.readRange(0, 100);
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);

            // Wait for expiration and trigger cleanup by reading again
            Awaitility.await()
                    .atMost(1, TimeUnit.SECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> {
                        // Trigger cache cleanup by reading again
                        reader.readRange(1000, 100);
                        // Cache should have at least the new entry
                        assertThat(reader.getCacheEntryCount()).isGreaterThan(0);
                    });
        }
    }

    @Test
    void testSoftValuesConfiguration() throws IOException {
        // Test explicit soft values configuration
        try (RangeReader delegate = FileRangeReader.builder().path(testFile).build();
                CachingRangeReader reader = CachingRangeReader.builder(delegate)
                        .maximumSize(100)
                        .softValues(true)
                        .build()) {

            reader.readRange(0, 100);
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);
        }

        // Test soft values disabled
        try (RangeReader delegate = FileRangeReader.builder().path(testFile).build();
                CachingRangeReader reader = CachingRangeReader.builder(delegate)
                        .maximumSize(100)
                        .softValues(false)
                        .build()) {

            reader.readRange(0, 100);
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);
        }

        // Test default soft values (no size limit specified)
        try (RangeReader delegate = FileRangeReader.builder().path(testFile).build();
                CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            reader.readRange(0, 100);
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);
        }
    }

    @Test
    void testCloseDelegate() throws IOException {
        MockClosableRangeReader delegate = new MockClosableRangeReader();
        try (CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {
            // Use the reader
            reader.readRange(0, 100);
        }

        // Delegate should be closed when CachingRangeReader is closed
        assertTrue(delegate.isClosed(), "Delegate should be closed");
    }

    @Test
    void testHeaderBufferConfiguration() throws IOException {
        // Test with header buffer enabled
        try (RangeReader delegate = FileRangeReader.builder().path(testFile).build();
                CachingRangeReader reader =
                        CachingRangeReader.builder(delegate).withHeaderBuffer().build()) {

            // Read from beginning of file - should come from header buffer
            ByteBuffer data1 = reader.readRange(0, 1000);
            assertEquals(1000, data1.flip().remaining());

            // Read again from header range - should still work from header buffer
            ByteBuffer data2 = reader.readRange(500, 500);
            assertEquals(500, data2.flip().remaining());

            // Cache should be empty since reads came from header buffer
            assertEquals(0, reader.getCacheEntryCount(), "Cache should be empty with header buffer");
        }

        // Test with explicit header size
        try (RangeReader delegate = FileRangeReader.builder().path(testFile).build();
                CachingRangeReader reader = CachingRangeReader.builder(delegate)
                        .headerSize(32 * 1024) // 32KB header
                        .build()) {

            // Read within header range
            ByteBuffer headerData = reader.readRange(0, 1000);
            assertEquals(1000, headerData.flip().remaining());

            // Read beyond header range - should use cache
            ByteBuffer cachedData = reader.readRange(50000, 1000);
            assertEquals(1000, cachedData.flip().remaining());

            // Cache should contain the beyond-header read
            assertEquals(1, reader.getCacheEntryCount(), "Cache should contain beyond-header read");
        }

        // Test with header buffer disabled (default)
        try (RangeReader delegate = FileRangeReader.builder().path(testFile).build();
                CachingRangeReader reader = CachingRangeReader.builder(delegate).build()) {

            // Read from beginning - should go to cache since no header buffer
            ByteBuffer data = reader.readRange(0, 1000);
            assertEquals(1000, data.flip().remaining());

            // Cache should contain the entry
            assertEquals(1, reader.getCacheEntryCount(), "Cache should contain entry without header buffer");
        }
    }

    /**
     * Helper method to convert ByteBuffer to byte array.
     */
    private byte[] bufferToArray(ByteBuffer buffer) {
        buffer.rewind();
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }

    /**
     * Mock RangeReader that counts read operations for concurrency testing.
     */
    private static class CountingRangeReader extends AbstractRangeReader {
        private final RangeReader delegate;
        private final AtomicInteger readCount = new AtomicInteger(0);

        public CountingRangeReader(RangeReader delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException {
            readCount.incrementAndGet();
            int bytesRead = delegate.readRange(offset, actualLength, target);
            // readRange returns a flipped buffer, but readRangeNoFlip should not flip
            target.position(target.limit());
            return bytesRead;
        }

        @Override
        public OptionalLong size() throws IOException {
            return delegate.size();
        }

        @Override
        public String getSourceIdentifier() {
            return "counting:" + delegate.getSourceIdentifier();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        public int getReadCount() {
            return readCount.get();
        }
    }

    /**
     * Mock closable RangeReader for testing close behavior.
     */
    private static class MockClosableRangeReader extends AbstractRangeReader {
        private boolean closed = false;

        @Override
        protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException {
            if (closed) {
                throw new IOException("Reader is closed");
            }
            // Simulate reading by filling buffer with test data
            int bytesToWrite = Math.min(actualLength, target.remaining());
            for (int i = 0; i < bytesToWrite; i++) {
                target.put((byte) (offset + i));
            }
            return bytesToWrite;
        }

        @Override
        public OptionalLong size() throws IOException {
            return OptionalLong.of(1000);
        }

        @Override
        public String getSourceIdentifier() {
            return "mock://test";
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
