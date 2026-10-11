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
package io.tileverse.storage.cache;

import static io.tileverse.storage.RangeReaderTestSupport.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.Scheduler;
import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.CaffeineCache;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.ByteArrayRangeReader;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.cache.PausingRangeReaderCache.Pause;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link CachingRangeReader}. */
class CachingRangeReaderTest {

    @TempDir
    private Path tempDir;

    private Path testFile;
    private static final int FILE_SIZE = 100_000;
    private static final byte[] TEST_DATA = new byte[FILE_SIZE];
    private CacheManager cacheManager;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file with predictable data
        testFile = tempDir.resolve("test.bin");
        new Random(42).nextBytes(TEST_DATA); // Use fixed seed for reproducibility
        Files.write(testFile, TEST_DATA);
        cacheManager = CacheManager.newInstance();
    }

    @AfterEach
    void cleanUp() {
        cacheManager.invalidateAll();
    }

    @Test
    void testBuilderRequiresDelegateParameter() {
        // Test that builder requires delegate parameter
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        CachingRangeReader.Builder builder = CachingRangeReader.builder(delegate);
        assertNotNull(builder, "Builder should be created successfully");

        assertThatThrownBy(() -> CachingRangeReader.builder(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testBasicCaching() throws IOException {
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {
            String sourceId = reader.getSourceIdentifier();
            assertThat(sourceId).isEqualTo(testFile.toRealPath().toString());
        }
    }

    /**
     * The cache is partitioned by the delegate's source identifier: delegates with different identifiers never see each
     * other's bytes, and delegates with the same identifier share one partition. The second half is what turns an
     * identifier collision (the same bucket and key on two endpoints) into one source serving the other's bytes.
     */
    @Test
    void cachePartitionsFollowTheSourceIdentifier() throws IOException {
        RangeReader first = new ConstantBytesRangeReader("id://first", (byte) 1);
        RangeReader second = new ConstantBytesRangeReader("id://second", (byte) 2);
        RangeReader sameIdentifierAsFirst = new ConstantBytesRangeReader("id://first", (byte) 3);

        try (CachingRangeReader cachedFirst = cached(first);
                CachingRangeReader cachedSecond = cached(second);
                CachingRangeReader cachedSameIdentifier = cached(sameIdentifierAsFirst)) {
            assertThat(cachedFirst.readRange(0, 4).get(0)).isEqualTo((byte) 1);
            assertThat(cachedSecond.readRange(0, 4).get(0))
                    .as("a different identifier reads its own partition")
                    .isEqualTo((byte) 2);
            assertThat(cachedSameIdentifier.readRange(0, 4).get(0))
                    .as("an equal identifier shares the partition and is served the first source's bytes")
                    .isEqualTo((byte) 1);
        }
    }

    private CachingRangeReader cached(RangeReader delegate) {
        return CachingRangeReader.builder(delegate).cacheManager(cacheManager).build();
    }

    @Test
    void testCacheMaxSize() throws IOException {
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);

        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

            // Read some data to generate stats
            reader.readRange(1000, 500);
            reader.readRange(1000, 500); // Cache hit
            reader.readRange(2000, 500); // Cache miss

            CacheStats stats = reader.getCacheStats();
            assertNotNull(stats, "Stats should not be null");
            assertThat(stats.hitCount()).isGreaterThan(0);
            assertThat(stats.missCount()).isGreaterThan(0);
        }
    }

    @Test
    void testClearCache() throws IOException {
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
        CountingRangeReader delegate = new CountingRangeReader(RangeReaderTestSupport.fileReader(testFile));
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader = CachingRangeReader.of(delegate)) {
            assertEquals(FILE_SIZE, reader.size().getAsLong(), "Size should match file size");
        }
    }

    @Test
    void ofBuildsACachingReaderOverTheDelegate() throws IOException {
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);
        try (CachingRangeReader reader = CachingRangeReader.of(delegate)) {
            ByteBuffer data = reader.readRange(0, 100);
            assertEquals(100, data.flip().remaining());
        }
    }

    @Test
    void testExpireAfterAccess() throws IOException {
        RangeReader delegate = RangeReaderTestSupport.fileReader(testFile);

        assertThat(cacheManager.getCacheNames()).doesNotContain(ByteRangeCache.SHARED_CACHE_NAME);

        // Create a cache with very short expiration
        @SuppressWarnings("unused")
        Cache<ByteRange, ByteBuffer> cache = cacheManager.getCache(ByteRangeCache.SHARED_CACHE_NAME, () -> {
            return CaffeineCache.<ByteRange, ByteBuffer>newBuilder()
                    .expireAfterAccess(Duration.ofMillis(100))
                    .maxHeapPercent(10, ByteRangeCache::weigh)
                    .averageWeight(64 * 1024)
                    // run eviction tasks when idle to return memory without waiting for cache usage
                    .scheduler(Scheduler.systemScheduler())
                    .recordStats()
                    .build();
        });

        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

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
    void testCloseDelegate() throws IOException {
        MockClosableRangeReader delegate = new MockClosableRangeReader();
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {
            // Use the reader
            reader.readRange(0, 100);
        }

        // Delegate should be closed when CachingRangeReader is closed
        assertTrue(delegate.isClosed(), "Delegate should be closed");
    }

    @Test
    void readRangesForwardsOnlyMissesAsOneBatch() throws IOException {
        ByteArrayRangeReader delegate = new ByteArrayRangeReader(TEST_DATA);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

            // warm two of the five ranges
            reader.readRange(0, 100);
            reader.readRange(5000, 200);
            delegate.clearRecordings();

            List<RangeRequest> requests = List.of(
                    RangeRequest.of(0, 100, ByteBuffer.allocate(100)),
                    RangeRequest.of(1000, 50, ByteBuffer.allocate(50)),
                    RangeRequest.of(5000, 200, ByteBuffer.allocate(200)),
                    RangeRequest.of(2000, 25, ByteBuffer.allocate(25)),
                    RangeRequest.of(1000, 50, ByteBuffer.allocate(50))); // duplicate of entry 1

            BatchReadResult result = reader.readRanges(requests);

            assertThat(counts(result)).containsExactly(100, 50, 200, 25, 50);
            // one batch reached the delegate, with the two distinct misses only
            assertThat(delegate.batchReads())
                    .containsExactly(List.of(new ByteRange(1000, 50), new ByteRange(2000, 25)));
            // the two hits and the repeated miss came from the cache; the delegate fetched the two distinct misses
            assertThat(result.bytesRequested()).isEqualTo(425);
            assertThat(result.bytesFromCache()).isEqualTo(350);
            assertThat(result.fetches()).isEqualTo(2);
            assertThat(result.bytesTransferred()).isEqualTo(75);
            for (RangeRequest request : requests) {
                ByteBuffer expected = ByteBuffer.wrap(TEST_DATA)
                        .position((int) request.range().offset())
                        .limit((int) request.range().end());
                assertThat(request.target().flip()).isEqualTo(expected);
            }
            // the four distinct ranges are cached; the duplicate request shares a key
            assertThat(reader.getCacheEntryCount()).isEqualTo(4);
        }
    }

    @Test
    void readRangesAllHitsSkipTheDelegate() throws IOException {
        ByteArrayRangeReader delegate = new ByteArrayRangeReader(TEST_DATA);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {
            reader.readRange(100, 40);
            delegate.clearRecordings();

            BatchReadResult result = reader.readRanges(List.of(RangeRequest.of(100, 40, ByteBuffer.allocate(40))));

            assertThat(counts(result)).containsExactly(40);
            assertThat(delegate.batchReads()).isEmpty();
            assertThat(delegate.singleReads()).isEmpty();
            assertThat(result.bytesFromCache()).isEqualTo(40);
            assertThat(result.fetches()).isZero();
            assertThat(result.bytesTransferred()).isZero();
        }
    }

    @Test
    void readRangesTrimsShortReadsAtEof() throws IOException {
        ByteArrayRangeReader delegate = new ByteArrayRangeReader(TEST_DATA);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

            ByteBuffer straddling = ByteBuffer.allocate(100);
            ByteBuffer past = ByteBuffer.allocate(10);
            int[] read = counts(reader.readRanges(List.of(
                    RangeRequest.of(FILE_SIZE - 30, 100, straddling), RangeRequest.of(FILE_SIZE + 1, 10, past))));

            assertThat(read).containsExactly(30, 0);
            assertThat(straddling.position()).isEqualTo(30);
            // a repeat straddling read is a cache hit
            delegate.clearRecordings();
            int[] again =
                    counts(reader.readRanges(List.of(RangeRequest.of(FILE_SIZE - 30, 100, ByteBuffer.allocate(100)))));
            assertThat(again).containsExactly(30);
            assertThat(delegate.batchReads()).isEmpty();
        }
    }

    @Test
    void statsCountARangeOncePerRead() throws IOException {
        ByteArrayRangeReader delegate = new ByteArrayRangeReader(TEST_DATA);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

            reader.readRanges(List.of(
                    RangeRequest.of(0, 100, ByteBuffer.allocate(100)),
                    RangeRequest.of(1000, 50, ByteBuffer.allocate(50)),
                    RangeRequest.of(0, 100, ByteBuffer.allocate(100)))); // repeats the first range
            CacheStats cold = reader.getCacheStats();

            reader.readRanges(List.of(
                    RangeRequest.of(0, 100, ByteBuffer.allocate(100)),
                    RangeRequest.of(1000, 50, ByteBuffer.allocate(50))));
            reader.readRange(0, 100);
            CacheStats warm = reader.getCacheStats();

            assertThat(cold.missCount()).isEqualTo(2);
            assertThat(cold.hitCount()).isZero();
            assertThat(cold.loadCount()).isEqualTo(2);
            assertThat(warm.missCount()).isEqualTo(2);
            assertThat(warm.hitCount()).isEqualTo(3);
        }
    }

    @Test
    void singleReadStaysSingle() throws IOException {
        ByteArrayRangeReader delegate = new ByteArrayRangeReader(TEST_DATA);
        try (CachingRangeReader reader =
                CachingRangeReader.builder(delegate).cacheManager(cacheManager).build()) {

            reader.readRange(1000, 50);

            assertThat(delegate.singleReads()).containsExactly(new ByteRange(1000, 50));
            assertThat(delegate.batchReads()).isEmpty();
        }
    }

    /** Reads through one reader needing the same ranges at the same time. */
    @Nested
    class ConcurrentReads {

        private static final ByteRange A = new ByteRange(0, 100);
        private static final ByteRange B = new ByteRange(1_000, 50);
        private static final ByteRange C = new ByteRange(5_000, 200);
        private static final ByteRange STRADDLING_THE_END = new ByteRange(FILE_SIZE - 30, 100);

        private ScriptedRangeReader delegate;
        private PausingRangeReaderCache cache;
        private BatchRangeLoader loader;
        private CachingRangeReader reader;

        @BeforeEach
        void openReader() {
            delegate = new ScriptedRangeReader(TEST_DATA);
            cache = new PausingRangeReaderCache(cacheManager, delegate.getSourceIdentifier());
            // a wait far longer than the caps of the helpers: a read of a scripted scenario never gives up on another
            // read
            loader = new BatchRangeLoader(cache, Duration.ofMinutes(1));
            reader = new CachingRangeReader(delegate, cache, loader);
        }

        @AfterEach
        void checkNothingLeftClaimed() {
            assertThat(loader.pendingLoads())
                    .as("ranges left claimed by the scenario")
                    .isZero();
        }

        @Test
        void overlappingBatchesShareAFetch() throws Exception {
            List<RangeRequest> firstBatch = requests(A, B);
            List<RangeRequest> secondBatch = requests(B, C, B, new ByteRange(7_000, 0));
            delegate.hold(1);

            RunningRead<BatchReadResult> first = RunningRead.start(() -> reader.readRanges(firstBatch));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> second = RunningRead.start(() -> reader.readRanges(secondBatch));
            second.awaitWaitingForAnotherRead(); // it fetched C and waits for B
            delegate.release(1);
            BatchReadResult firstResult = first.result();
            BatchReadResult secondResult = second.result();

            assertThat(delegate.calls()).containsExactly(List.of(A, B), List.of(C));
            assertThat(counts(firstResult)).containsExactly(100, 50);
            assertThat(counts(secondResult)).containsExactly(50, 200, 50, 0);
            assertHoldsTheBytesOfItsRange(firstBatch);
            assertHoldsTheBytesOfItsRange(secondBatch);
            assertThat(firstResult.bytesFromCache()).isZero();
            assertThat(firstResult.fetches()).isEqualTo(2);
            assertThat(secondResult.bytesFromCache()).isEqualTo(100);
            assertThat(secondResult.fetches()).isEqualTo(1);
            assertThat(secondResult.bytesTransferred()).isEqualTo(200);
            CacheStats stats = reader.getCacheStats();
            assertThat(stats.missCount()).isEqualTo(3);
            assertThat(stats.hitCount()).isEqualTo(1);
        }

        @Test
        void singleReadWaitsForBatch() throws Exception {
            delegate.hold(1);

            RunningRead<BatchReadResult> batch = RunningRead.start(() -> reader.readRanges(requests(A, B)));
            delegate.awaitCall(1);
            RunningRead<ByteBuffer> single = RunningRead.start(() -> reader.readRange(B.offset(), B.length()));
            single.awaitWaitingForAnotherRead();
            delegate.release(1);
            batch.result();
            ByteBuffer read = single.result();

            assertThat(read.flip()).isEqualTo(bytesOf(B));
            assertThat(delegate.calls()).containsExactly(List.of(A, B));
        }

        @Test
        void batchWaitsForSingleRead() throws Exception {
            List<RangeRequest> batchRequests = requests(A, C);
            delegate.hold(1);

            RunningRead<ByteBuffer> single = RunningRead.start(() -> reader.readRange(A.offset(), A.length()));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> batch = RunningRead.start(() -> reader.readRanges(batchRequests));
            batch.awaitWaitingForAnotherRead(); // it fetched C and waits for A
            delegate.release(1);
            single.result();
            BatchReadResult batchResult = batch.result();

            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(C));
            assertThat(counts(batchResult)).containsExactly(100, 200);
            assertHoldsTheBytesOfItsRange(batchRequests);
            assertThat(batchResult.bytesFromCache()).isEqualTo(100);
        }

        @ParameterizedTest
        @MethodSource("io.tileverse.storage.cache.CachingRangeReaderTest#failuresOfTheFirstFetch")
        void waiterRetriesAfterFailedFetch(Throwable firstFailure) throws Exception {
            List<RangeRequest> waiterRequests = requests(A);
            delegate.hold(1);
            delegate.fail(1, firstFailure);

            RunningRead<BatchReadResult> failing = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> waiter = RunningRead.start(() -> reader.readRanges(waiterRequests));
            waiter.awaitWaitingForAnotherRead();
            delegate.release(1);

            assertThat(failing.failure()).isSameAs(firstFailure);
            assertThat(counts(waiter.result())).containsExactly(100);
            assertHoldsTheBytesOfItsRange(waiterRequests);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
        }

        /**
         * The waiter loses its first fetch, retries, and finds the range claimed by a newcomer. The fetch of the
         * newcomer fails too: the waiter fails with that failure.
         */
        @ParameterizedTest
        @MethodSource("io.tileverse.storage.cache.CachingRangeReaderTest#failuresOfTheNewerFetch")
        void waiterFailsWithNewerFailure(Throwable secondFailure) throws Exception {
            StorageException firstFailure = new StorageException("first failure");
            delegate.hold(1);
            delegate.fail(1, firstFailure);
            delegate.hold(2);
            delegate.fail(2, secondFailure);

            RunningRead<BatchReadResult> failing = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> waiter = RunningRead.start(() -> reader.readRanges(requests(A)));
            waiter.awaitWaitingForAnotherRead();
            Pause beforeTheWaiterRetries = cache.pauseNextLookupBy(waiter.thread());
            delegate.release(1);
            beforeTheWaiterRetries.awaitReached();
            RunningRead<BatchReadResult> newcomer = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(2); // the newcomer claimed A
            beforeTheWaiterRetries.resume();
            waiter.awaitWaitingForAnotherRead();
            delegate.release(2);

            assertThat(failing.failure()).isSameAs(firstFailure);
            assertThat(newcomer.failure()).isSameAs(secondFailure);
            assertThat(waiter.failure()).isSameAs(secondFailure);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
        }

        /**
         * The waiter loses its first fetch, retries, and finds the range claimed by a newcomer. It waits a second time
         * and takes the value fetched by the newcomer.
         */
        @Test
        void waiterTakesTheNewerFetch() throws Exception {
            StorageException firstFailure = new StorageException("first failure");
            List<RangeRequest> waiterRequests = requests(A);
            delegate.hold(1);
            delegate.fail(1, firstFailure);
            delegate.hold(2);

            RunningRead<BatchReadResult> failing = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> waiter = RunningRead.start(() -> reader.readRanges(waiterRequests));
            waiter.awaitWaitingForAnotherRead();
            Pause beforeTheWaiterRetries = cache.pauseNextLookupBy(waiter.thread());
            delegate.release(1);
            beforeTheWaiterRetries.awaitReached();
            RunningRead<BatchReadResult> newcomer = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(2); // the newcomer claimed A
            beforeTheWaiterRetries.resume();
            waiter.awaitWaitingForAnotherRead();
            delegate.release(2);
            BatchReadResult waiterResult = waiter.result();

            assertThat(failing.failure()).isSameAs(firstFailure);
            assertThat(counts(newcomer.result())).containsExactly(100);
            assertThat(counts(waiterResult)).containsExactly(100);
            assertHoldsTheBytesOfItsRange(waiterRequests);
            assertThat(waiterResult.fetches()).isZero();
            assertThat(waiterResult.bytesFromCache()).isEqualTo(100);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
        }

        @Test
        void errorInFetchReleasesWaiters() throws Exception {
            Error crash = new SimulatedError();
            delegate.hold(1);
            delegate.fail(1, crash);

            RunningRead<BatchReadResult> crashing = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> waiter = RunningRead.start(() -> reader.readRanges(requests(A)));
            waiter.awaitWaitingForAnotherRead();
            delegate.release(1);

            assertThat(crashing.failure()).isSameAs(crash);
            assertThat(counts(waiter.result())).containsExactly(100);
        }

        // the fetch keeps running
        @Test
        void interruptedWaiterGivesUp() throws Exception {
            delegate.hold(1);

            RunningRead<BatchReadResult> fetching = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<String> waiter = RunningRead.start(() -> outcomeOfReading(A));
            waiter.awaitWaitingForAnotherRead();
            waiter.interrupt();

            assertThat(waiter.result()).isEqualTo("gave up, interrupt flag set");
            delegate.release(1);
            assertThat(counts(fetching.result())).containsExactly(100);
        }

        @Test
        void interruptedReadDoesNotWait() throws Exception {
            delegate.hold(1);

            RunningRead<BatchReadResult> fetching = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<String> arrivingInterrupted = RunningRead.start(() -> {
                Thread.currentThread().interrupt();
                return outcomeOfReading(A);
            });

            assertThat(arrivingInterrupted.result()).isEqualTo("gave up, interrupt flag set");
            delegate.release(1);
            assertThat(counts(fetching.result())).containsExactly(100);
        }

        private String outcomeOfReading(ByteRange range) {
            try {
                reader.readRanges(requests(range));
                return "returned";
            } catch (TransientStorageException gaveUp) {
                boolean flagSet = Thread.currentThread().isInterrupted();
                return flagSet ? "gave up, interrupt flag set" : "gave up, interrupt flag lost";
            }
        }

        @Test
        void waiterFetchesPastDeadline() throws Exception {
            BatchRangeLoader impatient = new BatchRangeLoader(cache, Duration.ofMillis(50));
            CachingRangeReader impatientReader = new CachingRangeReader(delegate, cache, impatient);
            List<RangeRequest> waiterRequests = requests(A);
            delegate.hold(1);

            RunningRead<BatchReadResult> stalled = RunningRead.start(() -> impatientReader.readRanges(requests(A)));
            delegate.awaitCall(1);
            BatchReadResult waiterResult = impatientReader.readRanges(waiterRequests);

            assertThat(counts(waiterResult)).containsExactly(100);
            assertHoldsTheBytesOfItsRange(waiterRequests);
            assertThat(waiterResult.fetches()).isEqualTo(1);
            assertThat(waiterResult.bytesFromCache()).isZero();
            assertThat(impatient.pendingLoads()).isZero(); // the waiter freed the range of the stalled fetch
            delegate.release(1);
            assertThat(counts(stalled.result())).containsExactly(100);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
            assertThat(impatient.pendingLoads()).isZero();
        }

        // the reads served meanwhile keep their bytes
        @Test
        void lateFailureOfStalledFetch() throws Exception {
            BatchRangeLoader impatient = new BatchRangeLoader(cache, Duration.ofMillis(50));
            CachingRangeReader impatientReader = new CachingRangeReader(delegate, cache, impatient);
            StorageException lateFailure = new StorageException("late failure");
            List<RangeRequest> waiterRequests = requests(A);
            List<RangeRequest> laterRequests = requests(A);
            delegate.hold(1);
            delegate.fail(1, lateFailure);

            RunningRead<BatchReadResult> stalled = RunningRead.start(() -> impatientReader.readRanges(requests(A)));
            delegate.awaitCall(1);
            impatientReader.readRanges(waiterRequests);
            int loadsLeftByTheWaiter = impatient.pendingLoads();
            delegate.release(1);
            Throwable failureOfTheStalledRead = stalled.failure();
            impatientReader.readRanges(laterRequests);

            assertThat(loadsLeftByTheWaiter).isZero();
            assertThat(failureOfTheStalledRead).isSameAs(lateFailure);
            assertHoldsTheBytesOfItsRange(waiterRequests);
            assertHoldsTheBytesOfItsRange(laterRequests);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
            assertThat(impatient.pendingLoads()).isZero();
        }

        @Test
        void failedFetchPastDeadline() throws Exception {
            BatchRangeLoader impatient = new BatchRangeLoader(cache, Duration.ofMillis(50));
            CachingRangeReader impatientReader = new CachingRangeReader(delegate, cache, impatient);
            StorageException backendDown = new StorageException("backend down");
            List<RangeRequest> waiterRequests = requests(A);
            delegate.hold(1);
            delegate.fail(2, backendDown);

            RunningRead<BatchReadResult> stalled = RunningRead.start(() -> impatientReader.readRanges(requests(A)));
            delegate.awaitCall(1);

            assertThatThrownBy(() -> impatientReader.readRanges(waiterRequests)).isSameAs(backendDown);

            assertThat(impatient.pendingLoads()).isEqualTo(1); // the stalled fetch keeps its claim
            delegate.release(1);
            assertThat(counts(stalled.result())).containsExactly(100);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
            assertThat(impatient.pendingLoads()).isZero();
        }

        @Test
        void waiterGetsShortReadAtEnd() throws Exception {
            List<RangeRequest> batchRequests = requests(STRADDLING_THE_END);
            delegate.hold(1);

            RunningRead<BatchReadResult> batch = RunningRead.start(() -> reader.readRanges(batchRequests));
            delegate.awaitCall(1);
            RunningRead<ByteBuffer> single =
                    RunningRead.start(() -> reader.readRange(STRADDLING_THE_END.offset(), STRADDLING_THE_END.length()));
            single.awaitWaitingForAnotherRead();
            delegate.release(1);
            BatchReadResult batchResult = batch.result();
            ByteBuffer read = single.result().flip();

            assertThat(counts(batchResult)).containsExactly(30);
            assertHoldsTheBytesOfItsRange(batchRequests);
            assertThat(read.remaining()).isEqualTo(30);
            assertThat(read).isEqualTo(bytesOf(STRADDLING_THE_END));
            assertThat(delegate.calls()).hasSize(1);
        }

        // the reads in progress keep their bytes
        @Test
        void clearingCacheUnderReads() throws Exception {
            List<RangeRequest> fetchingRequests = requests(A);
            List<RangeRequest> waiterRequests = requests(A);
            delegate.hold(1);

            RunningRead<BatchReadResult> fetching = RunningRead.start(() -> reader.readRanges(fetchingRequests));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> waiter = RunningRead.start(() -> reader.readRanges(waiterRequests));
            waiter.awaitWaitingForAnotherRead();
            reader.clearCache();
            delegate.release(1);
            fetching.result();
            waiter.result();

            assertHoldsTheBytesOfItsRange(fetchingRequests);
            assertHoldsTheBytesOfItsRange(waiterRequests);
            assertThat(delegate.calls()).containsExactly(List.of(A));
        }

        /**
         * The entry leaves the cache between the store of the fetching read and the lookup of the waiter, as with an
         * eviction. The waiter keeps the bytes handed over by the fetching read and stores nothing. Its lookup is
         * recorded as a miss.
         */
        @Test
        void waiterKeepsBytesOfLostEntry() throws Exception {
            ByteRangeCache cacheLosingItsFirstStore =
                    new CacheLosingItsFirstStore(cacheManager, delegate.getSourceIdentifier());
            BatchRangeLoader itsLoader = new BatchRangeLoader(cacheLosingItsFirstStore);
            CachingRangeReader itsReader = new CachingRangeReader(delegate, cacheLosingItsFirstStore, itsLoader);
            List<RangeRequest> fetchingRequests = requests(A);
            List<RangeRequest> waiterRequests = requests(A);
            delegate.hold(1);

            RunningRead<BatchReadResult> fetching = RunningRead.start(() -> itsReader.readRanges(fetchingRequests));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> waiter = RunningRead.start(() -> itsReader.readRanges(waiterRequests));
            waiter.awaitWaitingForAnotherRead();
            delegate.release(1);
            fetching.result();
            waiter.result();

            assertHoldsTheBytesOfItsRange(fetchingRequests);
            assertHoldsTheBytesOfItsRange(waiterRequests);
            assertThat(delegate.calls()).containsExactly(List.of(A));
            CacheStats stats = itsReader.getCacheStats();
            assertThat(stats.missCount()).isEqualTo(2);
            assertThat(stats.loadCount()).isEqualTo(1);
            assertThat(stats.hitCount()).isZero();
            assertThat(cacheLosingItsFirstStore.getIfPresentQuietly(A)).isNull();
            assertThat(itsLoader.pendingLoads()).isZero();
        }

        /**
         * The other reader waits up to a minute for a fetch in progress, longer than {@link RunningRead#result()} waits
         * for a read. A read of the other reader waiting for the held one would fail the test.
         */
        @Test
        void separateReadersEachFetch() throws Exception {
            ByteRangeCache samePartition = new ByteRangeCache(cacheManager, delegate.getSourceIdentifier());
            BatchRangeLoader patient = new BatchRangeLoader(samePartition, Duration.ofMinutes(1));
            CachingRangeReader otherReader = new CachingRangeReader(delegate, samePartition, patient);
            List<RangeRequest> otherRequests = requests(A);
            delegate.hold(1);

            RunningRead<BatchReadResult> held = RunningRead.start(() -> reader.readRanges(requests(A)));
            delegate.awaitCall(1);
            RunningRead<BatchReadResult> other = RunningRead.start(() -> otherReader.readRanges(otherRequests));
            BatchReadResult otherResult = other.result(); // taken while the first read is held
            delegate.release(1);
            held.result();

            assertThat(counts(otherResult)).containsExactly(100);
            assertHoldsTheBytesOfItsRange(otherRequests);
            assertThat(otherResult.fetches()).isEqualTo(1);
            assertThat(delegate.calls()).containsExactly(List.of(A), List.of(A));
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);
            assertThat(patient.pendingLoads()).isZero();
        }

        @Test
        void concurrentReadsFetchOnce() throws Exception {
            List<ByteRange> pool = rangePool(64);
            int threads = 8;
            ExecutorService readers = Executors.newFixedThreadPool(threads);
            try {
                List<Future<Void>> running = new ArrayList<>();
                for (int thread = 0; thread < threads; thread++) {
                    Random random = new Random(thread);
                    running.add(readers.submit(readingAtRandom(pool, random, 300)));
                }
                for (Future<Void> read : running) {
                    read.get(60, TimeUnit.SECONDS);
                }
            } finally {
                readers.shutdownNow();
            }

            Map<ByteRange, Long> fetchesPerRange = fetchesPerRange();
            assertThat(fetchesPerRange.keySet()).containsExactlyInAnyOrderElementsOf(pool);
            assertThat(fetchesPerRange.values()).containsOnly(1L);
        }

        private Callable<Void> readingAtRandom(List<ByteRange> pool, Random random, int reads) {
            return () -> {
                for (int read = 0; read < reads; read++) {
                    if (random.nextInt(4) == 0) {
                        readSingle(pool.get(random.nextInt(pool.size())));
                    } else {
                        readBatch(randomRanges(pool, random));
                    }
                }
                return null;
            };
        }

        private void readSingle(ByteRange range) {
            ByteBuffer read = reader.readRange(range.offset(), range.length());
            assertThat(read.flip()).isEqualTo(bytesOf(range));
        }

        private void readBatch(List<ByteRange> ranges) {
            List<RangeRequest> batch = requests(ranges.toArray(ByteRange[]::new));
            reader.readRanges(batch);
            assertHoldsTheBytesOfItsRange(batch);
        }

        private static List<ByteRange> randomRanges(List<ByteRange> pool, Random random) {
            int size = 1 + random.nextInt(6);
            List<ByteRange> ranges = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                ranges.add(pool.get(random.nextInt(pool.size())));
            }
            return ranges;
        }

        private static List<ByteRange> rangePool(int size) {
            List<ByteRange> pool = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                pool.add(new ByteRange(i * 1_000L, 100 + i));
            }
            return pool;
        }

        private Map<ByteRange, Long> fetchesPerRange() {
            Map<ByteRange, Long> fetches = new HashMap<>();
            for (List<ByteRange> call : delegate.calls()) {
                for (ByteRange range : call) {
                    fetches.merge(range, 1L, Long::sum);
                }
            }
            return fetches;
        }
    }

    static Stream<Arguments> failuresOfTheFirstFetch() {
        IOException connectionReset = new IOException("connection reset");
        return Stream.of(
                Arguments.argumentSet("a storage exception", new StorageException("backend down")),
                Arguments.argumentSet("a cancellation", new CancellationException("fetch cancelled")),
                Arguments.argumentSet("a completion exception with a cause", new CompletionException(connectionReset)));
    }

    static Stream<Arguments> failuresOfTheNewerFetch() {
        return Stream.of(
                Arguments.argumentSet("an exception", new StorageException("second failure")),
                Arguments.argumentSet("an error", new SimulatedError()));
    }

    private static List<RangeRequest> requests(ByteRange... ranges) {
        List<RangeRequest> requests = new ArrayList<>(ranges.length);
        for (ByteRange range : ranges) {
            requests.add(new RangeRequest(range, ByteBuffer.allocate(range.length())));
        }
        return requests;
    }

    private static void assertHoldsTheBytesOfItsRange(List<RangeRequest> filled) {
        for (RangeRequest request : filled) {
            ByteBuffer read = request.target().duplicate().flip();
            assertThat(read).isEqualTo(bytesOf(request.range()));
        }
    }

    /** The bytes of {@code range} in the source, cut at its end. */
    private static ByteBuffer bytesOf(ByteRange range) {
        int offset = Math.toIntExact(range.offset());
        int available = Math.min(range.length(), TEST_DATA.length - offset);
        return ByteBuffer.wrap(TEST_DATA, offset, available).slice();
    }

    private static final class SimulatedError extends Error {
        private static final long serialVersionUID = 1L;
    }

    /** A cache losing the first value stored in it right after the store, as with an eviction. */
    private static final class CacheLosingItsFirstStore extends ByteRangeCache {
        private final AtomicBoolean lost = new AtomicBoolean();

        CacheLosingItsFirstStore(CacheManager cacheManager, String sourceIdentifier) {
            super(cacheManager, sourceIdentifier);
        }

        @Override
        public ByteBuffer getOrStore(ByteRange key, ByteBuffer value) {
            ByteBuffer stored = super.getOrStore(key, value);
            boolean firstStore = lost.compareAndSet(false, true);
            if (firstStore) {
                invalidateAll();
            }
            return stored;
        }
    }

    /** Helper method to convert ByteBuffer to byte array. */
    private byte[] bufferToArray(ByteBuffer buffer) {
        buffer.rewind();
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }

    /** Mock RangeReader that counts read operations for concurrency testing. */
    private static class CountingRangeReader extends AbstractRangeReader {
        private final RangeReader delegate;
        private final AtomicInteger readCount = new AtomicInteger(0);

        public CountingRangeReader(RangeReader delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
            readCount.incrementAndGet();
            int bytesRead = delegate.readRange(offset, actualLength, target);
            // readRange returns a flipped buffer, but readRangeNoFlip should not flip
            target.position(target.limit());
            return bytesRead;
        }

        @Override
        public OptionalLong size() {
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

    /** Serves one constant byte value under a caller-chosen source identifier. */
    private static final class ConstantBytesRangeReader extends AbstractRangeReader {
        private final String identifier;
        private final byte value;

        ConstantBytesRangeReader(String identifier, byte value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
            for (int i = 0; i < actualLength; i++) {
                target.put(value);
            }
            return actualLength;
        }

        @Override
        public OptionalLong size() {
            return OptionalLong.of(1000);
        }

        @Override
        public String getSourceIdentifier() {
            return identifier;
        }

        @Override
        public void close() {
            // nothing to release
        }
    }

    /** Mock closable RangeReader for testing close behavior. */
    private static class MockClosableRangeReader extends AbstractRangeReader {
        private boolean closed = false;

        @Override
        protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
            if (closed) {
                throw new IllegalStateException("Reader is closed");
            }
            // Simulate reading by filling buffer with test data
            int bytesToWrite = Math.min(actualLength, target.remaining());
            for (int i = 0; i < bytesToWrite; i++) {
                target.put((byte) (offset + i));
            }
            return bytesToWrite;
        }

        @Override
        public OptionalLong size() {
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
