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
package io.tileverse.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ByteBufferPoolTest {

    private ByteBufferPool pool;

    @BeforeEach
    void setUp() {
        pool = new ByteBufferPool(4, 8, 1024); // Small limits for testing
    }

    @AfterEach
    void clear() {
        pool.clear();
    }

    @Test
    void constructor_withValidParameters_createsPool() {
        ByteBufferPool customPool = new ByteBufferPool(10, 20, 512);
        assertThat(customPool).isNotNull();
        assertThat(customPool.toString())
                .contains(
                        "[heap buffers: PoolStatistics[maxSize=20, poolSize=0, bytesSize=0, created=0, reused=0, returned=0, discarded=0], direct buffers: PoolStatistics[maxSize=10, poolSize=0, bytesSize=0, created=0, reused=0, returned=0, discarded=0]]");
    }

    @Test
    void constructor_withInvalidParameters_throwsException() {
        assertThatThrownBy(() -> new ByteBufferPool(0, 10, 1024))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxDirectBuffers must be positive");

        assertThatThrownBy(() -> new ByteBufferPool(10, 0, 1024))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxHeapBuffers must be positive");

        assertThatThrownBy(() -> new ByteBufferPool(10, 20, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blockSize must be positive");
    }

    @Test
    void getDefault_returnsSameInstance() {
        ByteBufferPool default1 = ByteBufferPool.getDefault();
        ByteBufferPool default2 = ByteBufferPool.getDefault();
        assertThat(default1).isSameAs(default2);
    }

    @Test
    void borrowDirect_withValidCapacity_returnsDirectBuffer() {
        try (var pooledBuffer = pool.borrowDirect(2048)) {
            ByteBuffer buffer = pooledBuffer.buffer();

            assertThat(buffer).isNotNull();
            assertThat(buffer.isDirect()).isTrue();
            assertThat(buffer.capacity()).isEqualTo(2048);
            assertThat(buffer.position()).isZero();
            assertThat(buffer.limit()).isEqualTo(2048);
        }
    }

    @Test
    void borrowHeap_withValidCapacity_returnsHeapBuffer() {
        try (var pooledBuffer = pool.borrowHeap(2048)) {
            ByteBuffer buffer = pooledBuffer.buffer();

            assertThat(buffer).isNotNull();
            assertThat(buffer.isDirect()).isFalse();
            assertThat(buffer.capacity()).isEqualTo(2048);
            assertThat(buffer.position()).isZero();
            assertThat(buffer.limit()).isEqualTo(2048);
        }
    }

    @Test
    void borrowDirect_withNegativeCapacity_throwsException() {
        assertThatThrownBy(() -> pool.borrowDirect(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minCapacity cannot be negative");
    }

    @Test
    void borrowHeap_withNegativeCapacity_throwsException() {
        assertThatThrownBy(() -> pool.borrowHeap(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minCapacity cannot be negative");
    }

    @Test
    void returnBuffer_withNullBuffer_doesNothing() {
        // Should not throw exception
        pool.returnBuffer(null);

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.returned()).isZero();
    }

    @Test
    void returnBuffer_withValidBuffer_addsToPool() {
        ByteBuffer pooled = null;
        try (var pooledBuffer = pool.borrowHeap(2048)) {
            ByteBuffer buffer = pooledBuffer.buffer();
            // Modify buffer to ensure it gets cleared
            buffer.putInt(12345);
            buffer.flip();
            pooled = ((ByteBufferPool.PooledByteBufferImpl) pooledBuffer).pooled();
        }

        // Borrow again and verify it's reused and cleared
        try (var pooledBuffer = pool.borrowHeap(2048)) {

            assertThat(((ByteBufferPool.PooledByteBufferImpl) pooledBuffer).pooled())
                    .isSameAs(pooled);

            ByteBuffer reused = pooledBuffer.buffer();
            assertThat(reused.position()).isZero();
            assertThat(reused.limit()).isEqualTo(2048);
            assertThat(reused.capacity()).isEqualTo(2048);
        }
    }

    @Test
    void returnBuffer_withSmallBuffer_discardsBuffer() {
        ByteBuffer smallBuffer = ByteBuffer.allocateDirect(512); // Below blockSize

        pool.returnBuffer(smallBuffer);

        ByteBufferPool.PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.discarded()).isEqualTo(1);
        assertThat(stats.poolSize()).isZero();
    }

    @Test
    void bufferReuse_withSufficientCapacity_reusesBuffer() {
        // Use the pool from setUp() which has 1KB block size
        try (var pooledBuffer = pool.borrowHeap(2048)) {
            ByteBuffer buffer = pooledBuffer.buffer();
            assertThat(buffer.capacity()).isEqualTo(2048);
        }
        // Borrow same size buffer - should reuse the pooled one (no new allocation)
        try (var pooledBuffer = pool.borrowHeap(2048)) {
            ByteBuffer reused = pooledBuffer.buffer();
            assertThat(reused.capacity()).isEqualTo(2048); // slice matches requested size
        }

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        // First borrow creates one buffer, second borrow reuses it
        assertThat(stats.created()).isEqualTo(1);
        assertThat(stats.reused()).isEqualTo(1);
    }

    @Test
    void bestFit_selectsSmallestSufficientBuffer() {
        // Pool config from setUp: maxDirect=4, blockSize=1024
        // We must use multiples of 1024 to match the pool's rounding logic
        ByteBuffer b2048 = ByteBuffer.allocateDirect(2048);
        ByteBuffer b3072 = ByteBuffer.allocateDirect(3072);
        ByteBuffer b4096 = ByteBuffer.allocateDirect(4096);

        pool.returnBuffer(b2048);
        pool.returnBuffer(b3072);
        pool.returnBuffer(b4096);

        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(3);

        // Request 2500 - rounds to 3072. Should match b3072.
        try (var pooled = pool.borrowDirect(2500)) {
            ByteBuffer buffer = ((ByteBufferPool.PooledByteBufferImpl) pooled).pooled();
            assertThat(buffer.capacity()).isEqualTo(3072);
            assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(2);
        }

        // Request 1500 - rounds to 2048. Should match b2048.
        try (var pooled = pool.borrowDirect(1500)) {
            ByteBuffer buffer = ((ByteBufferPool.PooledByteBufferImpl) pooled).pooled();
            assertThat(buffer.capacity()).isEqualTo(2048);
        }

        // Request 3500 - rounds to 4096. Should match b4096.
        try (var pooled = pool.borrowDirect(3500)) {
            ByteBuffer buffer = ((ByteBufferPool.PooledByteBufferImpl) pooled).pooled();
            assertThat(buffer.capacity()).isEqualTo(4096);
        }
    }

    @Test
    void missingBuffer_createsNewWithoutDiscardingOthers() {
        ByteBuffer b2048 = ByteBuffer.allocateDirect(2048);
        ByteBuffer b3072 = ByteBuffer.allocateDirect(3072);

        pool.returnBuffer(b2048);
        pool.returnBuffer(b3072);

        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(2);

        // Request 5000 - rounds to 5120. No match in pool (largest 3072). Should create new.
        try (var pooled = pool.borrowDirect(5000)) {
            ByteBuffer buffer = ((ByteBufferPool.PooledByteBufferImpl) pooled).pooled();
            assertThat(buffer.capacity()).isGreaterThanOrEqualTo(5120);
            // Crucial check: The 2 smaller buffers should STILL be in the pool
            assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(2);
        }
    }

    @Test
    void eviction_prefersLargerBuffers() {
        // Pool config from setUp: maxDirect=4
        ByteBuffer b2048 = ByteBuffer.allocateDirect(2048);
        ByteBuffer b3072 = ByteBuffer.allocateDirect(3072);
        ByteBuffer b4096 = ByteBuffer.allocateDirect(4096);
        ByteBuffer b5120 = ByteBuffer.allocateDirect(5120);

        pool.returnBuffer(b2048);
        pool.returnBuffer(b3072);
        pool.returnBuffer(b4096);
        pool.returnBuffer(b5120);

        // Pool is full: [2048, 3072, 4096, 5120]
        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(4);
        // Try to add 6144. Should evict 2048 (smallest) and keep [3072, 4096, 5120, 6144].
        ByteBuffer b6144 = ByteBuffer.allocateDirect(6144);
        pool.returnBuffer(b6144);
        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(4);

        // Verify we have 3072...6144 available
        try (var p = pool.borrowDirect(5500)) { // Rounds to 6144. Matches b6144
            assertThat(((ByteBufferPool.PooledByteBufferImpl) p).pooled().capacity())
                    .isEqualTo(6144);
        }

        try (var p = pool.borrowDirect(2500)) { // Rounds to 3072. Matches b3072
            assertThat(((ByteBufferPool.PooledByteBufferImpl) p).pooled().capacity())
                    .isEqualTo(3072);
        }

        // Reset pool: [3072, 4096, 5120, 6144] (borrowed buffers are returned)
        // Try to add 1500 (alloc 2048). Pool is [3072, ...]. Smallest is 3072.
        // 2048 < 3072. Should be rejected.

        ByteBuffer b2048_2 = ByteBuffer.allocateDirect(2048);
        pool.returnBuffer(b2048_2);

        // Verify 2048 was NOT added. We should still have [3072, ...].
        // Borrowing 2500 (needs 3072) should still find 3072.

        try (var p = pool.borrowDirect(2500)) {
            assertThat(((ByteBufferPool.PooledByteBufferImpl) p).pooled().capacity())
                    .isEqualTo(3072);
        }
    }

    @Test
    void poolLimits_exceedingMaxBuffers_discardsExcess() {
        // Fill the direct buffer pool (max 4) by borrowing 4 buffers simultaneously
        var pooled1 = pool.borrowDirect(1024);
        var pooled2 = pool.borrowDirect(1024);
        var pooled3 = pool.borrowDirect(1024);
        var pooled4 = pool.borrowDirect(1024);

        // Return all 4 buffers
        pooled1.close();
        pooled2.close();
        pooled3.close();
        pooled4.close();

        ByteBufferPool.PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.poolSize()).isEqualTo(4);
        assertThat(stats.returned()).isEqualTo(4);

        // Create a new buffer (not from pool) and try to return it - should be discarded
        ByteBuffer extra = ByteBuffer.allocateDirect(1024);
        pool.returnBuffer(extra);

        stats = pool.getDirectPoolStatistics();
        assertThat(stats.poolSize()).isEqualTo(4); // Still at limit
        assertThat(stats.discarded()).isEqualTo(1);
    }

    @Test
    void separatePoolsForDirectAndHeap() {
        try (var pooledDirect = pool.borrowDirect(2048);
                var pooledHeap = pool.borrowHeap(2048)) {
            ByteBuffer direct = pooledDirect.buffer();
            ByteBuffer heap = pooledHeap.buffer();

            assertThat(direct.isDirect()).isTrue();
            assertThat(heap.isDirect()).isFalse();
        }

        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(1);
        assertThat(pool.getHeapPoolStatistics().poolSize()).isEqualTo(1);
    }

    @Test
    void clear_removesAllBuffers() {
        // Add some buffers to pools
        try (var pooledDirect = pool.borrowDirect(2048);
                var pooledHeap = pool.borrowHeap(2048)) {
            // Just borrow and return via try-with-resources
        }

        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(1);
        assertThat(pool.getHeapPoolStatistics().poolSize()).isEqualTo(1);

        pool.clear();

        assertThat(pool.getDirectPoolStatistics().poolSize()).isZero();
        assertThat(pool.getHeapPoolStatistics().poolSize()).isZero();
    }

    @Test
    void statistics_trackCorrectly() {
        // Create some activity - borrow 2 buffers simultaneously to force creation
        var pooledBuffer1 = pool.borrowDirect(2048); // created
        var pooledBuffer2 = pool.borrowDirect(2048); // created (can't reuse, buffer1 is still borrowed)

        pooledBuffer1.close(); // returned
        pooledBuffer2.close(); // returned

        try (var pooledBuffer3 = pool.borrowDirect(2048)) { // reused (buffer1 or buffer2)
            // Will be returned
        }

        // Return small buffer (should be discarded)
        ByteBuffer smallBuffer = ByteBuffer.allocateDirect(512);
        pool.returnBuffer(smallBuffer); // discarded

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.created()).isEqualTo(2);
        assertThat(stats.reused()).isEqualTo(1);
        assertThat(stats.returned()).isEqualTo(3); // All three PooledByteBuffers returned
        assertThat(stats.discarded()).isEqualTo(1); // Only the small buffer discarded

        assertThat(stats.hitRate()).isCloseTo(33.33, within(0.1)); // 1 reused out of 3 total
        assertThat(stats.returnRate()).isCloseTo(75.0, within(0.1)); // 3 returned out of 4 total (3 + 1 discarded)
    }

    @Test
    void concurrentAccess_isThreadSafe() throws Exception {
        int threadCount = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();

                    for (int op = 0; op < operationsPerThread; op++) {
                        // Randomly borrow direct or heap buffers
                        try (var pooledBuffer =
                                (op % 2 == 0) ? pool.borrowDirect(1024 + op * 10) : pool.borrowHeap(1024 + op * 10)) {
                            ByteBuffer buffer = pooledBuffer.buffer();
                            // Use the buffer briefly
                            buffer.putInt(op);
                        } // Auto-returned via try-with-resources
                    }
                    endLatch.countDown();
                    return null;
                } catch (Exception e) {
                    endLatch.countDown();
                    throw new RuntimeException(e);
                }
            }));
        }

        // Start all threads
        startLatch.countDown();

        // Wait for completion
        assertThat(endLatch.await(30, TimeUnit.SECONDS)).isTrue();

        // Check for exceptions
        for (Future<Void> future : futures) {
            future.get(); // Will throw if there were exceptions
        }

        executor.shutdown();

        // Verify statistics make sense
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.created() + stats.reused()).isLessThanOrEqualTo(threadCount * operationsPerThread);
    }

    @Test
    void toString_includesRelevantInformation() {
        try (var pooledDirect = pool.borrowDirect(2048);
                var pooledHeap = pool.borrowHeap(2048)) {
            // Will be returned
        }

        String result = pool.toString();
        assertThat(result)
                .contains("ByteBufferPool")
                .contains(
                        "[heap buffers: PoolStatistics[maxSize=8, poolSize=1, bytesSize=2048, created=1, reused=0, returned=1, discarded=0], direct buffers: PoolStatistics[maxSize=4, poolSize=1, bytesSize=2048, created=1, reused=0, returned=1, discarded=0]]");
    }

    @Test
    void defaultPool_hasReasonableDefaults() {
        ByteBufferPool defaultPool = ByteBufferPool.getDefault();

        // Should be able to borrow buffers
        try (var pooledDirect = defaultPool.borrowDirect(8192);
                var pooledHeap = defaultPool.borrowHeap(8192)) {
            ByteBuffer direct = pooledDirect.buffer();
            ByteBuffer heap = pooledHeap.buffer();

            assertThat(direct.isDirect()).isTrue();
            assertThat(heap.isDirect()).isFalse();
        }
    }

    @Test
    void directBufferAlignment_roundsUpToBlockSize() {
        // Test various sizes with default 8KB block size
        try (var pooledBuffer1 = pool.borrowDirect(1)) {
            assertThat(pooledBuffer1.buffer().capacity()).isEqualTo(1); // slice capacity matches request
        }

        try (var pooledBuffer2 = pool.borrowDirect(4096)) {
            assertThat(pooledBuffer2.buffer().capacity()).isEqualTo(4096); // slice capacity matches request
        }

        try (var pooledBuffer3 = pool.borrowDirect(8192)) {
            assertThat(pooledBuffer3.buffer().capacity()).isEqualTo(8192); // exact match, no slice
        }

        try (var pooledBuffer4 = pool.borrowDirect(8193)) {
            assertThat(pooledBuffer4.buffer().capacity()).isEqualTo(8193); // slice capacity matches request
        }

        try (var pooledBuffer5 = pool.borrowDirect(12288)) {
            assertThat(pooledBuffer5.buffer().capacity()).isEqualTo(12288); // slice capacity matches request
        }

        try (var pooledBuffer6 = pool.borrowDirect(16384)) {
            assertThat(pooledBuffer6.buffer().capacity()).isEqualTo(16384); // exact match, no slice
        }
    }

    @Test
    void customBlockSize_affectsAllocationAndPooling() {
        // Create pool with 2KB block size
        ByteBufferPool customPool = new ByteBufferPool(4, 8, 2048);

        // Test allocation rounding and that buffers get pooled
        // Borrow two buffers simultaneously to force creation of 2 buffers
        var pooled1 = customPool.borrowDirect(100);
        var pooled2 = customPool.borrowDirect(2049);

        // Verify capacities
        assertThat(pooled1.buffer().capacity()).isEqualTo(100);
        assertThat(pooled2.buffer().capacity()).isEqualTo(2049);

        // Return both
        pooled1.close(); // Will be pooled (>= block size)
        pooled2.close(); // Will be pooled (>= block size)

        ByteBufferPool.PoolStatistics stats = customPool.getDirectPoolStatistics();
        assertThat(stats.created()).isEqualTo(2);
        assertThat(stats.returned()).isEqualTo(2);
        assertThat(stats.poolSize()).isEqualTo(2); // Both pooled

        // Test pooling threshold - buffers smaller than block size are discarded
        ByteBuffer tooSmall = ByteBuffer.allocateDirect(1024); // 1KB < 2KB block size
        customPool.returnBuffer(tooSmall);

        stats = customPool.getDirectPoolStatistics();
        assertThat(stats.discarded()).isEqualTo(1); // Too small buffer discarded
        assertThat(stats.poolSize()).isEqualTo(2); // Still only the 2 pooled buffers

        // Verify buffer reuse
        try (var pooled3 = customPool.borrowDirect(2048)) {
            // Should reuse one of the pooled buffers
        }

        stats = customPool.getStatistics();
        assertThat(stats.reused()).isEqualTo(1); // Reused from pool
        assertThat(stats.returned()).isEqualTo(3); // 3 total returned
    }

    private static org.assertj.core.data.Offset<Double> within(double offset) {
        return org.assertj.core.data.Offset.offset(offset);
    }
}
