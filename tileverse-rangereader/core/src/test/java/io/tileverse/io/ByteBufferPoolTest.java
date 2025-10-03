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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ByteBufferPoolTest {

    private ByteBufferPool pool;

    @BeforeEach
    void setUp() {
        pool = new ByteBufferPool(4, 8, 1024); // Small limits for testing
    }

    @Test
    void constructor_withValidParameters_createsPool() {
        ByteBufferPool customPool = new ByteBufferPool(10, 20, 512);
        assertThat(customPool).isNotNull();
        assertThat(customPool.toString()).contains("direct=0/10", "heap=0/20");
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
                .hasMessageContaining("minBufferSize must be positive");
    }

    @Test
    void getDefault_returnsSameInstance() {
        ByteBufferPool default1 = ByteBufferPool.getDefault();
        ByteBufferPool default2 = ByteBufferPool.getDefault();
        assertThat(default1).isSameAs(default2);
    }

    @Test
    void borrowDirect_withValidCapacity_returnsDirectBuffer() {
        ByteBuffer buffer = pool.borrowDirect(2048);

        assertThat(buffer).isNotNull();
        assertThat(buffer.isDirect()).isTrue();
        assertThat(buffer.capacity()).isGreaterThanOrEqualTo(2048);
        assertThat(buffer.position()).isZero();
        assertThat(buffer.limit()).isEqualTo(2048);
    }

    @Test
    void borrowHeap_withValidCapacity_returnsHeapBuffer() {
        ByteBuffer buffer = pool.borrowHeap(2048);

        assertThat(buffer).isNotNull();
        assertThat(buffer.isDirect()).isFalse();
        assertThat(buffer.capacity()).isGreaterThanOrEqualTo(2048);
        assertThat(buffer.position()).isZero();
        assertThat(buffer.limit()).isEqualTo(2048);
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
        assertThat(stats.buffersReturned()).isZero();
    }

    @Test
    void returnBuffer_withValidBuffer_addsToPool() {
        ByteBuffer buffer = pool.borrowDirect(2048);

        // Modify buffer to ensure it gets cleared
        buffer.putInt(12345);
        buffer.flip();

        pool.returnBuffer(buffer);

        // Borrow again and verify it's reused and cleared
        ByteBuffer reused = pool.borrowDirect(2048);
        assertThat(reused).isSameAs(buffer);
        assertThat(reused.position()).isZero();
        assertThat(reused.limit()).isEqualTo(2048);
    }

    @Test
    void returnBuffer_withSmallBuffer_discardsBuffer() {
        ByteBuffer smallBuffer = ByteBuffer.allocateDirect(512); // Below minBufferSize

        pool.returnBuffer(smallBuffer);

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.buffersDiscarded()).isEqualTo(1);
        assertThat(stats.currentDirectBuffers()).isZero();
    }

    @Test
    void bufferReuse_withSufficientCapacity_reusesBuffer() {
        // Borrow and return a large buffer (will be rounded up to 8KB)
        ByteBuffer buffer = pool.borrowDirect(4096);
        pool.returnBuffer(buffer);

        // Borrow smaller buffer - should reuse the large one
        ByteBuffer reused = pool.borrowDirect(2048);
        assertThat(reused).isSameAs(buffer);
        assertThat(reused.capacity()).isEqualTo(8192); // 4096 was rounded up to 8KB
    }

    @Test
    void bufferReuse_withInsufficientCapacity_discardsAndCreatesNew() {
        // Borrow and return a buffer (will be rounded up to 8KB)
        ByteBuffer smallBuffer = pool.borrowDirect(2048);
        pool.returnBuffer(smallBuffer);

        // Borrow much larger buffer - need more than 8KB, should create new one
        ByteBuffer largeBuffer = pool.borrowDirect(16384); // 16KB
        assertThat(largeBuffer).isNotSameAs(smallBuffer); // Should create new buffer
        assertThat(largeBuffer.capacity()).isEqualTo(16384); // Should be rounded to 16KB

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.buffersDiscarded()).isEqualTo(1); // Small buffer was discarded when looking for 16KB
    }

    @Test
    void poolLimits_exceedingMaxBuffers_discardsExcess() {
        List<ByteBuffer> buffers = new ArrayList<>();

        // Fill the direct buffer pool (max 4)
        for (int i = 0; i < 4; i++) {
            buffers.add(pool.borrowDirect(2048));
        }

        // Return all buffers
        for (ByteBuffer buffer : buffers) {
            pool.returnBuffer(buffer);
        }

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.currentDirectBuffers()).isEqualTo(4);
        assertThat(stats.buffersReturned()).isEqualTo(4);

        // Create a new buffer (not from pool) and try to return it - should be discarded
        ByteBuffer extra = ByteBuffer.allocateDirect(2048);
        pool.returnBuffer(extra);

        stats = pool.getStatistics();
        assertThat(stats.currentDirectBuffers()).isEqualTo(4); // Still at limit
        assertThat(stats.buffersDiscarded()).isEqualTo(1);
    }

    @Test
    void separatePoolsForDirectAndHeap() {
        ByteBuffer direct = pool.borrowDirect(2048);
        ByteBuffer heap = pool.borrowHeap(2048);

        assertThat(direct.isDirect()).isTrue();
        assertThat(heap.isDirect()).isFalse();

        pool.returnBuffer(direct);
        pool.returnBuffer(heap);

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.currentDirectBuffers()).isEqualTo(1);
        assertThat(stats.currentHeapBuffers()).isEqualTo(1);
    }

    @Test
    void clear_removesAllBuffers() {
        // Add some buffers to pools
        ByteBuffer direct = pool.borrowDirect(2048);
        ByteBuffer heap = pool.borrowHeap(2048);
        pool.returnBuffer(direct);
        pool.returnBuffer(heap);

        assertThat(pool.getStatistics().currentDirectBuffers()).isEqualTo(1);
        assertThat(pool.getStatistics().currentHeapBuffers()).isEqualTo(1);

        pool.clear();

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.currentDirectBuffers()).isZero();
        assertThat(stats.currentHeapBuffers()).isZero();
    }

    @Test
    void statistics_trackCorrectly() {
        // Create some activity
        ByteBuffer buffer1 = pool.borrowDirect(2048); // created
        ByteBuffer buffer2 = pool.borrowDirect(2048); // created

        pool.returnBuffer(buffer1); // returned
        pool.returnBuffer(buffer2); // returned

        ByteBuffer buffer3 = pool.borrowDirect(2048); // reused (buffer1 or buffer2)

        // Return small buffer (should be discarded)
        ByteBuffer smallBuffer = ByteBuffer.allocateDirect(512);
        pool.returnBuffer(smallBuffer); // discarded

        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        assertThat(stats.buffersCreated()).isEqualTo(2);
        assertThat(stats.buffersReused()).isEqualTo(1);
        assertThat(stats.buffersReturned()).isEqualTo(2);
        assertThat(stats.buffersDiscarded()).isEqualTo(1);

        assertThat(stats.hitRate()).isCloseTo(33.33, within(0.1)); // 1 reused out of 3 total
        assertThat(stats.returnRate()).isCloseTo(66.67, within(0.1)); // 2 returned out of 3 total
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
                        ByteBuffer buffer =
                                (op % 2 == 0) ? pool.borrowDirect(1024 + op * 10) : pool.borrowHeap(1024 + op * 10);

                        // Use the buffer briefly
                        buffer.putInt(op);

                        // Return the buffer
                        pool.returnBuffer(buffer);
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
        assertThat(stats.buffersCreated() + stats.buffersReused())
                .isLessThanOrEqualTo(threadCount * operationsPerThread);
    }

    @Test
    void toString_includesRelevantInformation() {
        ByteBuffer direct = pool.borrowDirect(2048);
        ByteBuffer heap = pool.borrowHeap(2048);
        pool.returnBuffer(direct);
        pool.returnBuffer(heap);

        String result = pool.toString();
        assertThat(result)
                .contains("ByteBufferPool")
                .contains("direct=1/4")
                .contains("heap=1/8")
                .contains("created=2")
                .contains("returned=2");
    }

    @Test
    void defaultPool_hasReasonableDefaults() {
        ByteBufferPool defaultPool = ByteBufferPool.getDefault();

        // Should be able to borrow buffers
        ByteBuffer direct = defaultPool.borrowDirect(8192);
        ByteBuffer heap = defaultPool.borrowHeap(8192);

        assertThat(direct.isDirect()).isTrue();
        assertThat(heap.isDirect()).isFalse();

        // Clean up
        defaultPool.returnBuffer(direct);
        defaultPool.returnBuffer(heap);
    }

    @Test
    void directBufferAlignment_roundsUpTo8KB() {
        // Test various sizes to ensure they're rounded up to multiples of 8KB
        ByteBuffer buffer1 = pool.borrowDirect(1);
        assertThat(buffer1.capacity()).isEqualTo(8192); // 1 byte -> 8KB

        ByteBuffer buffer2 = pool.borrowDirect(4096);
        assertThat(buffer2.capacity()).isEqualTo(8192); // 4KB -> 8KB

        ByteBuffer buffer3 = pool.borrowDirect(8192);
        assertThat(buffer3.capacity()).isEqualTo(8192); // 8KB -> 8KB (no change)

        ByteBuffer buffer4 = pool.borrowDirect(8193);
        assertThat(buffer4.capacity()).isEqualTo(16384); // 8KB+1 -> 16KB

        ByteBuffer buffer5 = pool.borrowDirect(12288);
        assertThat(buffer5.capacity()).isEqualTo(16384); // 12KB -> 16KB

        ByteBuffer buffer6 = pool.borrowDirect(16384);
        assertThat(buffer6.capacity()).isEqualTo(16384); // 16KB -> 16KB (no change)
    }

    private static org.assertj.core.data.Offset<Double> within(double offset) {
        return org.assertj.core.data.Offset.offset(offset);
    }
}
