/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import io.tileverse.io.ByteBufferPool.PoolStatistics;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Verifies idle eviction: the pool helps the hot path, but once activity stops for longer than the idle timeout it
 * releases its whole free list so it does not retain memory while quiet. The unconditional release is tested directly;
 * the background scheduler is exercised with a short timeout.
 */
class ByteBufferPoolIdleEvictionTest {

    private static ByteBufferPool poolWithIdleTimeout(Duration idleTimeout) {
        return ByteBufferPool.builder()
                .maxDirectBuffers(10)
                .maxHeapBuffers(10)
                .blockSize(1024)
                .idleTimeout(idleTimeout)
                .build();
    }

    @Test
    void evictAllRetained_freesEveryRetainedBuffer() {
        ByteBufferPool pool = poolWithIdleTimeout(Duration.ZERO); // scheduler disabled; drive eviction directly
        pool.returnBuffer(ByteBuffer.allocateDirect(2048));
        pool.returnBuffer(ByteBuffer.allocate(2048));
        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(1);
        assertThat(pool.getHeapPoolStatistics().poolSize()).isEqualTo(1);

        pool.evictAllRetained();

        assertThat(pool.getDirectPoolStatistics().poolSize()).isZero();
        assertThat(pool.getHeapPoolStatistics().poolSize()).isZero();
        assertThat(pool.getDirectPoolStatistics().bytesSize()).isZero();
        assertThat(pool.getHeapPoolStatistics().bytesSize()).isZero();
    }

    @Test
    void evictAllRetained_countsDiscardsAndPreservesCumulativeStats() {
        ByteBufferPool pool = poolWithIdleTimeout(Duration.ZERO);
        pool.returnBuffer(ByteBuffer.allocateDirect(2048));
        pool.returnBuffer(ByteBuffer.allocateDirect(2048));

        pool.evictAllRetained();

        PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.poolSize()).isZero();
        assertThat(stats.returned()).isEqualTo(2); // cumulative counters survive eviction
        assertThat(stats.discarded()).isEqualTo(2); // both evicted buffers counted as discarded
    }

    @Test
    void scheduler_drainsPoolAfterInactivity() throws InterruptedException {
        ByteBufferPool pool = poolWithIdleTimeout(Duration.ofMillis(60));
        pool.returnBuffer(ByteBuffer.allocateDirect(2048)); // returning is activity, schedules a sweep
        pool.returnBuffer(ByteBuffer.allocate(2048));

        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (retainedBuffers(pool) > 0 && System.nanoTime() < deadlineNanos) {
            Thread.sleep(10);
        }

        assertThat(pool.getDirectPoolStatistics().poolSize()).isZero();
        assertThat(pool.getHeapPoolStatistics().poolSize()).isZero();
    }

    @Test
    void disabledTimeout_retainsBuffersWhileQuiet() throws InterruptedException {
        ByteBufferPool pool = poolWithIdleTimeout(Duration.ZERO);
        pool.returnBuffer(ByteBuffer.allocateDirect(2048));

        Thread.sleep(150);

        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(1);
    }

    private static int retainedBuffers(ByteBufferPool pool) {
        return pool.getDirectPoolStatistics().poolSize()
                + pool.getHeapPoolStatistics().poolSize();
    }
}
