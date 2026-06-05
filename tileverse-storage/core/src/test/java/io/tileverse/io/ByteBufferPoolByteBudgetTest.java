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
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the retention byte budget: it bounds the bytes the free list keeps, without ever blocking or refusing a
 * borrow. Borrow always returns a usable buffer (creating one beyond the budget if needed); only retention is capped,
 * so the pool cannot grow native memory without bound.
 */
class ByteBufferPoolByteBudgetTest {

    private static ByteBufferPool poolWithDirectByteBudget(long maxDirectBytes) {
        return ByteBufferPool.builder()
                .maxDirectBuffers(100) // high, so the byte budget is the only limit
                .maxHeapBuffers(100)
                .blockSize(1024)
                .maxDirectBytes(maxDirectBytes)
                .maxHeapBytes(maxDirectBytes)
                .build();
    }

    @Test
    void retainedDirectBytes_areCappedByBudget() {
        ByteBufferPool pool = poolWithDirectByteBudget(4096);

        List<PooledByteBuffer> borrowed = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            borrowed.add(pool.borrowDirect(2048));
        }
        borrowed.forEach(PooledByteBuffer::close);

        // A 4096-byte budget holds exactly two 2048-byte buffers; the other two are discarded and freed.
        PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.bytesSize()).isEqualTo(4096);
        assertThat(stats.poolSize()).isEqualTo(2);
        assertThat(stats.discarded()).isEqualTo(2);
    }

    @Test
    void borrowNeverBlocks_evenWhenRequestExceedsTheWholeBudget() {
        ByteBufferPool pool = poolWithDirectByteBudget(4096);

        try (PooledByteBuffer pooled = pool.borrowDirect(8192)) {
            assertThat(pooled.buffer().capacity()).isEqualTo(8192);
            assertThat(pooled.buffer().isDirect()).isTrue();
        }

        // A buffer larger than the whole budget cannot be retained, so nothing is pooled.
        assertThat(pool.getDirectPoolStatistics().poolSize()).isZero();
    }

    @Test
    void incomingBuffer_notLargerThanRetained_isDiscardedWhenBudgetFull() {
        ByteBufferPool pool = poolWithDirectByteBudget(4096);

        pool.returnBuffer(ByteBuffer.allocateDirect(2048));
        pool.returnBuffer(ByteBuffer.allocateDirect(2048));
        assertThat(pool.getDirectPoolStatistics().poolSize()).isEqualTo(2);

        // The budget is full and the incoming buffer is no larger than the smallest retained, so it is rejected.
        pool.returnBuffer(ByteBuffer.allocateDirect(2048));

        PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.poolSize()).isEqualTo(2);
        assertThat(stats.bytesSize()).isEqualTo(4096);
    }

    @Test
    void largerIncomingBuffer_evictsSmallerOnesToFitBudget() {
        ByteBufferPool pool = poolWithDirectByteBudget(4096);

        pool.returnBuffer(ByteBuffer.allocateDirect(2048));
        pool.returnBuffer(ByteBuffer.allocateDirect(2048)); // free list holds [2048, 2048], budget full

        // Returning a 4096 buffer evicts both 2048s to stay within the 4096 budget.
        pool.returnBuffer(ByteBuffer.allocateDirect(4096));

        PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.bytesSize()).isEqualTo(4096);
        assertThat(stats.poolSize()).isEqualTo(1);
    }
}
