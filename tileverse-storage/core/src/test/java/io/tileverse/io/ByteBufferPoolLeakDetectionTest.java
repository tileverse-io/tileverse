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

import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Verifies the leak backstop: a borrowed buffer that is garbage-collected without being closed is detected and counted,
 * a properly closed borrow is not, and detection can be turned off. The backstop only observes; it never frees or
 * re-pools the leaked buffer, because the slice handed to the caller aliases the backing memory.
 */
class ByteBufferPoolLeakDetectionTest {

    @Test
    void detectsBorrowGarbageCollectedWithoutClose() {
        ByteBufferPool pool =
                ByteBufferPool.builder().idleTimeout(Duration.ZERO).build();

        abandonBorrow(pool); // borrows and drops the handle without closing it

        assertThat(awaitLeakCount(pool, 1)).isGreaterThanOrEqualTo(1);
    }

    @Test
    void doesNotCountAProperlyClosedBorrow() {
        ByteBufferPool pool =
                ByteBufferPool.builder().idleTimeout(Duration.ZERO).build();

        try (PooledByteBuffer pooled = pool.borrowDirect(2048)) {
            pooled.buffer();
        }

        forceGarbageCollection();
        assertThat(pool.getLeakCount()).isZero();
    }

    @Test
    void countsNothingWhenLeakDetectionDisabled() {
        ByteBufferPool pool = ByteBufferPool.builder()
                .idleTimeout(Duration.ZERO)
                .leakDetection(false)
                .build();

        abandonBorrow(pool);

        forceGarbageCollection();
        assertThat(pool.getLeakCount()).isZero();
    }

    /** Borrows a buffer and lets the handle go out of scope without closing it. */
    private static void abandonBorrow(ByteBufferPool pool) {
        PooledByteBuffer pooled = pool.borrowDirect(2048);
        pooled.buffer();
    }

    private static long awaitLeakCount(ByteBufferPool pool, long target) {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (pool.getLeakCount() < target && System.nanoTime() < deadlineNanos) {
            forceGarbageCollection();
        }
        return pool.getLeakCount();
    }

    private static void forceGarbageCollection() {
        System.gc();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
