/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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

        // Each poll nudges the collector; the Cleaner records the leak once the handle is reclaimed.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            System.gc();
            assertThat(pool.getLeakCount()).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void doesNotCountAProperlyClosedBorrow() {
        ByteBufferPool pool =
                ByteBufferPool.builder().idleTimeout(Duration.ZERO).build();

        try (PooledByteBuffer pooled = pool.borrowDirect(2048)) {
            pooled.buffer();
        }

        System.gc();
        assertNoLeakDetected(pool);
    }

    @Test
    void countsNothingWhenLeakDetectionDisabled() {
        ByteBufferPool pool = ByteBufferPool.builder()
                .idleTimeout(Duration.ZERO)
                .leakDetection(false)
                .build();

        abandonBorrow(pool);

        System.gc();
        assertNoLeakDetected(pool);
    }

    /** Borrows a buffer and lets the handle go out of scope without closing it. */
    private static void abandonBorrow(ByteBufferPool pool) {
        PooledByteBuffer pooled = pool.borrowDirect(2048);
        pooled.buffer();
    }

    /** Gives the collector and the Cleaner time to run, then asserts no leak was recorded. */
    private static void assertNoLeakDetected(ByteBufferPool pool) {
        await().pollDelay(Duration.ofMillis(200))
                .atMost(Duration.ofMillis(600))
                .untilAsserted(() -> assertThat(pool.getLeakCount()).isZero());
    }
}
