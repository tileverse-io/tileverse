/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BatchExecutorsTest {

    @Test
    void poolModeCreatesNamedDaemonThreads() throws Exception {
        Executor executor = BatchExecutors.create("pool", 2);
        try {
            CompletableFuture<Thread> observed = new CompletableFuture<>();
            executor.execute(() -> observed.complete(Thread.currentThread()));
            Thread worker = observed.get(5, TimeUnit.SECONDS);
            assertThat(worker.getName()).startsWith("tileverse-storage-batch-");
            assertThat(worker.isDaemon()).isTrue();
        } finally {
            ((ExecutorService) executor).shutdownNow();
        }
    }

    // Java 21 to 23 pin the carrier of a virtual thread waiting inside synchronized code
    @Test
    void autoModeIsVirtualFromJava24() throws Exception {
        Executor executor = BatchExecutors.create("auto", 2);
        try {
            boolean expectVirtual = Runtime.version().feature() >= 24;
            assertThat(isVirtual(workerOf(executor))).isEqualTo(expectVirtual);
        } finally {
            ((ExecutorService) executor).shutdownNow();
        }
    }

    @Test
    void virtualModeMatchesTheRuntime() throws Exception {
        if (Runtime.version().feature() >= 21) {
            Executor executor = BatchExecutors.create("virtual", 2);
            try {
                assertThat(isVirtual(workerOf(executor))).isTrue();
            } finally {
                ((ExecutorService) executor).shutdownNow();
            }
        } else {
            assertThatThrownBy(() -> BatchExecutors.create("virtual", 2)).isInstanceOf(IllegalStateException.class);
        }
    }

    private static Thread workerOf(Executor executor) throws Exception {
        CompletableFuture<Thread> observed = new CompletableFuture<>();
        executor.execute(() -> observed.complete(Thread.currentThread()));
        return observed.get(5, TimeUnit.SECONDS);
    }

    private static boolean isVirtual(Thread thread) throws Exception {
        if (Runtime.version().feature() < 21) {
            return false;
        }
        return (boolean) Thread.class.getMethod("isVirtual").invoke(thread);
    }

    @Test
    void invalidConfigurationIsRejected() {
        assertThatThrownBy(() -> BatchExecutors.create("fibers", 2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchExecutors.create("pool", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sharedIsASingleton() {
        assertThat(BatchExecutors.shared()).isSameAs(BatchExecutors.shared());
    }
}
