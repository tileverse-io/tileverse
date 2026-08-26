/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    @Test
    void autoModeResolvesOnEveryRuntime() throws Exception {
        Executor executor = BatchExecutors.create("auto", 2);
        try {
            CompletableFuture<Boolean> ran = new CompletableFuture<>();
            executor.execute(() -> ran.complete(true));
            assertThat(ran.get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            ((ExecutorService) executor).shutdownNow();
        }
    }

    @Test
    void virtualModeMatchesTheRuntime() throws Exception {
        if (Runtime.version().feature() >= 21) {
            Executor executor = BatchExecutors.create("virtual", 2);
            try {
                CompletableFuture<Thread> observed = new CompletableFuture<>();
                executor.execute(() -> observed.complete(Thread.currentThread()));
                Thread worker = observed.get(5, TimeUnit.SECONDS);
                boolean virtual = (boolean) Thread.class.getMethod("isVirtual").invoke(worker);
                assertThat(virtual).isTrue();
            } finally {
                ((ExecutorService) executor).shutdownNow();
            }
        } else {
            assertThatThrownBy(() -> BatchExecutors.create("virtual", 2)).isInstanceOf(IllegalStateException.class);
        }
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
