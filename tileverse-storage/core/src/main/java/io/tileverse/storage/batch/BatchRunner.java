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

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.storage.RangeRequest;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * Executes a batch plan: direct fetches land straight in their caller's target, merged fetches read into pooled heap
 * scratch and {@link PlannedFetch#scatter scatter}, and up to {@code maxConcurrentFetches} fetches run at once.
 *
 * <p>The calling thread always works; a plan needing {@code n} workers borrows {@code n - 1} executor threads. A
 * single-fetch plan, or a cap of 1, never resolves the executor supplier. The first failure wins: no new fetch starts
 * once one failed, fetches already in flight drain (blocking I/O is not cancellable), and the recorded failure is
 * rethrown to the caller unchanged. Results written by worker threads are visible to the caller when {@code run}
 * returns.
 */
public final class BatchRunner {

    private BatchRunner() {}

    /**
     * Runs every fetch of a plan and returns the per-request byte counts.
     *
     * @param requests the validated batch; targets are written at their current positions
     * @param fetches the plan from {@link BatchPlanner#plan}
     * @param reader reads one fetch, normally a {@code readRange} method reference
     * @param maxConcurrentFetches how many fetches may run at once, at least 1
     * @param executor supplies the executor for extra workers, resolved only when parallelism is used
     * @return bytes read per request, zero for entries no fetch satisfies (zero-length or past EOF)
     */
    public static int[] run(
            List<RangeRequest> requests,
            List<PlannedFetch> fetches,
            FetchReader reader,
            int maxConcurrentFetches,
            Supplier<Executor> executor) {
        int[] counts = new int[requests.size()];
        runConcurrently(
                fetches.size(),
                index -> runFetch(fetches.get(index), requests, reader, counts),
                maxConcurrentFetches,
                executor);
        return counts;
    }

    /**
     * Runs {@code taskCount} indexed tasks with at most {@code maxConcurrentTasks} running at once, the calling thread
     * included. With one worker the tasks run sequentially on the calling thread and the executor supplier is never
     * resolved. The first task failure wins: no new task starts once one failed, running tasks drain, and the recorded
     * failure is rethrown unchanged.
     *
     * <p>The supplied executor must not reject submissions (the shared batch executor never does); a rejecting executor
     * propagates its rejection exception with already-submitted workers left running.
     *
     * @param taskCount how many tasks to run, indexed 0 to taskCount - 1
     * @param task the work, invoked once per index
     * @param maxConcurrentTasks the concurrency cap, at least 1
     * @param executor supplies the executor for extra workers
     */
    public static void runConcurrently(
            int taskCount, IntConsumer task, int maxConcurrentTasks, Supplier<Executor> executor) {
        if (maxConcurrentTasks < 1) {
            throw new IllegalArgumentException("maxConcurrentTasks must be at least 1: " + maxConcurrentTasks);
        }
        int workers = Math.min(maxConcurrentTasks, taskCount);
        if (workers <= 1) {
            for (int index = 0; index < taskCount; index++) {
                task.accept(index);
            }
            return;
        }
        AtomicInteger nextTask = new AtomicInteger();
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        Runnable worker = () -> {
            int index;
            while (failure.get() == null && (index = nextTask.getAndIncrement()) < taskCount) {
                try {
                    task.accept(index);
                } catch (RuntimeException taskFailure) {
                    failure.compareAndSet(null, taskFailure);
                }
            }
        };
        CompletableFuture<?>[] helpers = new CompletableFuture<?>[workers - 1];
        Executor resolved = executor.get();
        for (int i = 0; i < helpers.length; i++) {
            helpers[i] = CompletableFuture.runAsync(worker, resolved);
        }
        worker.run();
        CompletableFuture.allOf(helpers).join();
        RuntimeException firstFailure = failure.get();
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private static void runFetch(PlannedFetch fetch, List<RangeRequest> requests, FetchReader reader, int[] counts) {
        if (fetch.isDirect()) {
            PlannedFetch.Slice only = fetch.slices().get(0);
            counts[only.requestIndex()] =
                    reader.read(fetch.range(), requests.get(only.requestIndex()).target());
            return;
        }
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(fetch.range().length())) {
            ByteBuffer scratch = pooled.buffer();
            int read = reader.read(fetch.range(), scratch);
            fetch.scatter(scratch, read, requests, counts);
        }
    }
}
