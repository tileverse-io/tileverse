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
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
 * rethrown to the caller, unchanged when it is a {@link RuntimeException} or an {@link Error}. Results written by
 * worker threads are visible to the caller when {@code run} returns.
 *
 * <p>The result counts one fetch per planned fetch and, as bytes transferred, what every fetch actually read: the
 * requested bytes plus the merged gaps, short of that only where a fetch ran into EOF.
 */
public final class BatchRunner {

    private BatchRunner() {}

    /**
     * Runs every fetch of a plan and returns the per-request byte counts with the cost of the plan.
     *
     * @param requests the validated batch; targets are written at their current positions
     * @param fetches the plan from {@link BatchPlanner#plan}
     * @param reader reads one fetch, normally a {@code readRange} method reference
     * @param maxConcurrentFetches how many fetches may run at once, at least 1
     * @param executor supplies the executor for extra workers, resolved only when parallelism is used
     * @return bytes read per request, zero for entries no fetch satisfies (zero-length or past EOF), one fetch per
     *     planned fetch and the bytes those fetches read as bytes transferred
     */
    public static BatchReadResult run(
            List<RangeRequest> requests,
            List<PlannedFetch> fetches,
            FetchReader reader,
            int maxConcurrentFetches,
            Supplier<Executor> executor) {
        int[] counts = new int[requests.size()];
        long[] transferredPerFetch = new long[fetches.size()];
        runConcurrently(
                fetches.size(),
                index -> transferredPerFetch[index] = runFetch(fetches.get(index), requests, reader, counts),
                maxConcurrentFetches,
                executor);
        long transferred = 0;
        for (long perFetch : transferredPerFetch) {
            transferred += perFetch;
        }
        return BatchReadResult.of(requests, counts, fetches.size(), transferred, 0);
    }

    /**
     * Runs {@code taskCount} indexed tasks with at most {@code maxConcurrentTasks} running at once, the calling thread
     * included. With one worker the tasks run sequentially on the calling thread and the executor supplier is never
     * resolved. The first task failure, an {@link Error} included, wins: no new task starts once one failed and running
     * tasks drain. The recorded failure is then rethrown, unchanged when it is a {@link RuntimeException} or an
     * {@link Error}; with several workers, a checked exception thrown sneakily by a task comes wrapped in a
     * {@link CompletionException}.
     *
     * <p>A failure to submit a worker, such as a rejection or an Error raised by a failed thread start, is recorded
     * like a task failure: the calling thread runs no task and waits for the workers already submitted.
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
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Runnable worker = () -> runTasks(taskCount, task, nextTask, failure);
        Executor resolved = executor.get();
        CompletableFuture<Void> helpers = submitHelpers(workers - 1, worker, resolved, failure);
        worker.run();
        helpers.join();
        rethrowIfFailed(failure.get());
    }

    /**
     * Submits up to {@code count} helper workers, records a failure to submit one like a task failure, and returns a
     * future completing when every submitted worker finished.
     */
    @SuppressWarnings("java:S1181") // an Error must not skip the join on the workers already submitted
    private static CompletableFuture<Void> submitHelpers(
            int count, Runnable worker, Executor executor, AtomicReference<Throwable> failure) {
        List<CompletableFuture<Void>> submitted = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                submitted.add(CompletableFuture.runAsync(worker, executor));
            }
        } catch (Throwable submissionFailure) {
            failure.compareAndSet(null, submissionFailure);
        }
        CompletableFuture<?>[] submittedWorkers = submitted.toArray(new CompletableFuture<?>[0]);
        return CompletableFuture.allOf(submittedWorkers);
    }

    /** Runs tasks until none is left or the batch failed, and records the first task failure instead of throwing it. */
    @SuppressWarnings("java:S1181") // an Error must not skip the join on helpers still writing caller targets
    private static void runTasks(
            int taskCount, IntConsumer task, AtomicInteger nextTask, AtomicReference<Throwable> failure) {
        int index;
        while (failure.get() == null && (index = nextTask.getAndIncrement()) < taskCount) {
            try {
                task.accept(index);
            } catch (Throwable taskFailure) {
                failure.compareAndSet(null, taskFailure);
            }
        }
    }

    private static void rethrowIfFailed(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        // a checked exception thrown sneakily by a task
        throw new CompletionException(failure);
    }

    /** Runs one fetch, records its slices' counts and returns how many bytes the fetch read. */
    private static int runFetch(PlannedFetch fetch, List<RangeRequest> requests, FetchReader reader, int[] counts) {
        if (fetch.isDirect()) {
            PlannedFetch.Slice only = fetch.slices().get(0);
            int read =
                    reader.read(fetch.range(), requests.get(only.requestIndex()).target());
            counts[only.requestIndex()] = read;
            return read;
        }
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(fetch.range().length())) {
            ByteBuffer scratch = pooled.buffer();
            int read = reader.read(fetch.range(), scratch);
            fetch.scatter(scratch, read, requests, counts);
            return read;
        }
    }
}
