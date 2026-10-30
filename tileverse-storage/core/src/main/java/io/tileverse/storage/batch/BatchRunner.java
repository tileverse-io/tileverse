/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
import java.util.concurrent.atomic.AtomicBoolean;
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
     * like a task failure: the calling thread runs no task and waits for the workers already started.
     *
     * <p>A worker still queued in the executor when the calling thread runs out of tasks is retired: it runs no task
     * when the executor gets to it, and the call does not wait for it. A busy shared pool then delays no batch beyond
     * its own fetches.
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
        List<Helper> helpers = submitHelpers(workers - 1, worker, resolved, failure);
        worker.run();
        retireOrAwait(helpers);
        rethrowIfFailed(failure.get());
    }

    /** Submits up to {@code count} helper workers and records a failure to submit one like a task failure. */
    @SuppressWarnings("java:S1181") // an Error must not skip the join on the workers already submitted
    private static List<Helper> submitHelpers(
            int count, Runnable worker, Executor executor, AtomicReference<Throwable> failure) {
        List<Helper> submitted = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                Helper helper = new Helper(worker);
                executor.execute(helper);
                submitted.add(helper);
            }
        } catch (Throwable submissionFailure) {
            failure.compareAndSet(null, submissionFailure);
        }
        return submitted;
    }

    private static void retireOrAwait(List<Helper> helpers) {
        for (Helper helper : helpers) {
            helper.retireOrAwait();
        }
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

    /**
     * A worker submitted to the executor. Either the executor starts it or the calling thread retires it, whichever
     * comes first. A started helper is awaited because it may still be writing a caller target.
     */
    private static final class Helper implements Runnable {

        private final Runnable worker;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final CompletableFuture<Void> finished = new CompletableFuture<>();

        Helper(Runnable worker) {
            this.worker = worker;
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) {
                return;
            }
            try {
                worker.run();
            } finally {
                finished.complete(null);
            }
        }

        /** Retires this helper if the executor has not started it, otherwise waits until it finishes. */
        void retireOrAwait() {
            boolean retired = claimed.compareAndSet(false, true);
            if (!retired) {
                finished.join();
            }
        }
    }
}
