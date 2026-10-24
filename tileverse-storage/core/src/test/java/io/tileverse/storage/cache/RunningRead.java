/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

/** A read running on a thread of its own, for tests driving two reads against each other. */
final class RunningRead<T> {

    private final Thread thread;
    private final FutureTask<T> task;

    private RunningRead(Thread thread, FutureTask<T> task) {
        this.thread = thread;
        this.task = task;
    }

    static <T> RunningRead<T> start(Callable<T> read) {
        FutureTask<T> task = new FutureTask<>(read);
        Thread thread = new Thread(task, "running-read");
        thread.setDaemon(true);
        thread.start();
        return new RunningRead<>(thread, task);
    }

    Thread thread() {
        return thread;
    }

    T result() throws Exception {
        return task.get(10, TimeUnit.SECONDS);
    }

    /** Waits for the read to fail and returns its failure. */
    Throwable failure() throws Exception {
        try {
            T value = result();
            throw new AssertionError("the read returned " + value + " instead of failing");
        } catch (ExecutionException failed) {
            return failed.getCause();
        }
    }

    /**
     * Returns once the read waits for the fetch of another read. That wait is the one timed wait of a read: the tests
     * hold delegate calls and cache lookups on untimed latches.
     */
    void awaitWaitingForAnotherRead() {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> thread.getState() == Thread.State.TIMED_WAITING);
    }

    void interrupt() {
        thread.interrupt();
    }
}
