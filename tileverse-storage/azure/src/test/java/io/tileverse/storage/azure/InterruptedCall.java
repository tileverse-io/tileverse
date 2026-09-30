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
package io.tileverse.storage.azure;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * How a call interrupted on a thread of its own ended: its failure, if any, and the interrupt status of that thread
 * right after the call returned or threw.
 */
record InterruptedCall(Exception failure, boolean interruptStatus) {

    /**
     * Runs {@code call} on a new thread, interrupts that thread once {@code inProgress} holds, and waits for the end.
     */
    static InterruptedCall interruptWhen(Callable<Boolean> inProgress, Call call) throws Exception {
        FutureTask<InterruptedCall> task = new FutureTask<>(() -> run(call));
        Thread callingThread = new Thread(task, "interrupted-call");

        callingThread.start();
        try {
            await().atMost(Duration.ofSeconds(10)).until(inProgress);
            callingThread.interrupt();
            return task.get(10, TimeUnit.SECONDS);
        } finally {
            callingThread.interrupt();
            callingThread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    private static InterruptedCall run(Call call) {
        try {
            call.run();
            return new InterruptedCall(null, Thread.currentThread().isInterrupted());
        } catch (Exception failure) {
            return new InterruptedCall(failure, Thread.currentThread().isInterrupted());
        }
    }

    /** A call under test; it may throw a checked exception, as a read of an InputStream does. */
    @FunctionalInterface
    interface Call {
        void run() throws Exception;
    }
}
