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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The shared executor for batch range fetches, resolved once at first use and kept for the JVM lifetime.
 *
 * <p>The {@code io.tileverse.storage.batch.executor} system property picks the strategy:
 *
 * <ul>
 *   <li>{@code auto} (default): virtual threads when the runtime provides them (Java 21+), otherwise the bounded pool
 *   <li>{@code virtual}: virtual threads, failing fast on a runtime without them
 *   <li>{@code pool}: a bounded pool of daemon platform threads; {@code io.tileverse.storage.batch.pool.size} overrides
 *       its size (default: the larger of 8 and the processor count)
 * </ul>
 *
 * <p>The virtual-thread lookup goes through {@link MethodHandles} ({@code Executors.newVirtualThreadPerTaskExecutor});
 * the class compiles and runs on the Java 17 baseline. All threads are daemons and the executor is never shut down.
 */
public final class BatchExecutors {

    static final String EXECUTOR_PROPERTY = "io.tileverse.storage.batch.executor";
    static final String POOL_SIZE_PROPERTY = "io.tileverse.storage.batch.pool.size";

    private BatchExecutors() {}

    private static final class Holder {
        static final Executor SHARED = create(
                System.getProperty(EXECUTOR_PROPERTY, "auto"),
                Integer.getInteger(POOL_SIZE_PROPERTY, defaultPoolSize()));
    }

    /**
     * Returns the JVM-wide executor for batch fetches, creating it on first call.
     *
     * @return the shared executor
     */
    public static Executor shared() {
        return Holder.SHARED;
    }

    static int defaultPoolSize() {
        return Math.max(8, Runtime.getRuntime().availableProcessors());
    }

    static Executor create(String mode, int poolSize) {
        switch (mode) {
            case "virtual":
                ExecutorService required = tryVirtualThreadExecutor();
                if (required == null) {
                    throw new IllegalStateException("virtual thread executor requested via " + EXECUTOR_PROPERTY
                            + " but this runtime does not provide one");
                }
                return required;
            case "pool":
                return platformPool(poolSize);
            case "auto":
                ExecutorService resolved = tryVirtualThreadExecutor();
                return resolved != null ? resolved : platformPool(poolSize);
            default:
                throw new IllegalArgumentException(
                        "unknown " + EXECUTOR_PROPERTY + " value '" + mode + "': expected auto, virtual, or pool");
        }
    }

    @SuppressWarnings("java:S1181") // MethodHandle.invoke declares Throwable; a narrower catch does not compile
    private static ExecutorService tryVirtualThreadExecutor() {
        try {
            MethodHandle factory = MethodHandles.publicLookup()
                    .findStatic(
                            Executors.class,
                            "newVirtualThreadPerTaskExecutor",
                            MethodType.methodType(ExecutorService.class));
            return (ExecutorService) factory.invoke();
        } catch (NoSuchMethodException | IllegalAccessException java17Runtime) {
            return null;
        } catch (Throwable unexpected) {
            throw new IllegalStateException("virtual thread executor lookup failed", unexpected);
        }
    }

    private static ExecutorService platformPool(int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("pool size must be positive: " + poolSize);
        }
        ThreadFactory namedDaemons = new ThreadFactory() {
            private final AtomicInteger created = new AtomicInteger();

            @Override
            public Thread newThread(Runnable work) {
                Thread thread = new Thread(work, "tileverse-storage-batch-" + created.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        };
        return Executors.newFixedThreadPool(poolSize, namedDaemons);
    }
}
