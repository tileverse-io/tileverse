/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
 *   <li>{@code auto} (default): virtual threads on Java 24 and later, otherwise the bounded pool
 *   <li>{@code virtual}: virtual threads, failing fast on a runtime without them
 *   <li>{@code pool}: a bounded pool of daemon platform threads; {@code io.tileverse.storage.batch.pool.size} overrides
 *       its size (default: the larger of 8 and the processor count)
 * </ul>
 *
 * <p>Before Java 24, a virtual thread waiting inside {@code synchronized} code keeps its carrier thread. A fetch waits
 * that way in JDK code, for example while reading an HTTP body through a channel. Once such waits hold all the
 * carriers, the other fetches of the batch cannot run, and the body they would let through never arrives. The
 * {@code virtual} mode exposes Java 21 to 23 to this deadlock.
 *
 * <p>The virtual-thread lookup goes through {@link MethodHandles} ({@code Executors.newVirtualThreadPerTaskExecutor});
 * the class compiles and runs on the Java 17 baseline. All threads are daemons and the executor is never shut down.
 */
public final class BatchExecutors {

    static final String EXECUTOR_PROPERTY = "io.tileverse.storage.batch.executor";
    static final String POOL_SIZE_PROPERTY = "io.tileverse.storage.batch.pool.size";

    /** The first Java release where a virtual thread waiting inside {@code synchronized} code frees its carrier. */
    private static final int FIRST_RELEASE_WITHOUT_MONITOR_PINNING = 24;

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
                return requireVirtualThreadExecutor();
            case "pool":
                return platformPool(poolSize);
            case "auto":
                return automaticExecutor(poolSize);
            default:
                throw new IllegalArgumentException(
                        "unknown " + EXECUTOR_PROPERTY + " value '" + mode + "': expected auto, virtual, or pool");
        }
    }

    private static Executor automaticExecutor(int poolSize) {
        if (Runtime.version().feature() >= FIRST_RELEASE_WITHOUT_MONITOR_PINNING) {
            return requireVirtualThreadExecutor();
        }
        return platformPool(poolSize);
    }

    private static ExecutorService requireVirtualThreadExecutor() {
        ExecutorService virtualThreads = tryVirtualThreadExecutor();
        if (virtualThreads == null) {
            throw new IllegalStateException("virtual thread executor requested via " + EXECUTOR_PROPERTY
                    + " but this runtime does not provide one");
        }
        return virtualThreads;
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
