/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.s3;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import org.jspecify.annotations.NullMarked;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

/**
 * Connection settings of the HTTP client shared by the S3 clients of the process, built by {@link S3SharedHttpClient}.
 * The HTTP client keeps one connection pool per host, and reads at most {@link #READ_WINDOW_BYTES} of a response ahead
 * of its consumer.
 *
 * <p>The settings belong to the process, read through {@link ProcessSettings}: a pool shared by Storages has no Storage
 * to take its parameters from. Without a configured size, the pool follows the memory limit seen by the JVM, or four
 * times its maximum heap when that is smaller ({@link #defaultMaxConcurrency(OptionalLong)}).
 *
 * @param maxConcurrency connections pooled per host
 * @param connectionTimeout longest wait for a connection to open
 * @param connectionAcquisitionTimeout longest wait of a request for a pooled connection; the request fails afterwards
 */
@NullMarked
record S3HttpClientSettings(int maxConcurrency, Duration connectionTimeout, Duration connectionAcquisitionTimeout) {

    static final String MAX_CONCURRENCY = "io.tileverse.storage.s3-http-client.max-concurrency";
    static final String CONNECTION_TIMEOUT = "io.tileverse.storage.s3-http-client.connection-timeout";
    static final String CONNECTION_ACQUISITION_TIMEOUT =
            "io.tileverse.storage.s3-http-client.connection-acquisition-timeout";

    /** Body bytes read by the HTTP client ahead of a response consumer. */
    static final long READ_WINDOW_BYTES = 1024L * 1024L;

    /**
     * Cost of a {@code Storage.read} stream left unread, measured with a window of {@link #READ_WINDOW_BYTES} and
     * rounded up: the heap holds the window and the 4 MiB stored by the SDK's blocking stream, and native memory about
     * 1.5 MB.
     */
    static final long STALLED_STREAM_BYTES = 7L * 1024L * 1024L;

    static final int MIN_DEFAULT_MAX_CONCURRENCY = 50;
    static final int MAX_DEFAULT_MAX_CONCURRENCY = 500;

    static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Three times the wait of the SDK, for a burst of requests wider than the pool to get its turn on a slow link, and
     * short enough to fail a request before its own caller gives up.
     */
    static final Duration DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(30);

    S3HttpClientSettings {
        Objects.requireNonNull(connectionTimeout, "connectionTimeout");
        Objects.requireNonNull(connectionAcquisitionTimeout, "connectionAcquisitionTimeout");
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive: " + maxConcurrency);
        }
        requirePositive(connectionTimeout, "connectionTimeout");
        requirePositive(connectionAcquisitionTimeout, "connectionAcquisitionTimeout");
    }

    private static void requirePositive(Duration duration, String name) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive: " + duration);
        }
    }

    static S3HttpClientSettings ofProcess() {
        return resolve(ProcessSettings.ofProcess(), memoryBudgetOfTheJvm());
    }

    static S3HttpClientSettings resolve(ProcessSettings process, OptionalLong memoryBudget) {
        int maxConcurrency = process.positiveInt(MAX_CONCURRENCY, defaultMaxConcurrency(memoryBudget));
        Duration connectionTimeout = process.positiveDuration(CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        Duration connectionAcquisitionTimeout =
                process.positiveDuration(CONNECTION_ACQUISITION_TIMEOUT, DEFAULT_CONNECTION_ACQUISITION_TIMEOUT);
        return new S3HttpClientSettings(maxConcurrency, connectionTimeout, connectionAcquisitionTimeout);
    }

    /**
     * The pool size used when none is configured: the stalled streams fitting in a tenth of the memory budget
     * ({@link #memoryBudget}), at {@link #STALLED_STREAM_BYTES} each, between 50 and 500; 50 when the budget is
     * unknown.
     */
    static int defaultMaxConcurrency(OptionalLong memoryBudget) {
        if (memoryBudget.isEmpty()) {
            return MIN_DEFAULT_MAX_CONCURRENCY;
        }
        long streamBudget = memoryBudget.getAsLong() / 10;
        long stalledStreams = streamBudget / STALLED_STREAM_BYTES;
        long bounded = Math.max(MIN_DEFAULT_MAX_CONCURRENCY, Math.min(MAX_DEFAULT_MAX_CONCURRENCY, stalledStreams));
        return (int) bounded;
    }

    private static OptionalLong memoryBudgetOfTheJvm() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        return memoryBudget(memoryLimitOfTheJvm(), maxHeap);
    }

    /** The memory limit, or four times the maximum heap when that is smaller; empty when neither is known. */
    static OptionalLong memoryBudget(OptionalLong memoryLimit, long maxHeap) {
        if (maxHeap > Long.MAX_VALUE / 4) {
            // Runtime.maxMemory() answers Long.MAX_VALUE for a JVM without a heap limit
            return memoryLimit;
        }
        long fourHeaps = 4 * maxHeap;
        if (memoryLimit.isEmpty()) {
            return OptionalLong.of(fourHeaps);
        }
        return OptionalLong.of(Math.min(memoryLimit.getAsLong(), fourHeaps));
    }

    /**
     * The memory limit of the container running the JVM, or the memory of the machine outside a container; empty when
     * the runtime reports none.
     */
    static OptionalLong memoryLimitOfTheJvm() {
        try {
            if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean jvm) {
                long total = jvm.getTotalMemorySize();
                return total > 0 ? OptionalLong.of(total) : OptionalLong.empty();
            }
            return OptionalLong.empty();
        } catch (LinkageError missingModule) {
            // a runtime linked without jdk.management has no such class
            return OptionalLong.empty();
        }
    }

    /** The caller owns the client and closes it. */
    SdkAsyncHttpClient newAsyncHttpClient() {
        return AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(maxConcurrency)
                .connectionTimeout(connectionTimeout)
                .connectionAcquisitionTimeout(connectionAcquisitionTimeout)
                .readBufferSizeInBytes(READ_WINDOW_BYTES)
                .build();
    }

    /**
     * Parts of one multipart upload in flight at once, an eighth of the pool: the rest serves single reads, batches and
     * other uploads.
     */
    int uploadPartsInFlight() {
        return Math.max(1, maxConcurrency / 8);
    }
}
