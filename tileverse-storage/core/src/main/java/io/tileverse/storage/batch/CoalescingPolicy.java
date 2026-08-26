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

import static java.util.Objects.requireNonNull;

import java.time.Duration;

/**
 * Controls how {@link BatchPlanner} merges nearby byte ranges into shared fetches.
 *
 * <p>Two nearby ranges are cheaper to read as one fetch when the wasted gap bytes transfer faster than a second round
 * trip costs. {@link #fromNetworkMetrics} computes the largest gap worth bridging from the connection's time to first
 * byte and bandwidth (the model behind Apache Arrow's {@code CacheOptions} and GDAL's multi-range merging):
 * {@code maxGap = ttfb * bandwidth * (1/utilization - 1)}, the bytes transferable in the fraction of a round trip one
 * is willing to waste on unrequested data.
 *
 * <p>{@link #NONE} disables merging: every request becomes its own fetch and nothing is deduplicated. It is the right
 * policy when a round trip costs nothing (local files) and the default of the {@code AbstractRangeReader} template.
 *
 * @param maxGapBytes largest gap between two ranges, in bytes, bridged by fetching the bytes in between; a negative
 *     value disables merging entirely
 * @param maxFetchBytes upper bound on a single merged fetch, in bytes; always positive, capping scratch buffers and
 *     keeping every fetch within {@link io.tileverse.io.ByteRange}'s int length
 */
public record CoalescingPolicy(int maxGapBytes, int maxFetchBytes) {

    /** One fetch per request, nothing merged, nothing deduplicated. */
    public static final CoalescingPolicy NONE = new CoalescingPolicy(-1, Integer.MAX_VALUE);

    /** Default upper bound on a single merged fetch (32 MiB). */
    public static final int DEFAULT_MAX_FETCH_BYTES = 32 * 1024 * 1024;

    static final String OBJECT_STORE_MAX_GAP_PROPERTY = "io.tileverse.storage.batch.objectstore.maxgap";
    static final String HTTP_MAX_GAP_PROPERTY = "io.tileverse.storage.batch.http.maxgap";
    static final String MAX_FETCH_PROPERTY = "io.tileverse.storage.batch.maxfetch";

    private static final long HUNDRED_MIB_PER_SECOND = 100L * 1024 * 1024;
    private static final double DEFAULT_UTILIZATION = 0.9;

    /**
     * Validates the fetch cap.
     *
     * @throws IllegalArgumentException if {@code maxFetchBytes} is not positive
     */
    public CoalescingPolicy {
        if (maxFetchBytes <= 0) {
            throw new IllegalArgumentException("maxFetchBytes must be positive: " + maxFetchBytes);
        }
    }

    /**
     * Computes the policy for a connection profile: the gap budget grows with latency and bandwidth and shrinks as the
     * required utilization approaches 1.
     *
     * @param timeToFirstByte typical delay before the first byte of a response arrives
     * @param bytesPerSecond sustained transfer rate once bytes flow
     * @param utilization required fraction of useful bytes per transfer, exclusive between 0 and 1; the remainder is
     *     the budget for gap bytes
     * @return a policy with the computed gap and the {@value #DEFAULT_MAX_FETCH_BYTES}-byte fetch cap
     * @throws IllegalArgumentException on a negative duration, non-positive rate, or utilization outside (0, 1)
     */
    public static CoalescingPolicy fromNetworkMetrics(
            Duration timeToFirstByte, long bytesPerSecond, double utilization) {
        requireNonNull(timeToFirstByte, "timeToFirstByte cannot be null");
        if (timeToFirstByte.isNegative()) {
            throw new IllegalArgumentException("timeToFirstByte cannot be negative: " + timeToFirstByte);
        }
        if (bytesPerSecond <= 0) {
            throw new IllegalArgumentException("bytesPerSecond must be positive: " + bytesPerSecond);
        }
        if (utilization <= 0.0 || utilization >= 1.0) {
            throw new IllegalArgumentException("utilization must be exclusively between 0 and 1: " + utilization);
        }
        double ttfbSeconds = timeToFirstByte.toNanos() / 1_000_000_000.0;
        double gapBytes = ttfbSeconds * bytesPerSecond * (1.0 / utilization - 1.0);
        int maxGap = (int) Math.min(Integer.MAX_VALUE, Math.round(gapBytes));
        return new CoalescingPolicy(maxGap, DEFAULT_MAX_FETCH_BYTES);
    }

    /**
     * The policy for object stores (S3, GCS, Azure): 30 ms to first byte at 100 MiB/s and 90% utilization, about a 350
     * KB gap budget. The {@code io.tileverse.storage.batch.objectstore.maxgap} and
     * {@code io.tileverse.storage.batch.maxfetch} system properties override the computed values.
     *
     * @return the object-store policy honoring the system-property overrides
     */
    public static CoalescingPolicy objectStoreDefaults() {
        CoalescingPolicy computed =
                fromNetworkMetrics(Duration.ofMillis(30), HUNDRED_MIB_PER_SECOND, DEFAULT_UTILIZATION);
        return withOverrides(OBJECT_STORE_MAX_GAP_PROPERTY, computed);
    }

    /**
     * The policy for plain HTTP servers: 20 ms to first byte at 100 MiB/s and 90% utilization, about a 230 KB gap
     * budget. The {@code io.tileverse.storage.batch.http.maxgap} and {@code io.tileverse.storage.batch.maxfetch} system
     * properties override the computed values.
     *
     * @return the HTTP policy honoring the system-property overrides
     */
    public static CoalescingPolicy httpDefaults() {
        CoalescingPolicy computed =
                fromNetworkMetrics(Duration.ofMillis(20), HUNDRED_MIB_PER_SECOND, DEFAULT_UTILIZATION);
        return withOverrides(HTTP_MAX_GAP_PROPERTY, computed);
    }

    private static CoalescingPolicy withOverrides(String gapProperty, CoalescingPolicy computed) {
        int maxGap = Integer.getInteger(gapProperty, computed.maxGapBytes());
        int maxFetch = Integer.getInteger(MAX_FETCH_PROPERTY, computed.maxFetchBytes());
        return new CoalescingPolicy(maxGap, maxFetch);
    }
}
