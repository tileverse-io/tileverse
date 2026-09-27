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

/**
 * The batch tuning of one {@code Storage}, resolved once when the Storage is created and handed to every reader it
 * opens: how far apart two ranges may be and still share a fetch, how large a merged fetch may grow, and how many
 * fetches of one batch may be in flight at once.
 *
 * <p>Peak scratch memory of one batched read is the in-flight bound times the fetch cap, since each merged fetch
 * borrows heap scratch for its whole extent.
 *
 * @param maxGapBytes largest gap between two ranges bridged by one fetch, in bytes; negative disables merging
 * @param maxFetchBytes upper bound on a single merged fetch, in bytes; positive
 * @param maxInFlightFetches how many fetches of one batch may run at once; zero removes the bound
 */
public record BatchSettings(int maxGapBytes, int maxFetchBytes, int maxInFlightFetches) {

    /** Default in-flight bound, the parallelism the backends used before it became configurable. */
    public static final int DEFAULT_MAX_IN_FLIGHT_FETCHES = 8;

    /**
     * Validates the fetch cap and the in-flight bound.
     *
     * @throws IllegalArgumentException if {@code maxFetchBytes} is not positive or {@code maxInFlightFetches} is
     *     negative
     */
    public BatchSettings {
        if (maxFetchBytes <= 0) {
            throw new IllegalArgumentException("maxFetchBytes must be positive: " + maxFetchBytes);
        }
        if (maxInFlightFetches < 0) {
            throw new IllegalArgumentException("maxInFlightFetches cannot be negative: " + maxInFlightFetches);
        }
    }

    /**
     * Combines a merge policy with an in-flight bound.
     *
     * @param policy the merge policy
     * @param maxInFlightFetches how many fetches of one batch may run at once; zero removes the bound
     * @return the settings
     */
    public static BatchSettings of(CoalescingPolicy policy, int maxInFlightFetches) {
        requireNonNull(policy, "policy cannot be null");
        return new BatchSettings(policy.maxGapBytes(), policy.maxFetchBytes(), maxInFlightFetches);
    }

    /**
     * The default settings for object stores: {@link CoalescingPolicy#objectStoreDefaults()} and
     * {@value #DEFAULT_MAX_IN_FLIGHT_FETCHES} fetches in flight.
     *
     * @return the object-store defaults
     */
    public static BatchSettings objectStoreDefaults() {
        return of(CoalescingPolicy.objectStoreDefaults(), DEFAULT_MAX_IN_FLIGHT_FETCHES);
    }

    /**
     * The default settings for plain HTTP servers: {@link CoalescingPolicy#httpDefaults()} and
     * {@value #DEFAULT_MAX_IN_FLIGHT_FETCHES} fetches in flight.
     *
     * @return the HTTP defaults
     */
    public static BatchSettings httpDefaults() {
        return of(CoalescingPolicy.httpDefaults(), DEFAULT_MAX_IN_FLIGHT_FETCHES);
    }

    /**
     * The merge policy these settings describe.
     *
     * @return the policy for {@link BatchPlanner}
     */
    public CoalescingPolicy coalescingPolicy() {
        return new CoalescingPolicy(maxGapBytes, maxFetchBytes);
    }

    /**
     * The in-flight bound as a concurrency cap for {@link BatchRunner}: zero, meaning no bound, becomes
     * {@link Integer#MAX_VALUE}.
     *
     * @return the cap, at least 1
     */
    public int concurrencyCap() {
        return maxInFlightFetches == 0 ? Integer.MAX_VALUE : maxInFlightFetches;
    }
}
