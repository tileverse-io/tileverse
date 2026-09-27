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

import static io.tileverse.storage.StorageParameter.GROUP_BATCH;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@code storage.batch.*} parameters shared by the object-store and HTTP providers, and their resolution into a
 * {@link BatchSettings}: the parameter on the {@link StorageConfig} wins, else the value derived from the backend's
 * connection profile. The local file provider declares none of them: its reads merge nothing.
 */
public final class BatchProviderHelper {

    private BatchProviderHelper() {}

    /**
     * Largest gap between two ranges of one batch that is fetched along with them. Key {@code storage.batch.max-gap}.
     */
    public static final StorageParameter<Integer> BATCH_MAX_GAP = StorageParameter.builder()
            .key("storage.batch.max-gap")
            .title("Largest gap merged between ranges")
            .description("""
                    Largest gap, in bytes, between two requested ranges of one batch that is fetched along with them \
                    as a single request instead of two. A larger gap trades unrequested bytes for fewer round trips; \
                    measure it per deployment. A negative value disables merging.

                    Defaults to the value derived from the backend's typical latency and bandwidth, about 350 KB for \
                    object stores and 230 KB for HTTP servers.
                    """)
            .type(Integer.class)
            .group(GROUP_BATCH)
            .build();

    /** Upper bound on one merged fetch. Key {@code storage.batch.max-fetch}. */
    public static final StorageParameter<Integer> BATCH_MAX_FETCH = StorageParameter.builder()
            .key("storage.batch.max-fetch")
            .title("Largest merged fetch")
            .description("""
                    Upper bound, in bytes, on a single merged fetch. Ranges whose merged extent would exceed it are \
                    fetched separately. Bounds the scratch buffer borrowed by a merged fetch. Default 32 MiB.
                    """)
            .type(Integer.class)
            .group(GROUP_BATCH)
            .defaultValue(CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES)
            .build();

    /** How many fetches of one batch may run at once. Key {@code storage.batch.max-in-flight-fetches}. */
    public static final StorageParameter<Integer> BATCH_MAX_IN_FLIGHT_FETCHES = StorageParameter.builder()
            .key("storage.batch.max-in-flight-fetches")
            .title("Fetches in flight per batch")
            .description("""
                    How many fetches of one batched read may be in flight at once; each completion admits the next. \
                    Peak scratch memory of a batch is this bound times the largest merged fetch. Zero removes the \
                    bound. Default 8.
                    """)
            .type(Integer.class)
            .group(GROUP_BATCH)
            .defaultValue(BatchSettings.DEFAULT_MAX_IN_FLIGHT_FETCHES)
            .build();

    private static final List<StorageParameter<?>> PARAMS =
            List.of(BATCH_MAX_GAP, BATCH_MAX_FETCH, BATCH_MAX_IN_FLIGHT_FETCHES);

    /**
     * Returns a new list with the given parameters followed by the batch parameters.
     *
     * @param params a provider's own parameters
     * @return the provider's parameters plus the batch parameters
     */
    public static List<StorageParameter<?>> withBatchParameters(List<StorageParameter<?>> params) {
        List<StorageParameter<?>> withBatch = new ArrayList<>(params);
        withBatch.addAll(PARAMS);
        return withBatch;
    }

    /**
     * Returns the batch parameters.
     *
     * @return an unmodifiable list of the three {@code storage.batch.*} parameters
     */
    public static List<StorageParameter<?>> configParameters() {
        return PARAMS;
    }

    /**
     * Resolves an object store's settings: each parameter on the config, else the
     * {@link BatchSettings#objectStoreDefaults() object-store default}.
     *
     * @param config the storage configuration
     * @return the settings for every reader of the Storage
     */
    public static BatchSettings objectStoreSettings(StorageConfig config) {
        return resolve(config, BatchSettings.objectStoreDefaults());
    }

    /**
     * Resolves an HTTP origin's settings: each parameter on the config, else the {@link BatchSettings#httpDefaults()
     * HTTP default}.
     *
     * @param config the storage configuration
     * @return the settings for every reader of the Storage
     */
    public static BatchSettings httpSettings(StorageConfig config) {
        return resolve(config, BatchSettings.httpDefaults());
    }

    private static BatchSettings resolve(StorageConfig config, BatchSettings defaults) {
        int maxGap = config.getParameter(BATCH_MAX_GAP).orElse(defaults.maxGapBytes());
        int maxFetch = config.getParameter(BATCH_MAX_FETCH).orElse(defaults.maxFetchBytes());
        int maxInFlight = config.getParameter(BATCH_MAX_IN_FLIGHT_FETCHES).orElse(defaults.maxInFlightFetches());
        return new BatchSettings(maxGap, maxFetch, maxInFlight);
    }
}
