/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.storage.spi;

import static io.tileverse.storage.StorageParameter.GROUP_CACHING;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.cache.CachingRangeReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Helper for caching-related {@link StorageProvider} parameters and for decorating {@link RangeReader} instances with
 * {@link CachingRangeReader} according to the {@code storage.caching.*} configuration. Caching is exact-range only;
 * block alignment is a format-reader concern composed with {@code BlockAlignedRangeReader} regions above the cache.
 * Used by both individual providers (to register the parameters) and by {@link io.tileverse.storage.StorageFactory} (to
 * apply auto-decoration at {@code Storage} open time).
 */
public final class CachingProviderHelper {

    private CachingProviderHelper() {}

    /**
     * A {@link StorageParameter} to enable or disable memory caching for raw byte range requests. When enabled, a
     * {@link CachingRangeReader} will wrap the underlying {@link RangeReader}.
     */
    public static final StorageParameter<Boolean> MEMORY_CACHE_ENABLED = StorageParameter.builder()
            .key("storage.caching.enabled")
            .title("Enable memory cache for raw byte data")
            .description("""
                    Enables in-memory caching of byte ranges. When a range of data is requested, \
                    it is fetched from the underlying storage and stored in a memory cache.
                    Subsequent requests for the same or overlapping ranges can then be served \
                    directly from the cache, significantly reducing latency and I/O operations \
                    on the source. This is particularly effective for remote sources where
                    network latency is a factor.

                    This setting has no effect for local files.
                    """)
            .type(Boolean.class)
            .group(GROUP_CACHING)
            .defaultValue(true)
            .build();

    private static final List<StorageParameter<?>> PARAMS = List.of(MEMORY_CACHE_ENABLED);

    /**
     * Returns a new list of parameters that includes the provided parameters along with the caching-related parameters
     * defined in this helper.
     *
     * @param params The initial list of parameters.
     * @return A new list containing all parameters, including caching parameters.
     */
    public static List<StorageParameter<?>> withCachingParameters(List<StorageParameter<?>> params) {
        List<StorageParameter<?>> cachingParams = CachingProviderHelper.configParameters();
        List<StorageParameter<?>> withCaching = new ArrayList<>(cachingParams);
        withCaching.addAll(params);
        return withCaching;
    }

    /**
     * Returns an unmodifiable list of {@link StorageParameter}s related to caching configuration.
     *
     * @return A list of caching configuration parameters.
     */
    public static List<StorageParameter<?>> configParameters() {
        return PARAMS;
    }

    /**
     * Decorates the given {@link RangeReader} with a {@link CachingRangeReader} if caching is enabled in the provided
     * {@link StorageConfig}.
     *
     * @param reader The {@link RangeReader} to decorate.
     * @param opts The {@link StorageConfig} containing caching settings.
     * @return The decorated {@link RangeReader} (a {@link CachingRangeReader}) or the original reader if caching is
     *     disabled.
     */
    public static RangeReader decorate(RangeReader reader, StorageConfig opts) {
        return cachingDecoratorFor(opts).map(d -> d.apply(reader)).orElse(reader);
    }

    /**
     * Build the per-reader caching transform implied by {@code opts}, or {@link Optional#empty()} if
     * {@link #MEMORY_CACHE_ENABLED} is not set. Used by {@link io.tileverse.storage.StorageFactory} to decide whether
     * to wrap the opened {@code Storage} with a caching decorator.
     */
    public static Optional<UnaryOperator<RangeReader>> cachingDecoratorFor(StorageConfig opts) {
        final boolean enableCaching = opts.getParameter(MEMORY_CACHE_ENABLED).orElse(false);
        if (!enableCaching) {
            return Optional.empty();
        }
        return Optional.of(CachingRangeReader::of);
    }
}
