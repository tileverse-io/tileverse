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
package io.tileverse.rangereader.spi;

import static io.tileverse.rangereader.spi.RangeReaderParameter.GROUP_CACHING;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader;
import io.tileverse.rangereader.cache.CachingRangeReader.Builder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A helper class for {@link RangeReaderProvider} implementations to manage caching-related parameters
 * and to decorate {@link RangeReader} instances with {@link CachingRangeReader}.
 */
class CachingProviderHelper {

    /**
     * A {@link RangeReaderParameter} to enable or disable memory caching for raw byte range requests.
     * When enabled, a {@link CachingRangeReader} will wrap the underlying {@link RangeReader}.
     */
    static final RangeReaderParameter<Boolean> MEMORY_CACHE = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.caching.enabled")
            .title("Enable memory cache for raw byte data")
            .description(
                    """
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

    /**
     * A {@link RangeReaderParameter} to control whether cached byte ranges should be block-aligned.
     * Block alignment can improve performance for certain storage types by optimizing read patterns.
     */
    static final RangeReaderParameter<Boolean> MEMORY_CACHE_BLOCK_ALIGNED = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.caching.blockaligned")
            .title("Apply block alignment for cached byte ranges")
            .description(
                    """
                    When enabled, all read requests are aligned to predefined block boundaries.

                    For example, a request for bytes 10-20 with a block size of 100 would \
                    result in a read of the entire block from 0-99. This can improve \
                    performance by fetching larger, contiguous chunks of data, which is often
                    more efficient for underlying storage systems (e.g., SSDs, cloud storage).

                    It also helps in reducing the number of small, fragmented reads.

                    This setting is only effective when caching is enabled.
                    """)
            .type(Boolean.class)
            .group(GROUP_CACHING)
            .defaultValue(true)
            .build();

    /**
     * A {@link RangeReaderParameter} to specify the block size in bytes for the memory cache.
     * The block size should be a power of 2 for optimal performance.
     */
    static final RangeReaderParameter<Integer> MEMORY_CACHE_BLOCK_SIZE = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.caching.blocksize")
            .title("Cache block size in bytes (power of 2)")
            .description(
                    """
                    Defines the size of the blocks used for block-aligned caching.

                    The optimal value for this parameter often depends on the characteristics of the \
                    underlying storage and the data access patterns.

                    Larger block sizes can lead to better throughput for sequential access patterns, \
                    while smaller block sizes might be more efficient for random access.

                    For best performance, it is recommended to use a power of 2. \

                    This setting is only effective when both caching and block alignment are enabled.
                    """)
            .type(Integer.class)
            .group(GROUP_CACHING)
            .defaultValue(64 * 1024)
            .options(8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024)
            .build();

    private static final List<RangeReaderParameter<?>> PARAMS =
            List.of(MEMORY_CACHE, MEMORY_CACHE_BLOCK_ALIGNED, MEMORY_CACHE_BLOCK_SIZE);

    /**
     * Returns a new list of parameters that includes the provided parameters along with
     * the caching-related parameters defined in this helper.
     *
     * @param params The initial list of parameters.
     * @return A new list containing all parameters, including caching parameters.
     */
    public static List<RangeReaderParameter<?>> withCachingParameters(List<RangeReaderParameter<?>> params) {
        List<RangeReaderParameter<?>> cachingParams = CachingProviderHelper.configParameters();
        List<RangeReaderParameter<?>> withCaching = new ArrayList<>(cachingParams);
        withCaching.addAll(params);
        return withCaching;
    }

    /**
     * Returns an unmodifiable list of {@link RangeReaderParameter}s related to caching configuration.
     *
     * @return A list of caching configuration parameters.
     */
    public static List<RangeReaderParameter<?>> configParameters() {
        return PARAMS;
    }

    /**
     * Decorates the given {@link RangeReader} with a {@link CachingRangeReader} if caching is enabled
     * in the provided {@link RangeReaderConfig}.
     * The caching behavior (block alignment, block size) is configured based on the {@code opts}.
     *
     * @param reader The {@link RangeReader} to decorate.
     * @param opts The {@link RangeReaderConfig} containing caching settings.
     * @return The decorated {@link RangeReader} (a {@link CachingRangeReader}) or the original reader if caching is disabled.
     */
    public static RangeReader decorate(RangeReader reader, RangeReaderConfig opts) {
        final boolean enableCaching = opts.getParameter(MEMORY_CACHE).orElse(false);
        if (!enableCaching) {
            return reader;
        }
        Optional<Boolean> blockAligned = opts.getParameter(MEMORY_CACHE_BLOCK_ALIGNED);
        Optional<Integer> blockSize = opts.getParameter(MEMORY_CACHE_BLOCK_SIZE);

        Builder builder = CachingRangeReader.builder(reader);
        if (blockAligned.isPresent()) {
            if (blockSize.isPresent()) {
                builder.blockSize(blockSize.get());
            } else {
                builder.withBlockAlignment();
            }
        } else {
            builder.withoutBlockAlignment();
        }
        return builder.build();
    }
}
