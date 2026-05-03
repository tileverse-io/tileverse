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

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.cache.CachingRangeReader;
import java.util.List;

/**
 * Convenience base for {@link StorageProvider} implementations: maintains the canonical parameter list (optionally
 * augmented with the {@link CachingProviderHelper caching parameters}) so subclasses only have to implement
 * {@link #createStorage(StorageConfig) createStorage} and the metadata methods. Caching auto-decoration itself is
 * applied by {@link io.tileverse.storage.StorageFactory} based on the resolved {@link StorageConfig}; subclasses do not
 * need to wrap the returned {@link io.tileverse.storage.Storage} themselves.
 */
public abstract class AbstractStorageProvider implements StorageProvider {

    /** Re-export of {@link CachingProviderHelper#MEMORY_CACHE_ENABLED} for parameter registration. */
    public static final StorageParameter<Boolean> MEMORY_CACHE_ENABLED = CachingProviderHelper.MEMORY_CACHE_ENABLED;

    /**
     * Re-export of {@link CachingProviderHelper#MEMORY_CACHE_BLOCK_ALIGNED}. When enabled, all read requests are
     * aligned to the configured block boundaries before consulting the {@link CachingRangeReader} cache. Only
     * meaningful when {@link #MEMORY_CACHE_ENABLED} is true.
     */
    public static final StorageParameter<Boolean> MEMORY_CACHE_BLOCK_ALIGNED =
            CachingProviderHelper.MEMORY_CACHE_BLOCK_ALIGNED;

    /**
     * Re-export of {@link CachingProviderHelper#MEMORY_CACHE_BLOCK_SIZE}. Block size in bytes for block-aligned
     * caching; recommended power of two. Only meaningful when both {@link #MEMORY_CACHE_ENABLED} and
     * {@link #MEMORY_CACHE_BLOCK_ALIGNED} are true.
     */
    public static final StorageParameter<Integer> MEMORY_CACHE_BLOCK_SIZE =
            CachingProviderHelper.MEMORY_CACHE_BLOCK_SIZE;

    private final List<StorageParameter<?>> params;

    /** Constructs a provider whose parameter list includes the caching parameters. */
    protected AbstractStorageProvider() {
        this(true);
    }

    /**
     * @param supportsCaching whether to advertise the {@link CachingProviderHelper caching parameters} alongside the
     *     subclass-specific parameters
     */
    protected AbstractStorageProvider(boolean supportsCaching) {
        List<StorageParameter<?>> own = buildParameters();
        this.params =
                supportsCaching ? List.copyOf(CachingProviderHelper.withCachingParameters(own)) : List.copyOf(own);
    }

    @Override
    public final List<StorageParameter<?>> getParameters() {
        return params;
    }

    /** Subclass-specific parameters; the caching parameters (if enabled) are appended automatically. */
    protected List<StorageParameter<?>> buildParameters() {
        return List.of();
    }
}
