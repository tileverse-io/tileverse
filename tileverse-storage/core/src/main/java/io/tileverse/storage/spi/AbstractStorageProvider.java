/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.spi;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
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
