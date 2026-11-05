/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import org.jspecify.annotations.NonNull;

public interface CacheManager {

    @NonNull
    static CacheManager getDefault() {
        return DefaultCacheManagerHolder.get();
    }

    static void setDefault(@NonNull CacheManager newDefault) {
        DefaultCacheManagerHolder.set(newDefault);
    }

    static CacheManager newInstance() {
        return new CaffeineCacheManager();
    }

    /**
     * Get the cache associated with the given name.
     *
     * <p>Note that the cache may be lazily created at runtime if the native provider supports it.
     *
     * @param cacheIdentifier the cache identifier (must not be {@code null})
     * @return the associated cache, or {@code null} if such a cache does not exist or could be not created
     */
    @NonNull
    <K, V, C extends Cache<K, V>> C getCache(@NonNull String cacheIdentifier, @NonNull Supplier<C> builder);

    /**
     * Get a collection of the cache names known by this manager.
     *
     * @return the names of all caches known by the cache manager
     */
    Collection<String> getCacheNames();

    Map<String, CacheStats> stats();

    void invalidateAll();
}
