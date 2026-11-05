/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CaffeineCacheManager implements CacheManager {

    private ConcurrentMap<String, Cache<?, ?>> caffeineCaches;

    public CaffeineCacheManager() {
        caffeineCaches = new ConcurrentHashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V, C extends Cache<K, V>> C getCache(String cacheIdentifier, Supplier<C> builder) {
        return (C) caffeineCaches.computeIfAbsent(cacheIdentifier, n -> builder.get());
    }

    @SuppressWarnings("unchecked")
    public com.github.benmanes.caffeine.cache.Cache<Object, Object> getNativeCache(String name) {
        Cache<?, ?> cache = caffeineCaches.get(name);
        if (cache instanceof CaffeineCache<?, ?> cc) {
            return (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cc.cache;
        }
        return (com.github.benmanes.caffeine.cache.Cache<Object, Object>) cache;
    }

    @Override
    public Collection<String> getCacheNames() {
        return caffeineCaches.keySet();
    }

    @Override
    public void invalidateAll() {
        caffeineCaches.values().forEach(Cache::invalidateAll);
    }

    @Override
    public Map<String, CacheStats> stats() {
        return caffeineCaches.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().stats()));
    }
}
