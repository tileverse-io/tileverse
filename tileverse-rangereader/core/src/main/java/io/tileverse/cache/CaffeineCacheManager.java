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
        if (cache instanceof CaffeineCache cc) {
            return cc.cache;
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
