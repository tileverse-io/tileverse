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

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

class SubCache<K, V> implements Cache<K, V> {

    protected final ConcurrentMap<String, ? extends Cache<K, V>> subCaches = new ConcurrentHashMap<>();

    protected final long cacheId;
    protected final Cache<K, V> cache;

    public static <K, V> Cache<K, V> of(@NonNull String resourceId, @NonNull Cache<K, V> sharedCache) {
        return new SubCache<>(resourceId, sharedCache);
    }

    SubCache(@NonNull String resourceId, Cache<K, V> cache) {
        this(computeCacheId(resourceId), cache);
    }

    SubCache(long cacheId, Cache<K, V> cache) {
        this.cacheId = cacheId;
        this.cache = cache;
    }

    @SuppressWarnings("unchecked")
    protected Cache<SubCacheKey, V> asSubKey() {
        return (Cache<SubCacheKey, V>) cache;
    }

    public Cache<K, V> subCache(String subCacheId) {
        throw new UnsupportedOperationException();
    }

    static long computeCacheId(String resourceId) {
        // Generates a Type 3 UUID (Name-based using MD5)
        // We take the most significant 64 bits.
        // Collision risk: Effectively zero for the number of cache instances you will have.
        return UUID.nameUUIDFromBytes(resourceId.getBytes(StandardCharsets.UTF_8))
                .getMostSignificantBits();
    }

    public long cacheId() {
        return this.cacheId;
    }

    @Override
    public @Nullable V getIfPresent(K key) {
        SubCacheKey innerKey = innerKey(key);
        return asSubKey().getIfPresent(innerKey);
    }

    @Override
    public V get(final K key, Function<? super K, ? extends V> mappingFunction) {
        SubCacheKey innerKey = innerKey(key);
        return asSubKey().get(innerKey, k -> mapValue(k, mappingFunction));
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    public Collection<K> keys() {
        return cacheKeys().map(this::outerKey).toList();
    }

    @Override
    public void forEach(BiConsumer<K, V> consumer) {
        asSubKey().forEach((subkey, v) -> {
            if (subCacheFilter(subkey)) {
                consumer.accept(subkey.outerKey(), v);
            }
        });
    }

    @Override
    public Cache<K, V> invalidateAll() {
        asSubKey().invalidateAll(cacheKeys().toList());
        return this;
    }

    @Override
    public void invalidate(K key) {
        asSubKey().invalidate(innerKey(key));
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        List<SubCacheKey> sharedKeys = StreamSupport.stream(keys.spliterator(), false)
                .map(this::innerKey)
                .toList();
        asSubKey().invalidateAll(sharedKeys);
    }

    protected SubCacheKey innerKey(K key) {
        return new SubCacheKey(cacheId, key);
    }

    protected K outerKey(SubCacheKey sk) {
        return sk.outerKey();
    }

    private V mapValue(SubCacheKey sk, Function<? super K, ? extends V> mappingFunction) {
        K outerKey = outerKey(sk);
        return mappingFunction.apply(outerKey);
    }

    /**
     * @return keys filtered by this {@link #cacheId}
     */
    private Stream<SubCacheKey> cacheKeys() {
        return cache.keys().stream().filter(this::subCacheFilter).map(SubCacheKey.class::cast);
    }

    private boolean subCacheFilter(Object key) {
        return key instanceof SubCacheKey subkey && subkey.cacheId() == this.cacheId;
    }

    public static <K, V> Weigher<K, V> weigher(final Weigher<K, V> weigher) {
        return (k, v) -> {
            k = SubCacheKey.outerKey(k);
            return weigher.weigh(k, v);
        };
    }

    public static <K, V> RemovalListener<K, V> removalListener(final RemovalListener<K, V> removalListener) {
        return (k, v, cause) -> {
            k = SubCacheKey.outerKey(k);
            removalListener.onRemoval(k, v, cause);
        };
    }

    static record SubCacheKey(long cacheId, Object key) {
        @SuppressWarnings("unchecked")
        public <T> T outerKey() {
            return (T) key();
        }

        @SuppressWarnings("unchecked")
        public static <T> T outerKey(Object key) {
            if (key instanceof SubCacheKey subKey) {
                return subKey.outerKey();
            }
            return (T) key;
        }
    }
}
