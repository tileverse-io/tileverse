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

import com.github.benmanes.caffeine.cache.CacheLoader;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A semi-persistent mapping from keys to values. Cache entries are manually
 * added using {@link #get(Object, Function)} and are stored in the cache until
 * either evicted or manually invalidated.
 * <p>
 * Implementations of this interface are expected to be thread-safe and can be
 * safely accessed by multiple concurrent threads.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values. If and only if a cache declares a
 *            nullable value type, then its loading function may return null
 *            values, and the cache will return those null values to callers.
 *            (Null values are still never <i>stored</i> in the cache.)
 */
public interface Cache<K, V extends Object> {

    public Cache<K, V> subCache(@NonNull String subCacheId);

    /**
     * Returns the value associated with the {@code key} in this cache, or
     * {@code null} if there is no cached value for the {@code key}.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or {@code null} if
     *         this cache does not contain a mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    @Nullable
    V getIfPresent(@NonNull K key);

    void forEach(BiConsumer<K, V> consumer);

    /**
     * Returns the value associated with the {@code key} in this cache, obtaining
     * that value from the {@code mappingFunction} if necessary. This method
     * provides a simple substitute for the conventional "if cached, return;
     * otherwise create, cache and return" pattern.
     * <p>
     * If the specified key is not already associated with a value, attempts to
     * compute its value using the given mapping function and enters it into this
     * cache unless {@code null}. The entire method invocation is performed
     * atomically, so the function is applied at most once per key. Some attempted
     * update operations on this cache by other threads may be blocked while the
     * computation is in progress, so the computation should be short and simple,
     * and must not attempt to update any other mappings of this cache.
     * <p>
     * <b>Warning:</b> as with {@link CacheLoader#load}, {@code mappingFunction}
     * <b>must not</b> attempt to update any other mappings of this cache.
     *
     * @param key             the key with which the specified value is to be
     *                        associated
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with the
     *         specified key, or {@code null} if the computed value is null
     * @throws NullPointerException  if the specified key or mappingFunction is null
     * @throws IllegalStateException if the computation detectably attempts a
     *                               recursive update to this cache that would
     *                               otherwise never complete
     * @throws RuntimeException      or Error if the mappingFunction does so, in
     *                               which case the mapping is left unestablished
     */
    @Nullable
    V get(@NonNull K key, @NonNull Function<? super K, ? extends V> mappingFunction);

    @NonNull
    CacheStats stats();

    /**
     * Discards all entries in the cache. The behavior of this operation is
     * undefined for an entry that is being loaded (or reloaded) and is otherwise
     * not present.
     *
     * @return this
     */
    @NonNull
    Cache<K, V> invalidateAll();

    /**
     * Discards any cached value for the {@code key}. The behavior of this operation
     * is undefined for an entry that is being loaded (or reloaded) and is otherwise
     * not present.
     *
     * @param key the key whose mapping is to be removed from the cache
     * @throws NullPointerException if the specified key is null
     */
    void invalidate(@NonNull K key);

    /**
     * Discards any cached values for the {@code keys}. The behavior of this
     * operation is undefined for an entry that is being loaded (or reloaded) and is
     * otherwise not present.
     *
     * @param keys the keys whose associated values are to be removed
     * @throws NullPointerException if the specified collection is null or contains
     *                              a null element
     */
    void invalidateAll(@NonNull Iterable<? extends K> keys);

    /**
     * @return the keys available in the cache
     */
    @NonNull
    Collection<K> keys();
}
