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
package io.tileverse.vectortile.store;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CaffeineCache;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.vectortile.model.VectorTile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * A format-neutral, thread-safe cache for decoded {@link VectorTile} objects.
 *
 * <p>This cache is keyed by {@link TileIndex} and is designed to minimize the overhead of decoding vector tiles when
 * multiple layers or features are requested for the same tile. It uses a {@link VectorTileLoader} to fetch and decode
 * tiles from an underlying source.
 *
 * <p>The cache is partitioned by a {@code sourceIdentifier} to allow sharing a single cache instance across multiple
 * vector tile sources while avoiding key collisions.
 */
@Slf4j
@NullMarked
public class VectorTileCache {

    /** Functional interface for loading a decoded {@link VectorTile} for a specific tile index. */
    @FunctionalInterface
    public interface VectorTileLoader {
        /**
         * Loads and decodes a vector tile.
         *
         * @param tileIndex the index of the tile to load
         * @return an optional containing the decoded vector tile, or empty if it doesn't exist
         * @throws IOException if an error occurs during loading or decoding
         */
        Optional<VectorTile> load(TileIndex tileIndex) throws IOException;
    }

    private static final String SHARED_CACHE_NAME = "tileverse-vectortile-cache";

    private static final int DEFAULT_MAX_HEAP_PERCENT = 15;

    private static final Duration DEFAULT_EXPIRE_AFTER_ACCESS = Duration.ofSeconds(10);

    private int maxHeapPercent = DEFAULT_MAX_HEAP_PERCENT;
    private static final Duration expireAfterAccess = DEFAULT_EXPIRE_AFTER_ACCESS;

    /**
     * Estimated average weight of a cached VectorTile entry in bytes.
     *
     * <p>This value accounts for:
     *
     * <ul>
     *   <li>Key/Value Overhead: Approximately 100 bytes of JVM object metadata.
     *   <li>Binary Size: An assumed average tile payload of 75-80 KB.
     *   <li>Inflation: A 5x multiplier to account for in-memory expansion of Protobuf objects compared to their
     *       serialized form.
     * </ul>
     *
     * Used by {@link #buildCache()} and effectively by {@code CaffeineCache#Builder}'s {@code computeInitialCapacity()}
     * to pre-size internal data structures and minimize GC churn during cache warm-up.
     */
    private static final int AVERAGE_TILE_WEIGHT = 1024 * 400;

    private final String sourceIdentifier;

    private final VectorTileLoader loader;

    @Nullable
    private CacheManager cacheManager;

    /**
     * Creates a new cache instance for the specified source.
     *
     * @param sourceIdentifier a unique identifier for the tile source
     * @param loader the loader used to fetch and decode tiles
     */
    public VectorTileCache(String sourceIdentifier, VectorTileLoader loader) {
        this.sourceIdentifier = Objects.requireNonNull(sourceIdentifier);
        this.loader = Objects.requireNonNull(loader);
    }

    /**
     * Sets the cache manager to use for this cache.
     *
     * <p>If a different manager was previously set, all existing entries in this cache will be invalidated.
     *
     * @param cacheManager the cache manager to use
     */
    public void setCacheManager(CacheManager cacheManager) {
        if (this.cacheManager != null) {
            cache().invalidateAll();
        }
        this.cacheManager = cacheManager;
    }

    /**
     * Short-lived (expireAfterAccess) {@link VectorTile} cache to account for consecutive single-layer requests.
     *
     * <p>Since {@link VectorTile} objects are immutable and relatively expensive to decode, caching them improves
     * performance when multiple layers are requested for the same tile.
     */
    private final io.tileverse.cache.Cache<TileIndex, Optional<VectorTile>> cache() {
        if (this.cacheManager == null) {
            this.cacheManager = CacheManager.getDefault();
        }
        CacheManager manager = this.cacheManager;
        return manager.getCache(SHARED_CACHE_NAME, this::buildCache).subCache(sourceIdentifier);
    }

    private io.tileverse.cache.Cache<TileIndex, Optional<VectorTile>> buildCache() {
        return CaffeineCache.<TileIndex, Optional<VectorTile>>newBuilder()
                .maxHeapPercent(maxHeapPercent, VectorTileCache::weigh)
                .averageWeight(AVERAGE_TILE_WEIGHT)
                .expireAfterAccess(expireAfterAccess)
                // run eviction tasks when idle to return memory without waiting for cache usage
                .scheduler(Scheduler.systemScheduler())
                .removalListener(this::onRemoval)
                .recordStats()
                .build();
    }

    /**
     * Retrieves a decoded vector tile from the cache, loading it through the configured {@link VectorTileLoader} on a
     * miss.
     *
     * @param tileIndex the index of the tile to retrieve
     * @return an optional containing the vector tile, or empty if no tile exists at that index
     * @throws IOException if an error occurs while loading the tile
     */
    @SuppressWarnings("java:S2637") // can't return null
    public Optional<VectorTile> getVectorTile(TileIndex tileIndex) throws IOException {
        try {
            return cache().get(tileIndex, this::loadVectorTile);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @NonNull
    private Optional<VectorTile> loadVectorTile(TileIndex tileIndex) {
        try {
            return loader.load(tileIndex);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void onRemoval(TileIndex key, Optional<VectorTile> value, RemovalCause cause) {
        if (log.isTraceEnabled()) {
            log.trace("Evicted {}, {}, cause: {}", value, key, cause);
        }
    }

    static int weigh(TileIndex key, Optional<VectorTile> value) {
        // Key overhead: Record header (12) + long (8) + long (8) + int (4) + padding
        // (4) = 36 bytes.
        int keyWeight = 36;

        // Value overhead:
        if (value.isEmpty()) {
            return keyWeight + 16; // Optional.empty is a singleton
        }

        // Protobuf expansion:
        // In-memory Protobuf objects are typically 4x to 8x larger than their binary size.
        // A multiplier of 5 is a safe, conservative industry standard for Caffeine.
        int serializedSize = value.get().serializedSize().orElse(0);
        int valueWeight = 32 + (serializedSize * 5);

        return keyWeight + valueWeight;
    }

    io.tileverse.cache.CacheStats stats() {
        return cache().stats();
    }
}
