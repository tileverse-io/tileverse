/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
import java.util.OptionalInt;
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

    /**
     * Estimated average weight of a cached VectorTile entry in bytes.
     *
     * <p>This value accounts for:
     *
     * <ul>
     *   <li>Key/Value Overhead: 52 bytes for the key and the {@code Optional} holding the tile.
     *   <li>Binary Size: An assumed average tile payload of 75-80 KB.
     *   <li>Heap Size: About 2.5 times the binary size, as estimated by {@link VectorTile#heapSize()} for a decoded
     *       tile. A tile without an estimate weighs 5 times its binary size.
     * </ul>
     *
     * Used by {@link #buildCache()} and effectively by {@code CaffeineCache#Builder}'s {@code computeInitialCapacity()}
     * to pre-size internal data structures and minimize GC churn during cache warm-up.
     */
    private static final int AVERAGE_TILE_WEIGHT = 1024 * 200;

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
        return manager.getCache(SHARED_CACHE_NAME, VectorTileCache::buildCache).subCache(sourceIdentifier);
    }

    // static, as the shared cache outlives this instance and must not reference it
    private static io.tileverse.cache.Cache<TileIndex, Optional<VectorTile>> buildCache() {
        return CaffeineCache.<TileIndex, Optional<VectorTile>>newBuilder()
                .maxHeapPercent(DEFAULT_MAX_HEAP_PERCENT, VectorTileCache::weigh)
                .averageWeight(AVERAGE_TILE_WEIGHT)
                .expireAfterAccess(DEFAULT_EXPIRE_AFTER_ACCESS)
                // run eviction tasks when idle to return memory without waiting for cache usage
                .scheduler(Scheduler.systemScheduler())
                .removalListener(VectorTileCache::onRemoval)
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

    static void onRemoval(TileIndex key, Optional<VectorTile> value, RemovalCause cause) {
        if (log.isTraceEnabled()) {
            log.trace("Evicted {}, {}, cause: {}", value, key, cause);
        }
    }

    /**
     * Returns the weight of a cache entry: the bytes of the key, of the {@code Optional} and of the tile. The tile
     * gives its size through {@link VectorTile#heapSize()}. A tile without an estimate weighs 5 times its encoded size.
     *
     * <p>The weight counts the bytes of the objects held by the tile. G1 stores an array of at least half a region in
     * whole regions. A tile decoded by {@code VectorTileCodec} keeps a copy of its encoded bytes. With the 2 MiB
     * regions of a 3 GB heap, the copy of a 1.1 MiB tile takes 2 MiB, and such a tile holds more memory than its
     * weight. G1 sizes its regions from the maximum heap, from 1 MiB for a heap of up to 2 GB to 32 MiB for a heap
     * above 32 GB.
     */
    static int weigh(TileIndex key, Optional<VectorTile> value) {
        // Key overhead: Record header (12) + long (8) + long (8) + int (4) + padding
        // (4) = 36 bytes.
        int keyWeight = 36;

        // Value overhead:
        if (value.isEmpty()) {
            return keyWeight + 16; // Optional.empty is a singleton
        }

        // Optional header (12) + reference (4) = 16 bytes.
        long valueWeight = 16L + tileWeight(value.get());

        return (int) Math.min(keyWeight + valueWeight, Integer.MAX_VALUE);
    }

    // An implementation without an estimate may decode the tile into objects, several times larger than its bytes
    private static long tileWeight(VectorTile tile) {
        OptionalInt heapSize = tile.heapSize();
        if (heapSize.isPresent()) {
            return heapSize.getAsInt();
        }
        int serializedSize = tile.serializedSize().orElse(0);
        return 5L * serializedSize;
    }

    io.tileverse.cache.CacheStats stats() {
        return cache().stats();
    }
}
