/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.store;

import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CachePool;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.PartitionSpec;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.vectortile.model.VectorTile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
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
 * <p>Tiles are grouped by the content identifier of their source, the
 * {@link io.tileverse.storage.RangeReader#contentIdentifier()} of the archive's reader. The cache is a partition of the
 * {@link CachePool#DATA} pool, shared with the byte-range cache.
 */
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

    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofSeconds(10);

    /**
     * A constant: registering the partition again requires an equal spec, and the pool compares weighers by identity.
     */
    private static final PartitionSpec<TileIndex, Optional<VectorTile>> SPEC =
            new PartitionSpec<>(VectorTileCache::weigh, EXPIRE_AFTER_ACCESS);

    private final String contentIdentifier;

    private final VectorTileLoader loader;

    @Nullable
    private CacheManager cacheManager;

    /**
     * Creates a new cache instance for the specified source.
     *
     * @param contentIdentifier the {@link io.tileverse.storage.RangeReader#contentIdentifier()} of the archive's reader
     * @param loader the loader used to fetch and decode tiles
     */
    public VectorTileCache(String contentIdentifier, VectorTileLoader loader) {
        this.contentIdentifier = Objects.requireNonNull(contentIdentifier);
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
    private Cache<TileIndex, Optional<VectorTile>> cache() {
        if (this.cacheManager == null) {
            this.cacheManager = CacheManager.getDefault();
        }
        CacheManager manager = this.cacheManager;
        Cache<TileIndex, Optional<VectorTile>> partition = manager.partition(CachePool.DATA, SHARED_CACHE_NAME, SPEC);
        return partition.subCache(contentIdentifier);
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

    CacheStats stats() {
        return cache().stats();
    }
}
