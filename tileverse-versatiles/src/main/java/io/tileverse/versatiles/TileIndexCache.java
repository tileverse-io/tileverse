/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CachePool;
import io.tileverse.cache.PartitionSpec;
import io.tileverse.storage.RangeReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Keeps the tile indexes of recently read blocks in a cache of a {@link CacheManager}.
 *
 * <p>A tile index takes a range request and a Brotli decompression to load, and holds up to 768 KiB for a block of 256
 * x 256 tiles.
 *
 * <p>The cache is a partition of the {@link CachePool#METADATA} pool, shared with the other metadata caches. Each
 * version of an archive has its own sub-partition, keyed by the {@link RangeReader#contentIdentifier()} of the
 * archive's reader. Readers of the same archive version share entries.
 */
@NullMarked
final class TileIndexCache {

    private static final String SHARED_CACHE_NAME = "tileverse-versatiles-tile-index-cache";

    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(10);

    /**
     * A constant: registering the partition again requires an equal spec, and the pool compares weighers by identity.
     */
    private static final PartitionSpec<BlockIndexEntry, TileIndexBlob> SPEC =
            new PartitionSpec<>(TileIndexCache::weigh, EXPIRE_AFTER_ACCESS);

    /** Approximate heap overhead of a cache entry: the key record, the tile index object, and the array header. */
    private static final int ENTRY_OVERHEAD_BYTES = 128;

    private final String contentIdentifier;

    private final BlobReader blobReader;

    @Nullable
    @SuppressWarnings("java:S3077") // CacheManager implementations are thread-safe; volatile publishes the reference
    private volatile CacheManager cacheManager;

    TileIndexCache(String contentIdentifier, BlobReader blobReader) {
        this.contentIdentifier = requireNonNull(contentIdentifier);
        this.blobReader = requireNonNull(blobReader);
    }

    /** Drops the entries of this archive from the current cache manager and keeps later entries in {@code manager}. */
    void setCacheManager(CacheManager manager) {
        invalidateAll();
        this.cacheManager = requireNonNull(manager);
    }

    TileIndexBlob getTileIndex(BlockIndexEntry block) throws IOException {
        try {
            TileIndexBlob tileIndex = cache().get(block, this::load);
            return requireNonNull(tileIndex);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Drops the entries of this archive, if a cache manager holds any. */
    void invalidateAll() {
        if (cacheManager != null) {
            cache().invalidateAll();
        }
    }

    private Cache<BlockIndexEntry, TileIndexBlob> cache() {
        CacheManager manager = cacheManager;
        if (manager == null) {
            manager = CacheManager.getDefault();
            cacheManager = manager;
        }
        Cache<BlockIndexEntry, TileIndexBlob> partition =
                manager.partition(CachePool.METADATA, SHARED_CACHE_NAME, SPEC);
        return partition.subCache(contentIdentifier);
    }

    private TileIndexBlob load(BlockIndexEntry block) {
        try {
            return blobReader.readBrotli(block.tileIndexRange(), in -> TileIndexBlob.parse(in, block.slotCount()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int weigh(BlockIndexEntry block, TileIndexBlob tileIndex) {
        return ENTRY_OVERHEAD_BYTES + tileIndex.sizeInBytes();
    }
}
