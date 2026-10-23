/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.versatiles;

import static java.util.Objects.requireNonNull;

import com.github.benmanes.caffeine.cache.Scheduler;
import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CaffeineCache;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Keeps the tile indexes of recently read blocks in a cache of a {@link CacheManager}.
 *
 * <p>A tile index takes a range request and a Brotli decompression to load, and holds up to 768 KiB for a block of 256
 * x 256 tiles. Readers using the same {@link CacheManager} share the cache, with one sub-cache per archive: readers of
 * the same archive share entries.
 */
@NullMarked
final class TileIndexCache {

    private static final String SHARED_CACHE_NAME = "tileverse-versatiles-tile-index-cache";

    private static final int MAX_HEAP_PERCENT = 5;

    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofSeconds(30);

    /** Approximate heap overhead of a cache entry: the key record, the tile index object, and the array header. */
    private static final int ENTRY_OVERHEAD_BYTES = 128;

    /** Sizes the initial capacity of the cache: a block of about 5,000 tiles has a tile index of 64 KiB. */
    private static final int AVERAGE_ENTRY_BYTES = 64 * 1024;

    private final String sourceIdentifier;

    private final BlobReader blobReader;

    @Nullable
    @SuppressWarnings("java:S3077") // CacheManager implementations are thread-safe; volatile publishes the reference
    private volatile CacheManager cacheManager;

    TileIndexCache(String sourceIdentifier, BlobReader blobReader) {
        this.sourceIdentifier = requireNonNull(sourceIdentifier);
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
        Cache<BlockIndexEntry, TileIndexBlob> shared = manager.getCache(SHARED_CACHE_NAME, TileIndexCache::newCache);
        return shared.subCache(sourceIdentifier);
    }

    private TileIndexBlob load(BlockIndexEntry block) {
        try {
            return blobReader.readBrotli(block.tileIndexRange(), in -> TileIndexBlob.parse(in, block.slotCount()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static CaffeineCache<BlockIndexEntry, TileIndexBlob> newCache() {
        return CaffeineCache.<BlockIndexEntry, TileIndexBlob>newBuilder()
                .maxHeapPercent(MAX_HEAP_PERCENT, TileIndexCache::weigh)
                .averageWeight(AVERAGE_ENTRY_BYTES)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                // evicts expired entries while the cache is idle, instead of on the next access
                .scheduler(Scheduler.systemScheduler())
                .recordStats()
                .build();
    }

    private static int weigh(BlockIndexEntry block, TileIndexBlob tileIndex) {
        return ENTRY_OVERHEAD_BYTES + tileIndex.sizeInBytes();
    }
}
