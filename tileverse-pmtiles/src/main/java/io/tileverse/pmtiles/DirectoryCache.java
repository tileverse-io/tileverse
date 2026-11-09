/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CachePool;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.PartitionSpec;
import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Caches the decoded directories of PMTiles archives.
 *
 * <p>The cache is a partition of the {@link CachePool#METADATA} pool, shared with the other metadata caches. Each
 * version of an archive has its own sub-partition, keyed by the {@link RangeReader#contentIdentifier()} of the
 * archive's reader.
 */
@NullMarked
class DirectoryCache {

    private static final String SHARED_CACHE_NAME = "tileverse-pmtiles-directory-cache";

    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(10);

    /**
     * A constant: registering the partition again requires an equal spec, and the pool compares weighers by identity.
     */
    private static final PartitionSpec<ByteRange, PMTilesDirectory> SPEC =
            new PartitionSpec<>(DirectoryCache::weigh, EXPIRE_AFTER_ACCESS);

    @Nullable
    private CacheManager cacheManager;

    private final PMTilesHeader header;
    private final String contentIdentifier;
    private final RangeReader rangeReader;

    public DirectoryCache(String contentIdentifier, PMTilesHeader header, RangeReader rangeReader) {
        this.contentIdentifier = contentIdentifier;
        this.header = header;
        this.rangeReader = rangeReader;
    }

    public void setCacheManager(CacheManager cacheManager) {
        invalidateAll();
        this.cacheManager = cacheManager;
    }

    public void invalidateAll() {
        if (cacheManager != null) {
            cache().invalidateAll();
        }
    }

    public PMTilesDirectory getRootDirectory() throws IOException {
        return getDirectory(header.rootDirectory());
    }

    public PMTilesDirectory getDirectory(PMTilesEntry entry) throws IOException {
        if (entry.isLeaf()) {
            ByteRange leafDirDataRange = header.leafDirDataRange(entry);
            return getDirectory(leafDirDataRange);
        }
        throw new IllegalArgumentException("Entry must be a leaf entry: " + entry);
    }

    private Cache<ByteRange, PMTilesDirectory> cache() {
        if (this.cacheManager == null) {
            this.cacheManager = CacheManager.getDefault();
        }
        CacheManager manager = this.cacheManager;
        Cache<ByteRange, PMTilesDirectory> partition = manager.partition(CachePool.METADATA, SHARED_CACHE_NAME, SPEC);
        return partition.subCache(contentIdentifier);
    }

    /** {@link #cache} loading function */
    PMTilesDirectory readDirectory(ByteRange directoryRange) {
        final Compression compression = Compression.fromCode(header.internalCompression());
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(directoryRange.length())) {
            ByteBuffer buffer = pooled.buffer();
            rangeReader.readRange(directoryRange, buffer);
            buffer.flip();
            try (InputStream in = compression.decompress(buffer)) {
                return DirectoryUtil.deserializeDirectory(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    PMTilesDirectory getDirectory(ByteRange absoluteDirRange) throws IOException {
        try {
            Cache<ByteRange, PMTilesDirectory> cache = cache();
            PMTilesDirectory directory = cache.get(absoluteDirRange, this::readDirectory);
            return Objects.requireNonNull(directory);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    static int weigh(ByteRange key, PMTilesDirectory value) {
        // Approximate overhead of the key record (long + int + object header)
        int keyWeight = 24;

        // Value weight: object header + int field + the actual data in the ByteBuffer
        int valueWeight = 16 + unpackedSize(value);

        return keyWeight + valueWeight;
    }

    private static int unpackedSize(PMTilesDirectory dir) {
        return PMTilesDirectoryImpl.SERIALIZED_ENTRY_SIZE * dir.size();
    }

    CacheStats stats() {
        return cache().stats();
    }
}
