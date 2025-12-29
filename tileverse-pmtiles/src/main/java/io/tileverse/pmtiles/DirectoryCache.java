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
package io.tileverse.pmtiles;

import static io.tileverse.pmtiles.CompressionUtil.decompressAndTransform;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CaffeineCache;
import io.tileverse.io.ByteRange;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NullMarked
class DirectoryCache {

    private static final Logger log = LoggerFactory.getLogger("io.tileverse.pmtiles");

    private static final String SHARED_CACHE_NAME = "tileverse-pmtiles-directory-cache";

    private static final int DEFAULT_MAX_HEAP_PERCENT = 5;

    private static final Duration DEFAULT_EXPIRE_AFTER_ACCESS = Duration.ofSeconds(10);

    private int maxHeapPercent = DEFAULT_MAX_HEAP_PERCENT;

    private Duration expireAfterAccess = DEFAULT_EXPIRE_AFTER_ACCESS;

    @Nullable
    private CacheManager cacheManager;

    private final PMTilesHeader header;
    private final String pmtilesUniqueUri;
    private final Supplier<SeekableByteChannel> channelSupplier;

    public DirectoryCache(
            String pmtilesUniqueUri, PMTilesHeader header, Supplier<SeekableByteChannel> channelSupplier) {
        this.pmtilesUniqueUri = pmtilesUniqueUri;
        this.header = header;
        this.channelSupplier = channelSupplier;
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

    private io.tileverse.cache.Cache<ByteRange, PMTilesDirectory> cache() {
        if (this.cacheManager == null) {
            this.cacheManager = CacheManager.getDefault();
        }
        CacheManager manager = this.cacheManager;
        return manager.getCache(SHARED_CACHE_NAME, this::buildCache).subCache(pmtilesUniqueUri);
    }

    /**
     * Short-lived (expireAfterAccess) cache of directory entries to account for
     * multiple/concurrent requests
     */
    private io.tileverse.cache.Cache<ByteRange, PMTilesDirectory> buildCache() {
        return CaffeineCache.<ByteRange, PMTilesDirectory>newBuilder()
                .maxHeapPercent(maxHeapPercent, DirectoryCache::weigh)
                .averageWeight(1024 * 1024)
                .expireAfterAccess(expireAfterAccess)
                // run eviction tasks when idle to return memory without waiting for cache usage
                .scheduler(Scheduler.systemScheduler())
                .removalListener(this::onRemoval)
                .recordStats()
                .build();
    }

    /**
     * {@link #cache} loading function
     */
    PMTilesDirectory readDirectory(ByteRange directoryRange) {
        try (SeekableByteChannel channel = channelSupplier.get()) {
            final byte compression = header.internalCompression();
            return decompressAndTransform(channel, directoryRange, compression, DirectoryUtil::deserializeDirectory);
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

    void onRemoval(ByteRange key, PMTilesDirectory value, RemovalCause cause) {
        if (log.isTraceEnabled()) {
            log.trace("Evicted {}, {}, cause: {}", value, key, cause);
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

    io.tileverse.cache.CacheStats stats() {
        return cache().stats();
    }
}
