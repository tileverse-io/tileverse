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
package io.tileverse.rangereader.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.tileverse.io.ByteRange;
import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A decorator for RangeReader that adds in-memory caching capabilities using Caffeine.
 * <p>
 * This implementation caches recently accessed byte ranges to improve
 * performance for repeated reads, which is common when accessing the same data
 * ranges multiple times.
 * <p>
 * <strong>Cache Configuration:</strong>
 * The cache can be configured with explicit size limits for predictable memory usage:
 * <ul>
 * <li><strong>Entry-based sizing:</strong> Use {@code maximumSize(long)} to limit the number of cached ranges</li>
 * <li><strong>Memory-based sizing:</strong> Use {@code maximumWeight(long)} or {@code maxSizeBytes(long)}
 *     to limit total memory usage (entries are automatically weighted by ByteBuffer capacity)</li>
 * <li><strong>Adaptive sizing (default):</strong> If no size limits are specified, soft references are used
 *     automatically, allowing the garbage collector to manage cache size based on memory pressure</li>
 * </ul>
 * <p>
 * <strong>Optional Configuration:</strong>
 * <ul>
 * <li><strong>Time-based expiration:</strong> Use {@code expireAfterAccess(duration, unit)} to automatically
 *     remove entries after a period of inactivity</li>
 * <li><strong>Memory pressure handling:</strong> Use {@code softValues()} to allow the garbage collector
 *     to reclaim cache entries when memory is needed</li>
 * </ul>
 * <p>
 * <strong>Usage Examples:</strong>
 * <pre>{@code
 * // Adaptive cache (default) - GC manages size automatically
 * CachingRangeReader reader = CachingRangeReader.builder(delegate)
 *     .build();
 *
 * // Fixed entry-count based cache
 * CachingRangeReader reader = CachingRangeReader.builder(delegate)
 *     .maximumSize(1000)
 *     .build();
 *
 * // Fixed memory-size based cache with expiration and block alignment
 * CachingRangeReader reader = CachingRangeReader.builder(delegate)
 *     .maximumWeight(64 * 1024 * 1024) // 64MB
 *     .expireAfterAccess(30, TimeUnit.MINUTES)
 *     .withBlockAlignment() // Enable 64KB block alignment
 *     .build();
 * }</pre>
 * <p>
 * <strong>Block Alignment:</strong>
 * The cache supports internal block alignment that can significantly improve cache
 * efficiency by encouraging cache reuse across overlapping ranges. When enabled,
 * the cache aligns reads to block boundaries and caches larger blocks but returns
 * only the requested bytes to the caller.
 * <ul>
 * <li><strong>Default behavior:</strong> Block alignment is disabled by default to maintain
 *     backward compatibility</li>
 * <li><strong>Enable with default size:</strong> Use {@code withBlockAlignment()} for 64KB blocks</li>
 * <li><strong>Custom block size:</strong> Use {@code blockSize(int)} to specify a custom block size</li>
 * <li><strong>Explicit disable:</strong> Use {@code withoutBlockAlignment()} to explicitly disable</li>
 * </ul>
 */
public class CachingRangeReader extends AbstractRangeReader implements RangeReader {

    private final RangeReader delegate;
    private final Cache<ByteRange, ByteBuffer> cache;
    private final int blockSize;
    private final boolean alignToBlocks;

    // Default block size (64KB) - good for memory cache optimization
    static final int DEFAULT_BLOCK_SIZE = 64 * 1024;

    // Default header buffer size (128KB)
    static final int DEFAULT_HEADER_SIZE = 128 * 1024;

    private final ByteBuffer header;

    /**
     * Creates a new CachingRangeReader with the provided cache.
     * Package-private constructor - use the builder pattern instead.
     *
     * @param delegate The underlying RangeReader to delegate to
     * @param cache    The cache to use for storing byte ranges
     * @param blockSize The block size for alignment (0 to disable alignment)
     * @param headerSize The size of the header buffer (0 to disable header buffering)
     */
    CachingRangeReader(RangeReader delegate, Cache<ByteRange, ByteBuffer> cache, int blockSize, int headerSize) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate RangeReader cannot be null");
        this.cache = Objects.requireNonNull(cache, "Cache cannot be null");
        if (blockSize < 0) {
            throw new IllegalArgumentException("Block size cannot be negative: " + blockSize);
        }
        if (headerSize < 0) {
            throw new IllegalArgumentException("Header size cannot be negative: " + headerSize);
        }
        this.blockSize = blockSize;
        this.alignToBlocks = blockSize > 0;

        // Initialize header buffer if header size > 0
        if (headerSize > 0) {
            try {
                ByteBuffer buff = ByteBuffer.allocate(headerSize);
                delegate.readRange(0, headerSize, buff);
                this.header = buff.flip().asReadOnlyBuffer();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to initialize header buffer", e);
            }
        } else {
            header = null;
        }
    }

    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException {
        // Use header buffer if available and range is within header bounds
        if (header != null) {
            long rangeEnd = offset + actualLength;
            if (rangeEnd <= header.limit()) {
                ByteBuffer h = header.duplicate();
                h.position((int) offset);
                h.limit((int) rangeEnd);
                target.put(h);
                return actualLength;
            }
        }

        if (alignToBlocks) {
            // Handle block-aligned reads by potentially reading from multiple single-block cache entries
            return readRangeWithBlockAlignment(offset, actualLength, target);
        } else {
            // No alignment - cache exactly what was requested
            return readRangeWithoutAlignment(offset, actualLength, target);
        }
    }

    /**
     * Reads a range with block alignment, potentially spanning multiple single-block cache entries.
     * Uses parallel loading for multi-block requests to improve performance.
     */
    private int readRangeWithBlockAlignment(final long offset, final int actualLength, ByteBuffer target)
            throws IOException {
        // Compute which blocks we need
        List<BlockRequest> blockRequests = computeRequiredBlocks(offset, actualLength, target.remaining());

        if (blockRequests.isEmpty()) {
            return 0;
        }

        if (blockRequests.size() == 1) {
            // Single block - handle directly
            return readSingleBlock(blockRequests.get(0), target);
        } else {
            // Multiple blocks - load in parallel
            return readBlocksParallel(blockRequests, target);
        }
    }

    /**
     * Computes the block requests needed to satisfy the given range request.
     */
    private List<BlockRequest> computeRequiredBlocks(long offset, int actualLength, int targetRemaining) {
        long currentOffset = offset;
        int remainingBytes = Math.min(actualLength, targetRemaining);
        int targetPosition = 0;

        // Calculate the first block request
        long blockStartOffset = (currentOffset / blockSize) * blockSize;
        int offsetWithinBlock = (int) (currentOffset - blockStartOffset);
        int availableInBlock = blockSize - offsetWithinBlock;
        int bytesFromThisBlock = Math.min(availableInBlock, remainingBytes);
        int cacheKeySize = computeBlockSize(blockStartOffset);

        BlockRequest firstRequest = new BlockRequest(
                new ByteRange(blockStartOffset, cacheKeySize), offsetWithinBlock, bytesFromThisBlock, targetPosition);

        // Check if we need only one block - optimize for the common case
        if (bytesFromThisBlock >= remainingBytes) {
            return List.of(firstRequest);
        }

        // Multiple blocks needed - use ArrayList
        List<BlockRequest> requests = new ArrayList<>();
        requests.add(firstRequest);

        // Move to next block and continue
        currentOffset += bytesFromThisBlock;
        remainingBytes -= bytesFromThisBlock;
        targetPosition += bytesFromThisBlock;

        while (remainingBytes > 0) {
            // Calculate the block-aligned cache offset for the current position
            blockStartOffset = (currentOffset / blockSize) * blockSize;
            offsetWithinBlock = (int) (currentOffset - blockStartOffset);

            // Calculate how many bytes we need from this block
            availableInBlock = blockSize - offsetWithinBlock;
            bytesFromThisBlock = Math.min(availableInBlock, remainingBytes);

            // Determine the appropriate cache key size, considering EOF
            cacheKeySize = computeBlockSize(blockStartOffset);

            // Create block request
            BlockRequest request = new BlockRequest(
                    new ByteRange(blockStartOffset, cacheKeySize),
                    offsetWithinBlock,
                    bytesFromThisBlock,
                    targetPosition);
            requests.add(request);

            // Move to next block
            currentOffset += bytesFromThisBlock;
            remainingBytes -= bytesFromThisBlock;
            targetPosition += bytesFromThisBlock;
        }

        return requests;
    }

    /**
     * Computes the appropriate block size for a cache key starting at the given offset.
     * This accounts for EOF by ensuring the block size doesn't extend beyond the file size.
     *
     * @param blockStartOffset the starting offset of the block
     * @return the appropriate block size (may be less than blockSize if near EOF)
     */
    private int computeBlockSize(long blockStartOffset) {
        try {
            OptionalLong fileSize = delegate.size();
            if (fileSize.isEmpty()) {
                return blockSize;
            }
            long maxPossibleSize = fileSize.getAsLong() - blockStartOffset;

            // If the full block size fits within the file, use it
            if (maxPossibleSize >= blockSize) {
                return blockSize;
            }

            // Otherwise, use the remaining bytes (but ensure it's positive)
            return (int) Math.max(0, maxPossibleSize);
        } catch (IOException e) {
            // If we can't determine file size, assume full block size
            return blockSize;
        }
    }

    /**
     * Reads a single block and copies the requested portion to the target buffer.
     */
    private int readSingleBlock(BlockRequest request, ByteBuffer target) throws IOException {
        try {
            // Use Caffeine's get-or-create pattern
            ByteBuffer cachedBuffer = cache.get(request.key, this::loadRange);

            // Copy the requested portion to the target
            return copyBlockData(cachedBuffer, request, target);

        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Failed to read block: " + request.key, e);
        }
    }

    /**
     * Reads multiple blocks in parallel and assembles the result.
     */
    private int readBlocksParallel(List<BlockRequest> blockRequests, ByteBuffer target) throws IOException {
        @SuppressWarnings("unchecked")
        CompletableFuture<ByteBuffer>[] futures = new CompletableFuture[blockRequests.size()];

        // Load all blocks in parallel using default ForkJoinPool
        for (int i = 0; i < blockRequests.size(); i++) {
            BlockRequest request = blockRequests.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> cache.get(request.key, this::loadRange));
        }

        try {
            // Wait for all blocks to load
            CompletableFuture.allOf(futures).join();

            // Assemble the result by copying data from each block to the target
            int totalBytesRead = 0;
            for (int i = 0; i < blockRequests.size(); i++) {
                BlockRequest request = blockRequests.get(i);
                ByteBuffer blockData = futures[i].get();

                int bytesFromBlock = copyBlockData(blockData, request, target);
                totalBytesRead += bytesFromBlock;

                // If we read fewer bytes than expected, we've hit EOF
                if (bytesFromBlock < request.bytesToRead) {
                    break;
                }
            }

            return totalBytesRead;

        } catch (Exception e) {
            // Handle any exceptions from the parallel loading
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to read blocks in parallel", e);
        }
    }

    /**
     * Copies data from a cached block to the target buffer according to the block request.
     */
    private int copyBlockData(ByteBuffer blockData, BlockRequest request, ByteBuffer target) {
        // Duplicate to avoid affecting the cached version
        ByteBuffer duplicate = blockData.duplicate();

        // Calculate how many bytes we can actually copy
        int availableFromOffset = duplicate.remaining() - request.offsetWithinBlock;
        int bytesToCopy = Math.min(Math.min(availableFromOffset, request.bytesToRead), target.remaining());

        if (bytesToCopy <= 0) {
            return 0;
        }

        // Position the duplicate to the correct offset within the block
        duplicate.position(request.offsetWithinBlock);
        duplicate.limit(request.offsetWithinBlock + bytesToCopy);

        // Copy the data to the target
        target.put(duplicate);

        return bytesToCopy;
    }

    /**
     * Represents a request for data from a specific block.
     */
    private record BlockRequest(
            ByteRange key, // The cache key for the block
            int offsetWithinBlock, // Offset within the block to start reading
            int bytesToRead, // Number of bytes to read from this block
            int targetPosition // Position in target buffer (for future use)
            ) {}

    /**
     * Reads a range without block alignment, caching exactly what was requested.
     */
    private int readRangeWithoutAlignment(final long offset, final int actualLength, ByteBuffer target)
            throws IOException {
        // Create a cache key for the exact range
        ByteRange key = new ByteRange(offset, actualLength);

        // Use Caffeine's get-or-create pattern
        ByteBuffer cachedBuffer = cache.get(key, this::loadRange);

        // Duplicate the cached buffer to avoid position/limit changes affecting the cached version
        ByteBuffer duplicate = cachedBuffer.duplicate();

        // Copy the data from the cached buffer into the target
        int bytesRead = duplicate.remaining();
        target.put(duplicate);

        return bytesRead;
    }

    private ByteBuffer loadRange(ByteRange key) {
        try {
            // Allocate a buffer for the cache entry
            ByteBuffer blockData = ByteBuffer.allocateDirect(key.length());

            // Read data directly into the buffer
            int bytesRead = this.delegate.readRange(key.offset(), key.length(), blockData);

            // Handle partial reads (e.g., EOF) by creating a buffer with only the actual data
            if (bytesRead < key.length() && bytesRead > 0) {
                ByteBuffer actualData = ByteBuffer.allocate(bytesRead);
                blockData.flip();
                actualData.put(blockData);
                actualData.flip(); // Flip to prepare for reading
                return actualData.asReadOnlyBuffer();
            }

            // Flip the buffer to prepare it for reading by cache consumers
            blockData.flip();
            return blockData.asReadOnlyBuffer();
        } catch (IOException e) {
            // Propagate the exception through
            throw new UncheckedIOException("Failed to load range from delegate", e);
        }
    }

    @Override
    public OptionalLong size() throws IOException {
        return delegate.size();
    }

    @Override
    public String getSourceIdentifier() {
        return "memory-cached:" + delegate.getSourceIdentifier();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        cache.invalidateAll();
    }

    /**
     * Clears the cache, forcing subsequent reads to go to the underlying source.
     */
    public void clearCache() {
        cache.invalidateAll();
    }

    /**
     * Gets the current number of entries in the cache.
     *
     * @return The number of cached entries
     */
    long getCacheEntryCount() {
        return cache.estimatedSize();
    }

    /**
     * Gets the estimated cache size in bytes.
     *
     * @return The estimated cache size in bytes
     */
    long getEstimatedCacheSizeBytes() {
        // Calculate estimated size by summing the capacity of all cached ByteBuffers
        return cache.asMap().values().stream().mapToLong(ByteBuffer::capacity).sum();
    }

    /**
     * Gets the cache statistics.
     *
     * @return The cache statistics
     */
    public CacheStats getCacheStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats = cache.stats();
        long entryCount = cache.estimatedSize();
        long estimatedSizeBytes = getEstimatedCacheSizeBytes();

        return CacheStats.fromCaffeine(caffeineStats, entryCount, estimatedSizeBytes);
    }

    /**
     * Creates a new builder for CachingRangeReader with the mandatory delegate
     * parameter.
     *
     * @param delegate the delegate RangeReader to wrap with caching
     * @return a new builder instance with the delegate set
     */
    public static Builder builder(RangeReader delegate) {
        return new Builder(delegate);
    }

    /**
     * Builder for CachingRangeReader with configurable cache settings.
     */
    public static class Builder {
        private final RangeReader delegate;
        private Long maximumSize;
        private Long maximumWeight;
        private Long expireAfterAccessDuration;
        private TimeUnit expireAfterAccessUnit;
        private boolean softValues = false;
        private Integer blockSize;
        private Integer headerSize;

        private Builder(RangeReader delegate) {
            this.delegate = Objects.requireNonNull(delegate, "Delegate cannot be null");
        }

        /**
         * Sets the maximum number of entries the cache can contain.
         * Cannot be used together with {@link #maximumWeight(long)}.
         *
         * @param maximumSize the maximum number of entries
         * @return this builder
         */
        public Builder maximumSize(long maximumSize) {
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("Maximum size must be positive: " + maximumSize);
            }
            if (this.maximumWeight != null) {
                throw new IllegalStateException("Cannot set both maximumSize and maximumWeight");
            }
            this.maximumSize = maximumSize;
            return this;
        }

        /**
         * Sets the maximum weight of entries the cache can contain.
         * When using this method, entries are weighted by their ByteBuffer capacity.
         * Cannot be used together with {@link #maximumSize(long)}.
         *
         * @param maximumWeight the maximum weight in bytes
         * @return this builder
         */
        public Builder maximumWeight(long maximumWeight) {
            if (maximumWeight <= 0) {
                throw new IllegalArgumentException("Maximum weight must be positive: " + maximumWeight);
            }
            if (this.maximumSize != null) {
                throw new IllegalStateException("Cannot set both maximumSize and maximumWeight");
            }
            this.maximumWeight = maximumWeight;
            return this;
        }

        /**
         * Sets the duration after which entries are automatically removed from the cache
         * following their last access.
         *
         * @param duration the duration
         * @param unit     the time unit
         * @return this builder
         */
        public Builder expireAfterAccess(long duration, TimeUnit unit) {
            if (duration <= 0) {
                throw new IllegalArgumentException("Duration must be positive: " + duration);
            }
            this.expireAfterAccessDuration = duration;
            this.expireAfterAccessUnit = Objects.requireNonNull(unit, "Time unit cannot be null");
            return this;
        }

        /**
         * Enables soft values, allowing the garbage collector to evict entries when memory is needed.
         * This can help prevent OutOfMemoryError in memory-constrained environments.
         *
         * @return this builder
         */
        public Builder softValues() {
            return softValues(true);
        }

        /**
         * Enables soft values with the specified setting.
         *
         * @param softValues true to enable soft values, false to use strong references
         * @return this builder
         */
        public Builder softValues(boolean softValues) {
            this.softValues = softValues;
            return this;
        }

        /**
         * Sets the block size for internal block alignment. When set, the cache will
         * align reads to block boundaries for better cache efficiency and reduced
         * cache fragmentation.
         * <p>
         * For example, if block size is 64KB and you request 1 byte at offset 50000,
         * the cache will read and store the entire 64KB block containing that byte,
         * but only return the requested 1 byte to the caller.
         *
         * @param blockSize the block size in bytes (must be positive, 0 disables alignment)
         * @return this builder
         * @throws IllegalArgumentException if blockSize is negative
         */
        public Builder blockSize(int blockSize) {
            if (blockSize < 0) {
                throw new IllegalArgumentException("Block size cannot be negative: " + blockSize);
            }
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Enables block alignment with the default block size (64KB).
         * This is equivalent to calling {@code blockSize(DEFAULT_BLOCK_SIZE)}.
         *
         * @return this builder
         */
        public Builder withBlockAlignment() {
            return blockSize(DEFAULT_BLOCK_SIZE);
        }

        /**
         * Disables block alignment by setting block size to 0.
         * This is equivalent to calling {@code blockSize(0)}.
         *
         * @return this builder
         */
        public Builder withoutBlockAlignment() {
            return blockSize(0);
        }

        /**
         * Sets the size of the header buffer for caching the beginning of the file
         * in memory. When enabled, reads from the beginning of the file (up to the
         * header size) are served directly from this buffer without going through
         * the cache.
         * <p>
         * This optimization is useful for file formats where metadata is stored at
         * the beginning of the file and accessed frequently.
         *
         * @param headerSize the header buffer size in bytes (0 to disable header buffering)
         * @return this builder
         * @throws IllegalArgumentException if headerSize is negative
         */
        public Builder headerSize(int headerSize) {
            if (headerSize < 0) {
                throw new IllegalArgumentException("Header size cannot be negative: " + headerSize);
            }
            this.headerSize = headerSize;
            return this;
        }

        /**
         * Enables header buffering with the default header size (128KB).
         * This is equivalent to calling {@code headerSize(DEFAULT_HEADER_SIZE)}.
         *
         * @return this builder
         */
        public Builder withHeaderBuffer() {
            return headerSize(DEFAULT_HEADER_SIZE);
        }

        /**
         * Disables header buffering by setting header size to 0.
         * This is equivalent to calling {@code headerSize(0)}.
         *
         * @return this builder
         */
        public Builder withoutHeaderBuffer() {
            return headerSize(0);
        }

        /**
         * Builds the CachingRangeReader with the configured cache settings.
         *
         * @return a new CachingRangeReader instance
         */
        public CachingRangeReader build() {
            // Start with a base Caffeine builder
            Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().recordStats();

            // Configure size limits
            if (maximumSize != null) {
                cacheBuilder.maximumSize(maximumSize);
            } else if (maximumWeight != null) {
                cacheBuilder
                        .maximumWeight(maximumWeight)
                        .weigher((ByteRange key, ByteBuffer value) -> value.capacity());
            } else {
                // Default to soft values when no size limit is specified
                // This prevents OutOfMemoryError while still providing caching benefits
                softValues = true;
            }

            // Configure expiration (only if explicitly set)
            if (expireAfterAccessDuration != null && expireAfterAccessUnit != null) {
                cacheBuilder.expireAfterAccess(expireAfterAccessDuration, expireAfterAccessUnit);
            }

            // Configure value references
            if (softValues) {
                cacheBuilder.softValues();
            }

            int effectiveBlockSize = blockSize != null ? blockSize : 0; // Default: no block alignment
            int effectiveHeaderSize = headerSize != null ? headerSize : 0; // Default: no header buffer
            return new CachingRangeReader(delegate, cacheBuilder.build(), effectiveBlockSize, effectiveHeaderSize);
        }
    }
}
