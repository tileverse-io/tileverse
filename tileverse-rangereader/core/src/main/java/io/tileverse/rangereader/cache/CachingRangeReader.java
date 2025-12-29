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

import static java.util.Objects.requireNonNull;

import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.io.ByteRange;
import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

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
    private final RangeReaderCache cache;
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
    CachingRangeReader(RangeReader delegate, RangeReaderCache cache, int blockSize, int headerSize) {
        this.delegate = requireNonNull(delegate, "Delegate RangeReader cannot be null");
        this.cache = requireNonNull(cache, "Cache cannot be null");
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
        ByteBuffer cachedBuffer = cache.get(request.key);
        return copyBlockData(cachedBuffer, request, target);
    }

    /**
     * Reads multiple blocks and assembles the result.
     * Note: With the blocking cache API, parallel loading happens at the cache level
     * when multiple threads request different blocks.
     */
    private int readBlocksParallel(List<BlockRequest> blockRequests, ByteBuffer target) throws IOException {
        int totalBytesRead = 0;
        for (BlockRequest request : blockRequests) {
            ByteBuffer blockData = cache.get(request.key);
            int bytesFromBlock = copyBlockData(blockData, request, target);
            totalBytesRead += bytesFromBlock;

            // If we read fewer bytes than expected, we've hit EOF
            if (bytesFromBlock < request.bytesToRead) {
                break;
            }
        }
        return totalBytesRead;
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

        ByteBuffer cachedBuffer = cache.get(key);
        if (cachedBuffer == null) {
            return 0;
        }

        // Duplicate the cached buffer to avoid position/limit changes affecting the cached version
        ByteBuffer duplicate = cachedBuffer.duplicate();

        // Copy the data from the cached buffer into the target
        int bytesRead = duplicate.remaining();
        target.put(duplicate);

        return bytesRead;
    }

    @Override
    public OptionalLong size() throws IOException {
        return delegate.size();
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
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
        return cache.stats().entryCount();
    }

    /**
     * Gets the estimated cache size in bytes.
     *
     * @return The estimated cache size in bytes
     */
    long getEstimatedCacheSizeBytes() {
        return cache.getEstimatedCacheSizeBytes();
    }

    /**
     * Gets the cache statistics.
     *
     * @return The cache statistics
     */
    public CacheStats getCacheStats() {
        return cache.stats();
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
        private Integer blockSize;
        private Integer headerSize;
        private CacheManager cacheManager = CacheManager.getDefault();

        private Builder(RangeReader delegate) {
            this.delegate = requireNonNull(delegate, "Delegate cannot be null");
        }

        public Builder cacheManager(CacheManager cacheManager) {
            this.cacheManager = requireNonNull(cacheManager);
            return this;
        }

        /**
         * Builds the CachingRangeReader with the configured cache settings.
         *
         * @return a new CachingRangeReader instance
         */
        public CachingRangeReader build() {
            RangeReaderCache cache = new RangeReaderCache(cacheManager, delegate);
            int effectiveBlockSize = blockSize != null ? blockSize : 0; // Default: no block alignment
            int effectiveHeaderSize = headerSize != null ? headerSize : 0; // Default: no header buffer
            return new CachingRangeReader(delegate, cache, effectiveBlockSize, effectiveHeaderSize);
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
    }
}
