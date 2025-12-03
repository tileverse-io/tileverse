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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteRange;
import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A decorator for {@link RangeReader} that caches ranges on disk for faster
 * subsequent access.
 * <p>
 * This implementation provides a disk-based cache that persists byte ranges to
 * disk, allowing for caching of larger datasets than would fit in memory. It
 * can be combined with {@link CachingRangeReader} for a two-level caching
 * strategy (memory + disk).
 * <p>
 * For optimal performance, consider wrapping this reader with
 * {@link io.tileverse.rangereader.block.BlockAlignedRangeReader} to ensure that all reads are aligned to
 * fixed-sized blocks, which can significantly improve cache efficiency and reduce
 * the number of cache entries by encouraging cache reuse across overlapping ranges.
 * <p>
 * Disk cache entries are stored in a subdirectory within the specified cache
 * directory, where the subdirectory name is based on a hash of the source
 * identifier. Cache files within the subdirectory use filenames based on the
 * range offset and length. Caffeine's built-in LRU eviction policy is applied
 * when the cache exceeds the configured maximum size.
 * <p>
 * <strong>Multi-Instance Sharing:</strong> Multiple DiskCachingRangeReader instances
 * for the same source can share cache files on disk, enabling efficient data sharing.
 * However, each instance maintains its own internal cache view for concurrency control
 * and size management. This means:
 * <ul>
 * <li>Cache files created by one instance are immediately accessible to other instances</li>
 * <li>Each instance may report different cache statistics (entry counts, hit rates)</li>
 * <li>Cache eviction decisions are made independently by each instance</li>
 * <li>An instance may find "surprise cache hits" from files cached by other instances</li>
 * </ul>
 * <p>
 * Inspired by DuckDB's cache_httpfs extension.
 */
public class DiskCachingRangeReader extends AbstractRangeReader implements RangeReader {

    private static final Logger logger = LoggerFactory.getLogger(DiskCachingRangeReader.class);

    private final RangeReader delegate;
    private final Path sourceCacheDirectory;
    private final long maxCacheSizeBytes;
    private final String sourceIdentifier;
    private final boolean deleteOnClose;
    private final String sourceHash;
    private final int blockSize;
    private final boolean alignToBlocks;

    // Default cache max size (1GB)
    static final long DEFAULT_MAX_CACHE_SIZE = 1024 * 1024 * 1024;

    // Default block size (1MB) - good for cloud storage optimization
    static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;

    // Caffeine cache with built-in LRU eviction and size tracking
    private final LoadingCache<ByteRange, Path> cache;

    /**
     * Creates a new DiskCachingRangeReader that caches ranges from the delegate on
     * disk. Package-private constructor - use the builder pattern instead.
     *
     * @param delegate           The delegate RangeReader
     * @param cacheDirectoryRoot The root directory for caches (a subdirectory will
     *                           be created)
     * @param maxCacheSizeBytes  The maximum size of the disk cache in bytes
     * @param deleteOnClose      Whether to delete cached files when this reader is
     *                           closed
     * @param blockSize          The block size for alignment (0 to disable alignment)
     * @throws IOException If an I/O error occurs
     */
    DiskCachingRangeReader(
            RangeReader delegate, Path cacheDirectoryRoot, long maxCacheSizeBytes, boolean deleteOnClose, int blockSize)
            throws IOException {

        this.delegate = Objects.requireNonNull(delegate, "Delegate RangeReader cannot be null");
        Objects.requireNonNull(cacheDirectoryRoot, "Cache directory cannot be null");
        this.sourceIdentifier = delegate.getSourceIdentifier();
        if (maxCacheSizeBytes <= 0) {
            throw new IllegalArgumentException("Max cache size must be positive: " + maxCacheSizeBytes);
        }
        if (blockSize < 0) {
            throw new IllegalArgumentException("Block size cannot be negative: " + blockSize);
        }
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.deleteOnClose = deleteOnClose;
        this.blockSize = blockSize;
        this.alignToBlocks = blockSize > 0;
        this.sourceHash = computeSourceHash();
        this.sourceCacheDirectory = cacheDirectoryRoot.resolve(sourceHash);

        // Create the source-specific cache directory if it doesn't exist
        Files.createDirectories(sourceCacheDirectory);

        // Initialize the Caffeine cache with weight-based eviction and loading function
        this.cache = Caffeine.newBuilder()
                .maximumWeight(this.maxCacheSizeBytes)
                .weigher(this::weighCacheEntry)
                .removalListener(this::onCacheFileRemoval)
                .recordStats()
                .build(this::loadFromDelegate);

        // Initialize the cache from existing files
        initializeCacheFromDisk();
    }

    @Override
    protected int readRangeNoFlip(final long offset, final int actualLength, ByteBuffer target) throws IOException {
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
        // Skip caching for individual blocks larger than the entire cache
        if (blockSize > maxCacheSizeBytes) {
            logger.debug(
                    "Block size too large to cache: {} bytes (max: {}), reading directly from delegate",
                    blockSize,
                    maxCacheSizeBytes);
            return fallbackToDelegate(offset, actualLength, target);
        }

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

        ByteRange cacheKey = new ByteRange(blockStartOffset, cacheKeySize);
        BlockRequest firstRequest = new BlockRequest(cacheKey, offsetWithinBlock, bytesFromThisBlock, targetPosition);

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
            cacheKey = new ByteRange(blockStartOffset, cacheKeySize);
            BlockRequest request = new BlockRequest(cacheKey, offsetWithinBlock, bytesFromThisBlock, targetPosition);
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
            logger.debug("Unable to determine file size, using full block size: {}", e.getMessage());
            return blockSize;
        }
    }

    /**
     * Reads a single block and copies the requested portion to the target buffer.
     */
    private int readSingleBlock(BlockRequest request, ByteBuffer target) throws IOException {
        try {
            // Use Caffeine for concurrency control and cache management
            Path cachePath = cache.get(request.key);

            // Check if we need to update the cache key due to partial read (e.g., EOF)
            if (cachePath != null) {
                String fileName = cachePath.getFileName().toString();
                ByteRange actualKey = parseCacheKey(fileName);
                if (actualKey != null && actualKey.length() != request.key.length()) {
                    cache.put(actualKey, cachePath);
                }
            }

            // Read from the cache file, starting at the offset within the block
            return readFromCacheFileWithOffset(cachePath, target, request.offsetWithinBlock, request.bytesToRead);

        } catch (NoSuchFileException fileDeleted) {
            // Cache file was deleted externally - invalidate and re-cache
            logger.debug("Cache file deleted externally, re-caching: key={}", request.key);
            cache.invalidate(request.key);

            try {
                Path newCachePath = cache.get(request.key);
                return readFromCacheFileWithOffset(
                        newCachePath, target, request.offsetWithinBlock, request.bytesToRead);
            } catch (NoSuchFileException stillDeleted) {
                logger.debug(
                        "Cache file still missing after re-caching, falling back to delegate: key={}", request.key);
                return fallbackToDelegate(
                        request.key.offset() + request.offsetWithinBlock, request.bytesToRead, target);
            }

        } catch (Exception e) {
            logger.warn("Failed to read from cache: key={}, falling back to delegate", request.key, e);
            return fallbackToDelegate(request.key.offset() + request.offsetWithinBlock, request.bytesToRead, target);
        }
    }

    /**
     * Reads multiple blocks in parallel and assembles the result.
     */
    private int readBlocksParallel(List<BlockRequest> blockRequests, ByteBuffer target) throws IOException {
        CompletableFuture<Path>[] futures = new CompletableFuture[blockRequests.size()];

        // Load all blocks in parallel using default ForkJoinPool
        for (int i = 0; i < blockRequests.size(); i++) {
            BlockRequest request = blockRequests.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> cache.get(request.key));
        }

        try {
            // Wait for all blocks to load
            CompletableFuture.allOf(futures).join();

            // Assemble the result by copying data from each block to the target
            int totalBytesRead = 0;
            for (int i = 0; i < blockRequests.size(); i++) {
                BlockRequest request = blockRequests.get(i);
                Path cachePath = futures[i].get();

                // Check if we need to update the cache key due to partial read (e.g., EOF)
                if (cachePath != null) {
                    String fileName = cachePath.getFileName().toString();
                    ByteRange actualKey = parseCacheKey(fileName);
                    if (actualKey != null && actualKey.length() != request.key.length()) {
                        cache.put(actualKey, cachePath);
                    }
                }

                int bytesFromBlock =
                        readFromCacheFileWithOffset(cachePath, target, request.offsetWithinBlock, request.bytesToRead);
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
        // Skip caching for ranges larger than the entire cache
        if (actualLength > maxCacheSizeBytes) {
            logger.debug(
                    "Range too large to cache: {} bytes (max: {}), reading directly from delegate",
                    actualLength,
                    maxCacheSizeBytes);
            return fallbackToDelegate(offset, actualLength, target);
        }

        // Create cache key for the exact range
        ByteRange key = new ByteRange(offset, actualLength);

        try {
            // Use Caffeine for concurrency control and cache management
            Path cachePath = cache.get(key);

            // Check if we need to update the cache key due to partial read
            if (cachePath != null) {
                String fileName = cachePath.getFileName().toString();
                ByteRange actualKey = parseCacheKey(fileName);
                if (actualKey != null && actualKey.length() != key.length()) {
                    cache.put(actualKey, cachePath);
                }
            }

            // Read from the cache file
            return readFromCacheFileWithOffset(cachePath, target, 0, actualLength);

        } catch (NoSuchFileException fileDeleted) {
            // Cache file was deleted externally - invalidate and re-cache
            logger.debug("Cache file deleted externally, re-caching: key={}", key);
            cache.invalidate(key);

            try {
                Path newCachePath = cache.get(key);
                return readFromCacheFileWithOffset(newCachePath, target, 0, actualLength);
            } catch (NoSuchFileException stillDeleted) {
                logger.debug("Cache file still missing after re-caching, falling back to delegate: key={}", key);
                return fallbackToDelegate(offset, actualLength, target);
            }

        } catch (Exception e) {
            logger.warn("Failed to read from cache: key={}, reading directly from delegate", key, e);
            return fallbackToDelegate(offset, actualLength, target);
        }
    }

    /**
     * Reads data from a cache file into the target buffer.
     * This method enables file-level sharing between multiple DiskCachingRangeReader instances.
     *
     * @param cachePath the path to the cache file
     * @param target the target buffer to read into
     * @return the number of bytes read
     * @throws IOException if an I/O error occurs
     */
    private int readFromCacheFile(Path cachePath, ByteBuffer target) throws IOException {
        return readFromCacheFileWithOffset(cachePath, target, 0, target.remaining());
    }

    /**
     * Reads data from a cache file into the target buffer, starting at a specific offset
     * within the cache file and reading only the requested length.
     *
     * @param cachePath the path to the cache file
     * @param target the target buffer to read into
     * @param offsetWithinFile the offset within the cache file to start reading from
     * @param requestedLength the number of bytes to read
     * @return the number of bytes read
     * @throws IOException if an I/O error occurs
     */
    private int readFromCacheFileWithOffset(
            Path cachePath, ByteBuffer target, int offsetWithinFile, int requestedLength) throws IOException {
        try (FileChannel channel = FileChannel.open(cachePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();

            // Check if the offset is within the file
            if (offsetWithinFile >= fileSize) {
                return 0; // Nothing to read
            }

            // Calculate how many bytes we can actually read
            long availableFromOffset = fileSize - offsetWithinFile;
            int bytesToRead = (int) Math.min(Math.min(availableFromOffset, requestedLength), target.remaining());

            if (bytesToRead <= 0) {
                return 0;
            }

            // Position the channel at the correct offset
            channel.position(offsetWithinFile);

            // Read the requested portion
            int totalRead = 0;
            while (totalRead < bytesToRead) {
                int read = channel.read(target);
                if (read == -1) {
                    break; // End of file
                }
                totalRead += read;
            }
            return totalRead;
        }
    }

    private int fallbackToDelegate(final long offset, final int actualLength, ByteBuffer target) throws IOException {
        int readCount = delegate.readRange(offset, actualLength, target);
        // With NIO conventions: delegate.readRange advances position but doesn't flip
        // For readRangeNoFlip contract, position should be advanced by bytes written
        // So no additional position manipulation is needed
        return readCount;
    }

    @Override
    public OptionalLong size() throws IOException {
        return delegate.size();
    }

    @Override
    public String getSourceIdentifier() {
        return "disk-cached:" + delegate.getSourceIdentifier();
    }

    @Override
    public void close() throws IOException {
        try {
            if (deleteOnClose) {
                deleteCacheFiles();
            }
        } finally {
            delegate.close();
        }
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
        return cache.asMap().values().stream()
                .distinct() // Avoid double-counting if multiple keys point to same file
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (IOException e) {
                        return 0; // File doesn't exist or can't be read
                    }
                })
                .sum();
    }

    /**
     * Gets the maximum size of the disk cache in bytes.
     *
     * @return The maximum cache size
     */
    long getMaxCacheSize() {
        return maxCacheSizeBytes;
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
     * Clears the disk cache, removing all cached entries and their corresponding cache files.
     * <p>
     * This method invalidates all entries in the cache and triggers the removal listener
     * to delete the associated cache files from disk. After this method completes,
     * all cache files known to this reader instance will have been removed from the
     * file system.
     * <p>
     * Note: If multiple DiskCachingRangeReader instances share the same cache directory
     * and source, this will only remove cache files known to this specific instance.
     * Cache files created by other instances may remain on disk.
     */
    public void clearCache() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    /**
     * Deletes all cache files for this source by removing the entire
     * source-specific cache subdirectory. This removes both cached entries and
     * their corresponding files on disk.
     */
    private void deleteCacheFiles() {
        // Delete the entire source-specific cache directory
        try {
            if (Files.isDirectory(sourceCacheDirectory)) {
                try (Stream<Path> files = Files.walk(sourceCacheDirectory)) {
                    files.sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                    logger.debug("Deleted cache path: {}", path);
                                } catch (IOException e) {
                                    logger.warn("Failed to delete cache path: {}", path, e);
                                }
                            });
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to delete source cache directory: {}", sourceCacheDirectory, e);
        }
        cache.invalidateAll();
        cache.cleanUp();
    }

    /**
     * Initializes the cache by scanning the cache directory.
     *
     * @throws IOException If an I/O error occurs
     */
    private void initializeCacheFromDisk() throws IOException {
        // Scan the source-specific cache directory for cache files
        if (Files.exists(sourceCacheDirectory)) {
            try (Stream<Path> files = Files.list(sourceCacheDirectory)) {
                files.filter(Files::isRegularFile).forEach(p -> {
                    try {
                        // Parse the filename to extract offset and length
                        ByteRange key = parseCacheKey(p.getFileName().toString());
                        if (key != null) {
                            // Add to the cache
                            cache.put(key, p);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to process cache file: {}", p, e);
                    }
                });
            }
        }

        if (logger.isInfoEnabled()) {
            long count = cache.estimatedSize();
            long size = this.getEstimatedCacheSizeBytes();
            logger.info("Initialized disk cache with {} entries, total size: {} bytes", count, size);
        }
    }

    /**
     * Loading function for Caffeine cache. Loads a range from the delegate reader
     * and caches it.
     *
     * @param key The cache key
     * @return Path to the cached file
     * @throws IOException If an I/O error occurs
     */
    private Path loadFromDelegate(ByteRange key) throws IOException {
        logger.debug("Cache miss for key: offset={}, length={}", key.offset(), key.length());

        // Read the data from the delegate
        try (var pooled = ByteBufferPool.directBuffer(key.length())) {
            ByteBuffer buffer = pooled.buffer();
            int bytesRead = delegate.readRange(key.offset(), key.length(), buffer);

            // It's acceptable to read fewer bytes if we reached EOF
            if (bytesRead <= 0) {
                throw new IOException("Failed to read data from delegate reader");
            }

            // Update the key's length to match what was actually read, if different
            if (bytesRead != key.length()) {
                logger.debug("Partial read from delegate: requested {} bytes, got {}", key.length(), bytesRead);
                key = new ByteRange(key.offset(), bytesRead);
            }

            // Generate a stable file name based on the key (after potential update from
            // partial read)
            String cacheFileName = computeFileName(key);
            Path cachePath = sourceCacheDirectory.resolve(cacheFileName);

            // Ensure the source cache directory exists before writing
            Files.createDirectories(sourceCacheDirectory);

            // Write the data to the cache file
            try (RandomAccessFile file = new RandomAccessFile(cachePath.toFile(), "rw");
                    FileChannel channel = file.getChannel()) {
                // Flip the buffer to prepare it for writing (position becomes 0, limit becomes data end)
                buffer.flip();
                channel.write(buffer);
            } catch (IOException e) {
                // Clean up if we couldn't write
                try {
                    Files.deleteIfExists(cachePath);
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
            logger.debug("Added to disk cache: offset={}, length={}, path={}", key.offset(), key.length(), cachePath);
            return cachePath;
        }
    }

    private String computeFileName(ByteRange key) {
        long rangeStart = key.offset();
        long rangeEnd = rangeStart + key.length() - 1;
        return String.format("%d_%d.range", rangeStart, rangeEnd);
    }

    /**
     * Weighs a cache entry for Caffeine's eviction policy based on file size.
     * Since we validate that cache entries don't exceed Integer.MAX_VALUE,
     * we can safely cast to int.
     *
     * @param key  The cache key
     * @param path The path to the cached file
     * @return The weight of the cache entry (file size in bytes)
     */
    private int weighCacheEntry(ByteRange key, Path path) {
        try {
            return (int) Files.size(path);
        } catch (IOException e) {
            // If we can't get the file size, assume it's gone and return 0
            return 0;
        }
    }

    /**
     * Computes a hash of the source identifier for use in cache file names.
     *
     * @return A hash string for the source
     */
    private String computeSourceHash() {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(sourceIdentifier.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString().substring(0, 8); // Use first 8 chars for brevity
        } catch (NoSuchAlgorithmException e) {
            // Fallback to simple hash for unlikely case MD5 is not available
            return String.format("%08x", sourceIdentifier.hashCode());
        }
    }

    /**
     * Parses a cache file name to extract the cache key.
     *
     * @param fileName The cache file name
     * @return The parsed cache key, or null if the format is invalid
     */
    private ByteRange parseCacheKey(String fileName) {
        String[] parts = fileName.split("_");
        if (parts.length < 2) {
            return null;
        }

        try {
            long rangeStart = Long.parseLong(parts[0]);
            long rangeEnd = Long.parseLong(parts[1].replace(".range", ""));
            int length = (int) (rangeEnd - rangeStart + 1);
            return new ByteRange(rangeStart, length);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Handles cache file removal when entries are evicted from the cache.
     *
     * @param key The cache key
     * @param path The path to the cache file
     * @param cause The reason for removal
     */
    private void onCacheFileRemoval(ByteRange key, Path path, @NonNull RemovalCause cause) {
        if (path != null && Files.isRegularFile(path)) {
            try {
                Files.delete(path);
                logger.debug("Removed from disk cache: path={}, cause={}", path, cause);
            } catch (NoSuchFileException alreadyRemoved) {
                logger.debug("File already removed from disk cache: path={}, cause={}", path, cause);
            } catch (IOException e) {
                logger.warn("Failed to delete cache file: {}", path, e);
            }
        }
    }

    /**
     * Creates a new builder for DiskCachingRangeReader with the mandatory delegate parameter.
     *
     * @param delegate the delegate RangeReader to wrap with disk caching
     * @return a new builder instance with the delegate set
     */
    public static Builder builder(RangeReader delegate) {
        return new Builder(delegate);
    }

    /**
     * Builder for DiskCachingRangeReader.
     */
    public static class Builder {
        private final RangeReader delegate;
        private Path cacheDirectory;
        private Long maxCacheSizeBytes;
        private boolean deleteOnClose = false;
        private Integer blockSize;

        private Builder(RangeReader delegate) {
            this.delegate = Objects.requireNonNull(delegate, "Delegate cannot be null");
        }

        /**
         * Sets the cache directory. If not set, defaults to a subdirectory
         * {@literal tileverse-rangereader-cache} in the system temporary directory.
         *
         * @param cacheDirectoryRoot the root directory for caches (a subdirectory will
         *                           be created)
         * @return this builder
         */
        public Builder cacheDirectory(Path cacheDirectoryRoot) {
            this.cacheDirectory = Objects.requireNonNull(cacheDirectoryRoot, "Cache directory cannot be null");
            return this;
        }

        /**
         * Sets the cache directory from a string path. If not set, defaults to a
         * subdirectory {@literal tileverse-rangereader-cache} in the system temporary
         * directory.
         *
         * @param cacheDirectoryPath the directory path as a string
         * @return this builder
         */
        public Builder cacheDirectory(String cacheDirectoryPath) {
            Objects.requireNonNull(cacheDirectoryPath, "Cache directory path cannot be null");
            this.cacheDirectory = Paths.get(cacheDirectoryPath);
            return this;
        }

        /**
         * Sets the maximum cache size in bytes.
         *
         * @param maxCacheSizeBytes the maximum cache size in bytes
         * @return this builder
         */
        public Builder maxCacheSizeBytes(long maxCacheSizeBytes) {
            if (maxCacheSizeBytes <= 0) {
                throw new IllegalArgumentException("Max cache size must be positive: " + maxCacheSizeBytes);
            }
            this.maxCacheSizeBytes = maxCacheSizeBytes;
            return this;
        }

        /**
         * Sets whether to delete cached files when the reader is closed. This is useful
         * for temporary caching scenarios where you want to clean up after processing
         * is complete.
         *
         * @param deleteOnClose true to delete cached files on close, false to keep them
         * @return this builder
         */
        public Builder deleteOnClose(boolean deleteOnClose) {
            this.deleteOnClose = deleteOnClose;
            return this;
        }

        /**
         * Configures the reader to delete cached files when closed. This is equivalent
         * to calling {@code deleteOnClose(true)}.
         *
         * @return this builder
         */
        public Builder deleteOnClose() {
            return deleteOnClose(true);
        }

        /**
         * Sets the block size for internal block alignment. When set, the cache will
         * align reads to block boundaries for better cache efficiency and reduced
         * cache fragmentation.
         * <p>
         * For example, if block size is 1MB and you request 1 byte at offset 500000,
         * the cache will read and store the entire 1MB block containing that byte,
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
         * Enables block alignment with the default block size (1MB).
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
         * Builds the DiskCachingRangeReader.
         *
         * @return a new DiskCachingRangeReader instance
         * @throws IOException           if an error occurs during construction
         * @throws IllegalStateException if required parameters are not set
         */
        public DiskCachingRangeReader build() throws IOException {
            // Use default temporary directory if none specified
            Path effectiveCacheDirectory = cacheDirectory != null
                    ? cacheDirectory
                    : Paths.get(System.getProperty("java.io.tmpdir"), "tileverse-rangereader-cache");

            long effectiveMaxCacheSize = maxCacheSizeBytes != null ? maxCacheSizeBytes : DEFAULT_MAX_CACHE_SIZE;
            int effectiveBlockSize = blockSize != null ? blockSize : 0; // Default: no block alignment
            return new DiskCachingRangeReader(
                    delegate, effectiveCacheDirectory, effectiveMaxCacheSize, deleteOnClose, effectiveBlockSize);
        }
    }
}
