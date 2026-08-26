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
package io.tileverse.storage.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.cache.CacheManager;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.block.BlockAlignedRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins the cache footprint (entry count, estimated size) produced by stacking {@link BlockAlignedRangeReader} above
 * {@link CachingRangeReader}, reproducing the block-grain caching the cache itself used to provide directly.
 */
class BlockAlignedCachingFootprintTest {

    @TempDir
    private Path tempDir;

    private Path testFile;
    private ByteBuffer result = ByteBuffer.allocate(2000);
    private CacheManager cacheManager;
    private CachingRangeReader cachingReader;

    @BeforeEach
    void setUp() throws IOException {
        result.clear();
        testFile = tempDir.resolve("test.bin");

        // Create a test file with 100KB of random data
        byte[] data = new byte[100 * 1024];
        new Random(42).nextBytes(data); // Deterministic random data
        Files.write(testFile, data);
        cacheManager = CacheManager.newInstance();
    }

    @AfterEach
    void cleanUp() {
        cacheManager.invalidateAll();
    }

    /** Builds a cache over {@code base} and an aligner over that cache, keeping the cache reference in the field. */
    private BlockAlignedRangeReader alignedOverCache(RangeReader base, int blockSize) {
        this.cachingReader =
                CachingRangeReader.builder(base).cacheManager(cacheManager).build();
        return BlockAlignedRangeReader.builder(cachingReader)
                .blockSize(blockSize)
                .alignWholeFile()
                .build();
    }

    @Test
    void blockAlignment_cachesLargerBlockButReturnsOnlyRequestedBytes() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
                BlockAlignedRangeReader reader = alignedOverCache(baseReader, 4096)) {

            // Request 1 byte at offset 2000 (within the middle of a 4KB block)
            reader.readRange(2000, 1, result);
            result.flip();
            // Should return exactly 1 byte
            assertThat(result.remaining()).isEqualTo(1);

            // But should have cached a 4KB block (from offset 0 to 4096)
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(1);
            assertThat(cachingReader.getEstimatedCacheSizeBytes()).isEqualTo(4096);

            // Verify the returned byte is correct
            byte expectedByte = Files.readAllBytes(testFile)[2000];
            assertThat(result.get()).isEqualTo(expectedByte);
        }
    }

    @Test
    void blockAlignment_reusesBlockForOverlappingRequests() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
                BlockAlignedRangeReader reader = alignedOverCache(baseReader, 4096)) {

            // Request byte at offset 2000 (will cache block 0-4096)
            reader.readRange(2000, 1, result);
            result.flip();
            assertThat(result.remaining()).isEqualTo(1);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(1);

            // Request bytes at offset 3000 (should use same cached block)
            reader.readRange(3000, 100, result.clear());
            result.flip();
            assertThat(result.remaining()).isEqualTo(100);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(1); // Still only 1 block cached

            // Request bytes at offset 5000 (should cache a new block 4096-8192)
            reader.readRange(5000, 50, result.clear());
            result.flip();
            assertThat(result.remaining()).isEqualTo(50);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Now 2 blocks cached
        }
    }

    @Test
    void blockAlignment_withSpanningRequest() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
                BlockAlignedRangeReader reader = alignedOverCache(baseReader, 4096)) {

            // Request data spanning multiple blocks (3500 to 5500 = 2000 bytes)
            // This spans from block 0 (3500 is in 0-4096) to block 1 (5500 is in 4096-8192)
            reader.readRange(3500, 2000, result);
            result.flip();
            // Should return exactly 2000 bytes
            assertThat(result.remaining()).isEqualTo(2000);

            // Should have cached two separate blocks: block 0 (0-4096) and block 1 (4096-8192)
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2);
            assertThat(cachingReader.getEstimatedCacheSizeBytes()).isEqualTo(8192); // 2 blocks worth

            // Verify the data is correct
            byte[] expectedBytes = new byte[2000];
            System.arraycopy(Files.readAllBytes(testFile), 3500, expectedBytes, 0, 2000);
            byte[] actualBytes = new byte[2000];
            result.get(actualBytes);
            assertThat(actualBytes).isEqualTo(expectedBytes);
        }
    }

    @Test
    void noBlockAlignment_cachesExactlyWhatWasRequested() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
                CachingRangeReader reader = CachingRangeReader.builder(baseReader)
                        .cacheManager(cacheManager)
                        .build()) {

            // Request 1 byte at offset 2000
            reader.readRange(2000, 1, result);
            result.flip();

            // Should return exactly 1 byte
            assertThat(result.remaining()).isEqualTo(1);

            // Should have cached exactly 1 byte
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);
            assertThat(reader.getEstimatedCacheSizeBytes()).isEqualTo(1);
        }
    }

    @Test
    void blockAlignment_builderMethods() throws IOException {
        // Default 64KB blocks
        try (RangeReader baseReader1 = RangeReaderTestSupport.fileReader(testFile);
                BlockAlignedRangeReader reader1 =
                        alignedOverCache(baseReader1, BlockAlignedRangeReader.DEFAULT_BLOCK_SIZE)) {

            // Request 1 byte - should cache 64KB block (but limited by file size)
            reader1.readRange(1000, 1, result);
            assertThat(cachingReader.getEstimatedCacheSizeBytes()).isEqualTo(65536); // Default 64KB block
        }

        // Custom 8KB blocks
        try (RangeReader baseReader2 = RangeReaderTestSupport.fileReader(testFile);
                BlockAlignedRangeReader reader2 = alignedOverCache(baseReader2, 8192)) {

            // Request 1 byte - should cache 8KB block
            reader2.readRange(1000, 1, result);
            assertThat(cachingReader.getEstimatedCacheSizeBytes()).isEqualTo(8192);
        }

        // No aligner
        try (RangeReader baseReader3 = RangeReaderTestSupport.fileReader(testFile);
                CachingRangeReader reader3 = CachingRangeReader.builder(baseReader3)
                        .cacheManager(cacheManager)
                        .build()) {

            // Request 1 byte - should cache exactly 1 byte
            reader3.readRange(1000, 1, result);
            assertThat(reader3.getEstimatedCacheSizeBytes()).isEqualTo(1);
        }
    }

    @Test
    void blockAlignment_defaultBehaviorWithoutConfiguration() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
                CachingRangeReader reader = CachingRangeReader.builder(baseReader)
                        .cacheManager(cacheManager)
                        .build()) {

            // Request 1 byte at offset 2000
            reader.readRange(2000, 1, result);
            result.flip();
            // Should return exactly 1 byte
            assertThat(result.remaining()).isEqualTo(1);

            // Should have cached exactly 1 byte
            assertThat(reader.getCacheEntryCount()).isEqualTo(1);
            assertThat(reader.getEstimatedCacheSizeBytes()).isEqualTo(1);
        }
    }

    @Test
    void blockAlignment_eofHandling_avoidsRedundantRequests() throws IOException {
        try (RangeReader baseReader = RangeReaderTestSupport.fileReader(testFile);
                BlockAlignedRangeReader reader = alignedOverCache(baseReader, 16384)) {

            // Request data near the end of file (spanning across the final block boundary)
            // offset 98000 is in block 5 (81920-98303), request extends into final block 6 (98304-102399)
            reader.readRange(98000, 1000, result); // Should load 2 blocks
            result.flip();
            assertThat(result.remaining()).isEqualTo(1000);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Block 5 + final partial block 6

            // Request data from the same final block - should NOT make new delegate calls
            reader.readRange(99000, 1000, result.clear()); // Also spans the same 2 blocks
            result.flip();
            assertThat(result.remaining()).isEqualTo(1000);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Same number of cache entries

            // Request data only from the final partial block
            reader.readRange(100000, 1000, result.clear()); // Only in final block
            result.flip();
            assertThat(result.remaining()).isEqualTo(1000);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Still same cache entries

            // Verify the final block is correctly sized (not full 16KB)
            long estimatedSize = cachingReader.getEstimatedCacheSizeBytes();
            // Block 5: 16KB (81920 to 98303) + Final block: 4KB (98304 to 102399) = 20KB total
            assertThat(estimatedSize).isEqualTo(20 * 1024);
        }
    }
}
