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

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskCachingRangeReaderBlockAlignmentTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private Path cacheDir;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test.bin");
        cacheDir = tempDir.resolve("cache");

        // Create a test file with 100KB of random data
        byte[] data = new byte[100 * 1024];
        new Random(42).nextBytes(data); // Deterministic random data
        Files.write(testFile, data);
    }

    @Test
    void blockAlignment_cachesLargerBlockButReturnsOnlyRequestedBytes() throws IOException {
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Configure with 4KB block alignment
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .blockSize(4096) // 4KB blocks
                .build()) {

            // Request 1 byte at offset 2000 (within the middle of a 4KB block)
            ByteBuffer result = cachingReader.readRange(2000, 1);

            // Should return exactly 1 byte
            assertThat(result.flip().remaining()).isEqualTo(1);

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
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Configure with 4KB block alignment
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .blockSize(4096) // 4KB blocks
                .build()) {

            // Request byte at offset 2000 (will cache block 0-4096)
            ByteBuffer result1 = cachingReader.readRange(2000, 1);
            assertThat(result1.flip().remaining()).isEqualTo(1);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(1);

            // Request bytes at offset 3000 (should use same cached block)
            ByteBuffer result2 = cachingReader.readRange(3000, 100);
            assertThat(result2.flip().remaining()).isEqualTo(100);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(1); // Still only 1 block cached

            // Request bytes at offset 5000 (should cache a new block 4096-8192)
            ByteBuffer result3 = cachingReader.readRange(5000, 50);
            assertThat(result3.flip().remaining()).isEqualTo(50);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Now 2 blocks cached
        }
    }

    @Test
    void blockAlignment_withSpanningRequest() throws IOException {
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Configure with 4KB block alignment
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .blockSize(4096) // 4KB blocks
                .build()) {

            // Request data spanning multiple blocks (3500 to 5500 = 2000 bytes)
            // This spans from block 0 (3500 is in 0-4096) to block 1 (5500 is in 4096-8192)
            ByteBuffer result = cachingReader.readRange(3500, 2000);

            // Should return exactly 2000 bytes
            assertThat(result.flip().remaining()).isEqualTo(2000);

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
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Configure without block alignment (default)
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .withoutBlockAlignment() // Explicitly disable
                .build()) {

            // Request 1 byte at offset 2000
            ByteBuffer result = cachingReader.readRange(2000, 1);

            // Should return exactly 1 byte
            assertThat(result.flip().remaining()).isEqualTo(1);

            // Should have cached exactly 1 byte
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(1);
            assertThat(cachingReader.getEstimatedCacheSizeBytes()).isEqualTo(1);
        }
    }

    @Test
    void blockAlignment_builderMethods() throws IOException {
        // Test different builder methods - each creates its own base reader
        try (RangeReader baseReader1 = FileRangeReader.of(testFile);
                DiskCachingRangeReader reader1 = DiskCachingRangeReader.builder(baseReader1)
                        .cacheDirectory(cacheDir.resolve("reader1"))
                        .withBlockAlignment() // Uses default 1MB
                        .build()) {

            // Request 1 byte - should cache 1MB block (but limited by file size)
            reader1.readRange(1000, 1);
            assertThat(reader1.getEstimatedCacheSizeBytes()).isEqualTo(102400); // File is only 100KB
        }

        try (RangeReader baseReader2 = FileRangeReader.of(testFile);
                DiskCachingRangeReader reader2 = DiskCachingRangeReader.builder(baseReader2)
                        .cacheDirectory(cacheDir.resolve("reader2"))
                        .blockSize(8192) // Custom 8KB blocks
                        .build()) {

            // Request 1 byte - should cache 8KB block
            reader2.readRange(1000, 1);
            assertThat(reader2.getEstimatedCacheSizeBytes()).isEqualTo(8192);
        }

        try (RangeReader baseReader3 = FileRangeReader.of(testFile);
                DiskCachingRangeReader reader3 = DiskCachingRangeReader.builder(baseReader3)
                        .cacheDirectory(cacheDir.resolve("reader3"))
                        .withoutBlockAlignment() // Disable alignment
                        .build()) {

            // Request 1 byte - should cache exactly 1 byte
            reader3.readRange(1000, 1);
            assertThat(reader3.getEstimatedCacheSizeBytes()).isEqualTo(1);
        }
    }

    @Test
    void blockAlignment_eofHandling_avoidsRedundantRequests() throws IOException {
        RangeReader baseReader = FileRangeReader.of(testFile);

        // Configure with 16KB block alignment (file is 100KB = 102400 bytes)
        try (DiskCachingRangeReader cachingReader = DiskCachingRangeReader.builder(baseReader)
                .cacheDirectory(cacheDir)
                .blockSize(16384) // 16KB blocks
                .build()) {

            // Request data near the end of file (spanning across the final block boundary)
            // offset 98000 is in block 5 (81920-98303), request extends into final block 6 (98304-102399)
            ByteBuffer result1 = cachingReader.readRange(98000, 1000); // Should load 2 blocks
            assertThat(result1.flip().remaining()).isEqualTo(1000);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Block 5 + final partial block 6

            // Request data from the same final block - should NOT make new delegate calls
            ByteBuffer result2 = cachingReader.readRange(99000, 1000); // Also spans the same 2 blocks
            assertThat(result2.flip().remaining()).isEqualTo(1000);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Same number of cache entries

            // Request data only from the final partial block
            ByteBuffer result3 = cachingReader.readRange(100000, 1000); // Only in final block
            assertThat(result3.flip().remaining()).isEqualTo(1000);
            assertThat(cachingReader.getCacheEntryCount()).isEqualTo(2); // Still same cache entries

            // Verify the final block is correctly sized (not full 16KB)
            long estimatedSize = cachingReader.getEstimatedCacheSizeBytes();
            // Block 5: 16KB (81920 to 98303) + Final block: 4KB (98304 to 102399) = 20KB total
            assertThat(estimatedSize).isEqualTo(20 * 1024);
        }
    }
}
