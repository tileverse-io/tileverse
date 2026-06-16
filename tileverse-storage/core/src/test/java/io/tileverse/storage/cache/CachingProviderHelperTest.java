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
package io.tileverse.storage.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.spi.CachingProviderHelper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that {@link CachingProviderHelper#cachingDecoratorFor(StorageConfig)} honors the value of
 * {@code storage.caching.blockaligned}, not merely its presence. Drives the real decorator entry point and asserts the
 * observable cache footprint of a 1-byte read. Lives in {@code io.tileverse.storage.cache} to read the package-private
 * {@link CachingRangeReader#getEstimatedCacheSizeBytes()}.
 */
class CachingProviderHelperTest {

    @TempDir
    Path tempDir;

    @Test
    void blockalignedFalseDisablesAlignment() throws IOException {
        Path testFile = writeRandomFile();

        StorageConfig opts = new StorageConfig(testFile.toUri())
                .setParameter(CachingProviderHelper.MEMORY_CACHE_ENABLED, true)
                .setParameter(CachingProviderHelper.MEMORY_CACHE_BLOCK_ALIGNED, false);

        try (RangeReader base = RangeReaderTestSupport.fileReader(testFile);
                CachingRangeReader reader = (CachingRangeReader) CachingProviderHelper.cachingDecoratorFor(opts)
                        .orElseThrow()
                        .apply(base)) {

            ByteBuffer out = ByteBuffer.allocate(16);
            reader.readRange(2000, 1, out);

            assertThat(reader.getEstimatedCacheSizeBytes()).isEqualTo(1L);
        }
    }

    @Test
    void blockalignedDefaultStillAligns() throws IOException {
        Path testFile = writeRandomFile();

        StorageConfig opts =
                new StorageConfig(testFile.toUri()).setParameter(CachingProviderHelper.MEMORY_CACHE_ENABLED, true);

        try (RangeReader base = RangeReaderTestSupport.fileReader(testFile);
                CachingRangeReader reader = (CachingRangeReader) CachingProviderHelper.cachingDecoratorFor(opts)
                        .orElseThrow()
                        .apply(base)) {

            ByteBuffer out = ByteBuffer.allocate(16);
            reader.readRange(2000, 1, out);

            assertThat(reader.getEstimatedCacheSizeBytes()).isEqualTo(64L * 1024);
        }
    }

    private Path writeRandomFile() throws IOException {
        Path testFile = tempDir.resolve("test.bin");
        byte[] data = new byte[100 * 1024];
        new Random(42).nextBytes(data);
        Files.write(testFile, data);
        return testFile;
    }
}
