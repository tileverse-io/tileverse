/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
 * Verifies that {@link CachingProviderHelper#cachingDecoratorFor(StorageConfig)} applies caching based on the
 * {@code storage.caching.enabled} configuration. Lives in {@code io.tileverse.storage.cache} to read the
 * package-private {@link CachingRangeReader#getEstimatedCacheSizeBytes()}.
 */
class CachingProviderHelperTest {

    @TempDir
    Path tempDir;

    @Test
    void enabledCachesExactRanges() throws IOException {
        Path testFile = writeRandomFile();
        StorageConfig opts =
                new StorageConfig(testFile.toUri()).setParameter(CachingProviderHelper.MEMORY_CACHE_ENABLED, true);

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
    void disabledYieldsNoDecorator() throws IOException {
        Path testFile = writeRandomFile();
        StorageConfig opts =
                new StorageConfig(testFile.toUri()).setParameter(CachingProviderHelper.MEMORY_CACHE_ENABLED, false);
        assertThat(CachingProviderHelper.cachingDecoratorFor(opts)).isEmpty();
    }

    @Test
    void onlyTheEnabledParameterIsDeclared() {
        assertThat(CachingProviderHelper.configParameters())
                .containsExactly(CachingProviderHelper.MEMORY_CACHE_ENABLED);
    }

    private Path writeRandomFile() throws IOException {
        Path testFile = tempDir.resolve("test.bin");
        byte[] data = new byte[100 * 1024];
        new Random(42).nextBytes(data);
        Files.write(testFile, data);
        return testFile;
    }
}
