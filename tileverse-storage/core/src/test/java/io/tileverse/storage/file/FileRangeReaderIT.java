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
package io.tileverse.storage.file;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeReaderTestSupport;
import io.tileverse.storage.it.AbstractRangeReaderIT;
import io.tileverse.storage.it.TestUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the shared {@link AbstractRangeReaderIT} contract against the local file backend, through a reader obtained from
 * a {@code FileStorage} the way production code obtains one.
 */
class FileRangeReaderIT extends AbstractRangeReaderIT {

    private static Path testFilePath;
    private static byte[] testFileContent;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void createTestFile() throws IOException {
        testFilePath = TestUtil.createMockTestFile(tempDir.resolve("test.bin"), TEST_FILE_SIZE);
        testFileContent = Files.readAllBytes(testFilePath);
    }

    @BeforeEach
    void setUp() {
        super.testFile = testFilePath;
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        return RangeReaderTestSupport.fileReader(testFilePath);
    }

    @Test
    void missingFileIsReportedWhenTheReaderIsOpened() {
        Path missing = tempDir.resolve("missing.bin");

        assertThatThrownBy(() -> RangeReaderTestSupport.fileReader(missing))
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("missing.bin");
    }

    @Test
    void concurrentReadsLandTheRightBytes() throws Exception {
        int threads = 10;
        int length = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try (RangeReader reader = createBaseReader()) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<ByteBuffer>> reads = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int offset = t * length;
                reads.add(pool.submit(() -> {
                    start.await();
                    return reader.readRange(offset, length).flip();
                }));
            }
            start.countDown();
            for (int t = 0; t < threads; t++) {
                int offset = t * length;
                ByteBuffer read = reads.get(t).get(30, TimeUnit.SECONDS);
                byte[] expected = Arrays.copyOfRange(testFileContent, offset, offset + length);
                assertThat(read).as("bytes read at offset " + offset).isEqualTo(ByteBuffer.wrap(expected));
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
