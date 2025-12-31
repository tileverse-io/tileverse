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
package io.tileverse.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.file.FileRangeReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.Random;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeReaderInputFileTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private byte[] testData;
    private static final int TEST_FILE_SIZE = 4096;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-data.bin");
        Random random = new Random(42);
        testData = new byte[TEST_FILE_SIZE];
        random.nextBytes(testData);
        Files.write(testFile, testData);
    }

    @Test
    void getLength_returnsFileSize() throws IOException {
        try (RangeReader reader = FileRangeReader.of(testFile)) {
            RangeReaderInputFile inputFile = new RangeReaderInputFile(reader);
            assertThat(inputFile.getLength()).isEqualTo(TEST_FILE_SIZE);
        }
    }

    @Test
    void getLength_cachesResult() throws IOException {
        try (RangeReader reader = FileRangeReader.of(testFile)) {
            RangeReaderInputFile inputFile = new RangeReaderInputFile(reader);
            long first = inputFile.getLength();
            long second = inputFile.getLength();
            assertThat(first).isEqualTo(second).isEqualTo(TEST_FILE_SIZE);
        }
    }

    @Test
    void getLength_throwsWhenSizeUnknown() {
        RangeReader unknownSizeReader = new RangeReader() {
            @Override
            public int readRange(long offset, int length, java.nio.ByteBuffer target) {
                return 0;
            }

            @Override
            public OptionalLong size() {
                return OptionalLong.empty();
            }

            @Override
            public String getSourceIdentifier() {
                return "unknown-size-source";
            }

            @Override
            public void close() {}
        };

        RangeReaderInputFile inputFile = new RangeReaderInputFile(unknownSizeReader);
        assertThatThrownBy(inputFile::getLength)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot determine size")
                .hasMessageContaining("unknown-size-source");
    }

    @Test
    void newStream_returnsWorkingStream() throws IOException {
        try (RangeReader reader = FileRangeReader.of(testFile)) {
            RangeReaderInputFile inputFile = new RangeReaderInputFile(reader);
            try (SeekableInputStream stream = inputFile.newStream()) {
                assertThat(stream).isNotNull();
                assertThat(stream.getPos()).isEqualTo(0);
            }
        }
    }

    @Test
    void newStream_createsIndependentStreams() throws IOException {
        try (RangeReader reader = FileRangeReader.of(testFile)) {
            RangeReaderInputFile inputFile = new RangeReaderInputFile(reader);
            try (SeekableInputStream stream1 = inputFile.newStream();
                    SeekableInputStream stream2 = inputFile.newStream()) {
                stream1.seek(100);
                assertThat(stream1.getPos()).isEqualTo(100);
                assertThat(stream2.getPos()).isEqualTo(0);
            }
        }
    }

    @Test
    void toString_includesSourceIdentifier() throws IOException {
        try (RangeReader reader = FileRangeReader.of(testFile)) {
            RangeReaderInputFile inputFile = new RangeReaderInputFile(reader);
            assertThat(inputFile.toString()).contains("RangeReaderInputFile");
        }
    }

    @Test
    void constructor_throwsOnNull() {
        assertThatThrownBy(() -> new RangeReaderInputFile(null)).isInstanceOf(NullPointerException.class);
    }
}
