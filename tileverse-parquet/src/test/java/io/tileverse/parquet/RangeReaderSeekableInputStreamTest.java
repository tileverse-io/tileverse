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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeReaderSeekableInputStreamTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private byte[] testData;
    private RangeReader rangeReader;
    private static final int TEST_FILE_SIZE = 4096;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-data.bin");
        Random random = new Random(42);
        testData = new byte[TEST_FILE_SIZE];
        random.nextBytes(testData);
        Files.write(testFile, testData);
        rangeReader = FileRangeReader.of(testFile);
    }

    private SeekableInputStream newStream() {
        return new RangeReaderSeekableInputStream(rangeReader);
    }

    @Test
    void initialPosition_isZero() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            assertThat(stream.getPos()).isEqualTo(0);
        }
    }

    @Test
    void seek_updatesPosition() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(100);
            assertThat(stream.getPos()).isEqualTo(100);
            stream.seek(0);
            assertThat(stream.getPos()).isEqualTo(0);
            stream.seek(TEST_FILE_SIZE - 1);
            assertThat(stream.getPos()).isEqualTo(TEST_FILE_SIZE - 1);
        }
    }

    @Test
    void read_singleByte() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            int b = stream.read();
            assertThat(b).isEqualTo(testData[0] & 0xFF);
            assertThat(stream.getPos()).isEqualTo(1);
        }
    }

    @Test
    void read_singleByte_afterSeek() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(500);
            int b = stream.read();
            assertThat(b).isEqualTo(testData[500] & 0xFF);
            assertThat(stream.getPos()).isEqualTo(501);
        }
    }

    @Test
    void read_singleByte_atEOF() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(TEST_FILE_SIZE);
            assertThat(stream.read()).isEqualTo(-1);
        }
    }

    @Test
    void read_byteArray() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            byte[] buf = new byte[256];
            int bytesRead = stream.read(buf, 0, buf.length);
            assertThat(bytesRead).isEqualTo(256);
            assertThat(buf).isEqualTo(sliceTestData(0, 256));
            assertThat(stream.getPos()).isEqualTo(256);
        }
    }

    @Test
    void read_byteArray_withOffset() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            byte[] buf = new byte[100];
            stream.seek(200);
            int bytesRead = stream.read(buf, 10, 50);
            assertThat(bytesRead).isEqualTo(50);
            byte[] expected = new byte[100];
            System.arraycopy(testData, 200, expected, 10, 50);
            assertThat(buf).isEqualTo(expected);
        }
    }

    @Test
    void read_byteArray_zeroLength() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            byte[] buf = new byte[10];
            int bytesRead = stream.read(buf, 0, 0);
            assertThat(bytesRead).isEqualTo(0);
            assertThat(stream.getPos()).isEqualTo(0);
        }
    }

    @Test
    void readFully_byteArray() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            byte[] buf = new byte[512];
            stream.readFully(buf);
            assertThat(buf).isEqualTo(sliceTestData(0, 512));
            assertThat(stream.getPos()).isEqualTo(512);
        }
    }

    @Test
    void readFully_byteArray_partial() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(1000);
            byte[] buf = new byte[200];
            stream.readFully(buf, 50, 100);
            byte[] expected = new byte[200];
            System.arraycopy(testData, 1000, expected, 50, 100);
            assertThat(buf).isEqualTo(expected);
            assertThat(stream.getPos()).isEqualTo(1100);
        }
    }

    @Test
    void readFully_byteArray_throwsEOF() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(TEST_FILE_SIZE - 10);
            byte[] buf = new byte[20];
            assertThatThrownBy(() -> stream.readFully(buf))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("bytes left to read");
        }
    }

    @Test
    void read_heapByteBuffer() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            ByteBuffer buf = ByteBuffer.allocate(256);
            int bytesRead = stream.read(buf);
            assertThat(bytesRead).isEqualTo(256);
            buf.flip();
            byte[] result = new byte[256];
            buf.get(result);
            assertThat(result).isEqualTo(sliceTestData(0, 256));
        }
    }

    @Test
    void read_directByteBuffer() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            ByteBuffer buf = ByteBuffer.allocateDirect(256);
            int bytesRead = stream.read(buf);
            assertThat(bytesRead).isEqualTo(256);
            buf.flip();
            byte[] result = new byte[256];
            buf.get(result);
            assertThat(result).isEqualTo(sliceTestData(0, 256));
        }
    }

    @Test
    void readFully_heapByteBuffer() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(1024);
            ByteBuffer buf = ByteBuffer.allocate(512);
            stream.readFully(buf);
            buf.flip();
            byte[] result = new byte[512];
            buf.get(result);
            assertThat(result).isEqualTo(sliceTestData(1024, 512));
        }
    }

    @Test
    void readFully_directByteBuffer() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(2048);
            ByteBuffer buf = ByteBuffer.allocateDirect(512);
            stream.readFully(buf);
            buf.flip();
            byte[] result = new byte[512];
            buf.get(result);
            assertThat(result).isEqualTo(sliceTestData(2048, 512));
        }
    }

    @Test
    void readFully_byteBuffer_throwsEOF() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.seek(TEST_FILE_SIZE - 10);
            ByteBuffer buf = ByteBuffer.allocate(20);
            assertThatThrownBy(() -> stream.readFully(buf))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("bytes left to read");
        }
    }

    @Test
    void sequentialReads_advancePosition() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            byte[] buf1 = new byte[100];
            byte[] buf2 = new byte[100];
            stream.readFully(buf1);
            stream.readFully(buf2);
            assertThat(stream.getPos()).isEqualTo(200);
            assertThat(buf1).isEqualTo(sliceTestData(0, 100));
            assertThat(buf2).isEqualTo(sliceTestData(100, 100));
        }
    }

    @Test
    void seekThenRead_readsFromNewPosition() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            stream.readFully(new byte[100]);
            stream.seek(500);
            byte[] buf = new byte[50];
            stream.readFully(buf);
            assertThat(buf).isEqualTo(sliceTestData(500, 50));
            assertThat(stream.getPos()).isEqualTo(550);
        }
    }

    @Test
    void readEntireFile() throws IOException {
        try (SeekableInputStream stream = newStream()) {
            byte[] buf = new byte[TEST_FILE_SIZE];
            stream.readFully(buf);
            assertThat(buf).isEqualTo(testData);
        }
    }

    private byte[] sliceTestData(int offset, int length) {
        byte[] slice = new byte[length];
        System.arraycopy(testData, offset, slice, 0, length);
        return slice;
    }
}
