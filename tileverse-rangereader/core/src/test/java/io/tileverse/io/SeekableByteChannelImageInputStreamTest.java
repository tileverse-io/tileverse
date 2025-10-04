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
package io.tileverse.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.imageio.stream.ImageInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SeekableByteChannelImageInputStreamTest {

    @TempDir
    Path tempDir;

    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-image-data.bin");
    }

    @Test
    void of_withNullChannel_throwsNullPointerException() {
        assertThatThrownBy(() -> SeekableByteChannelImageInputStream.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("channel cannot be null");
    }

    @Test
    void readSingleByte() throws IOException {
        Files.write(testFile, new byte[] {42, -128, 0, 127});

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            assertThat(imageStream.read()).isEqualTo(42);
            assertThat(imageStream.read()).isEqualTo(128); // -128 as unsigned
            assertThat(imageStream.read()).isEqualTo(0);
            assertThat(imageStream.read()).isEqualTo(127);
            assertThat(imageStream.read()).isEqualTo(-1); // EOF
        }
    }

    @Test
    void readByteArray() throws IOException {
        byte[] testData = "Hello, ImageInputStream World!".getBytes();
        Files.write(testFile, testData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Read entire content
            byte[] buffer = new byte[testData.length];
            int bytesRead = imageStream.read(buffer);
            assertThat(bytesRead).isEqualTo(testData.length);
            assertThat(buffer).isEqualTo(testData);

            // Try to read more - should return -1
            assertThat(imageStream.read(buffer)).isEqualTo(-1);
        }
    }

    @Test
    void readByteArrayWithOffsetAndLength() throws IOException {
        byte[] testData = "0123456789ABCDEF".getBytes();
        Files.write(testFile, testData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            byte[] buffer = new byte[20];
            int bytesRead = imageStream.read(buffer, 2, 8); // Read 8 bytes starting at offset 2
            assertThat(bytesRead).isEqualTo(8);

            // Check that only positions 2-9 are filled
            assertThat(buffer[0]).isEqualTo((byte) 0);
            assertThat(buffer[1]).isEqualTo((byte) 0);
            assertThat(buffer[2]).isEqualTo((byte) '0');
            assertThat(buffer[3]).isEqualTo((byte) '1');
            assertThat(buffer[9]).isEqualTo((byte) '7');
            assertThat(buffer[10]).isEqualTo((byte) 0);
        }
    }

    @Test
    void readByteArrayWithInvalidParameters() throws IOException {
        Files.write(testFile, "test".getBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            byte[] buffer = new byte[10];

            // Null array
            assertThatThrownBy(() -> imageStream.read(null, 0, 5)).isInstanceOf(NullPointerException.class);

            // Invalid offset/length combinations
            assertThatThrownBy(() -> imageStream.read(buffer, -1, 5)).isInstanceOf(IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> imageStream.read(buffer, 0, -1)).isInstanceOf(IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> imageStream.read(buffer, 5, 10)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void seeking() throws IOException {
        byte[] testData = new byte[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Files.write(testFile, testData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Initial position should be 0
            assertThat(imageStream.getStreamPosition()).isEqualTo(0);

            // Seek to middle
            imageStream.seek(500);
            assertThat(imageStream.getStreamPosition()).isEqualTo(500);
            assertThat(imageStream.read()).isEqualTo(500 % 256);
            assertThat(imageStream.getStreamPosition()).isEqualTo(501);

            // Seek back to beginning
            imageStream.seek(0);
            assertThat(imageStream.getStreamPosition()).isEqualTo(0);
            assertThat(imageStream.read()).isEqualTo(0);

            // Seek to end
            imageStream.seek(1000);
            assertThat(imageStream.getStreamPosition()).isEqualTo(1000);
            assertThat(imageStream.read()).isEqualTo(-1); // EOF
        }
    }

    @Test
    void seekWithNegativePosition() throws IOException {
        Files.write(testFile, "test".getBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            assertThatThrownBy(() -> imageStream.seek(-1))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("seek position cannot be negative");
        }
    }

    @Test
    void streamLength() throws IOException {
        byte[] testData = "Hello, World!".getBytes();
        Files.write(testFile, testData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            assertThat(imageStream.length()).isEqualTo(testData.length);
        }
    }

    @Test
    void readPrimitiveTypesBigEndian() throws IOException {
        // Create test data with various primitive types in big-endian format
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeBoolean(true);
            dos.writeByte(42);
            dos.writeShort(12345);
            dos.writeChar('A');
            dos.writeInt(1234567890);
            dos.writeLong(9223372036854775807L);
            dos.writeFloat(3.14159f);
            dos.writeDouble(2.718281828459045);
        }

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Default byte order should be big-endian
            assertThat(imageStream.getByteOrder()).isEqualTo(ByteOrder.BIG_ENDIAN);

            // Read primitive types
            assertThat(imageStream.readBoolean()).isTrue();
            assertThat(imageStream.readByte()).isEqualTo((byte) 42);
            assertThat(imageStream.readShort()).isEqualTo((short) 12345);
            assertThat(imageStream.readChar()).isEqualTo('A');
            assertThat(imageStream.readInt()).isEqualTo(1234567890);
            assertThat(imageStream.readLong()).isEqualTo(9223372036854775807L);
            assertThat(imageStream.readFloat()).isEqualTo(3.14159f);
            assertThat(imageStream.readDouble()).isEqualTo(2.718281828459045);
        }
    }

    @Test
    void readPrimitiveTypesLittleEndian() throws IOException {
        // Create test data in little-endian format manually
        byte[] littleEndianData = {
            // int 1234567890 in little-endian
            (byte) 0xD2,
            (byte) 0x02,
            (byte) 0x96,
            (byte) 0x49,
            // short 12345 in little-endian
            (byte) 0x39,
            (byte) 0x30,
            // char 'A' (65) in little-endian
            (byte) 0x41,
            (byte) 0x00
        };
        Files.write(testFile, littleEndianData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Set little-endian byte order
            imageStream.setByteOrder(ByteOrder.LITTLE_ENDIAN);
            assertThat(imageStream.getByteOrder()).isEqualTo(ByteOrder.LITTLE_ENDIAN);

            // Read primitive types - should be correctly interpreted as little-endian
            assertThat(imageStream.readInt()).isEqualTo(1234567890);
            assertThat(imageStream.readShort()).isEqualTo((short) 12345);
            assertThat(imageStream.readChar()).isEqualTo('A');
        }
    }

    @Test
    void readUnsignedTypes() throws IOException {
        // Create test data with values that test unsigned interpretation
        Files.write(testFile, new byte[] {
            (byte) 0xFF, // 255 as unsigned byte
            (byte) 0xFF,
            (byte) 0xFF, // 65535 as unsigned short
        });

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            assertThat(imageStream.readUnsignedByte()).isEqualTo(255);
            assertThat(imageStream.readUnsignedShort()).isEqualTo(65535);
        }
    }

    @Test
    void largeDirectRead() throws IOException {
        // Create large test data
        byte[] largeData = new byte[50000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        Files.write(testFile, largeData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Read large chunk
            byte[] readData = new byte[30000];
            int bytesRead = imageStream.read(readData);
            assertThat(bytesRead).isEqualTo(30000);

            // Verify data integrity
            for (int i = 0; i < 30000; i++) {
                assertThat(readData[i]).isEqualTo((byte) (i % 256));
            }
        }
    }

    @Test
    void seekAndReadLargeData() throws IOException {
        byte[] testData = new byte[5000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        Files.write(testFile, testData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Seek and read
            imageStream.seek(2500);
            byte[] readData = new byte[1000];
            int bytesRead = imageStream.read(readData);
            assertThat(bytesRead).isEqualTo(1000);

            // Verify data integrity
            for (int i = 0; i < readData.length; i++) {
                assertThat(readData[i]).isEqualTo((byte) ((2500 + i) % 256));
            }
        }
    }

    @Test
    void toString_includesChannelInfo() throws IOException {
        Files.write(testFile, "test data".getBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            String result = imageStream.toString();
            assertThat(result)
                    .contains("SeekableByteChannelImageInputStream")
                    .contains("position=0")
                    .contains("size=9")
                    .contains("byteOrder=BIG_ENDIAN");
        }
    }

    @Test
    void integrationTest_seekAndReadMixed() throws IOException {
        // Create structured test data
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(0x12345678); // bytes 0-3
            dos.writeShort(0x9ABC); // bytes 4-5
            dos.writeLong(0x123456789ABCDEF0L); // bytes 6-13
            dos.writeFloat(3.14159f); // bytes 14-17
            dos.writeDouble(2.71828); // bytes 18-25
        }

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Read header sequentially
            assertThat(imageStream.readInt()).isEqualTo(0x12345678);
            assertThat(imageStream.getStreamPosition()).isEqualTo(4);

            // Skip to long
            imageStream.seek(6);
            assertThat(imageStream.readLong()).isEqualTo(0x123456789ABCDEF0L);

            // Skip to double
            imageStream.seek(18);
            assertThat(imageStream.readDouble()).isEqualTo(2.71828);

            // Go back to read short
            imageStream.seek(4);
            assertThat(imageStream.readShort()).isEqualTo((short) 0x9ABC);
        }
    }

    @Test
    void eofBehavior() throws IOException {
        Files.write(testFile, new byte[] {1, 2, 3});

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ);
                ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel)) {

            // Read available bytes
            assertThat(imageStream.read()).isEqualTo(1);
            assertThat(imageStream.read()).isEqualTo(2);
            assertThat(imageStream.read()).isEqualTo(3);

            // EOF
            assertThat(imageStream.read()).isEqualTo(-1);
            assertThat(imageStream.read()).isEqualTo(-1); // Multiple EOF calls

            // Array read at EOF
            byte[] buffer = new byte[5];
            assertThat(imageStream.read(buffer)).isEqualTo(-1);
        }
    }
}
