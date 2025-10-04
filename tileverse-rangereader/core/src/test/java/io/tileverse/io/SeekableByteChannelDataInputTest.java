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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SeekableByteChannelDataInputTest {

    @TempDir
    Path tempDir;

    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-data.bin");
    }

    @Test
    void of_withNullChannel_throwsNullPointerException() {
        assertThatThrownBy(() -> SeekableByteChannelDataInput.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("channel cannot be null");
    }

    @Test
    void of_withInvalidBufferSize_throwsIllegalArgumentException() throws IOException {
        Files.write(testFile, "test".getBytes());
        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> SeekableByteChannelDataInput.of(channel, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("bufferSize must be positive");

            assertThatThrownBy(() -> SeekableByteChannelDataInput.of(channel, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("bufferSize must be positive");
        }
    }

    @Test
    void skipBytes_withSeekingOptimization() throws IOException {
        // Create a large test file for meaningful skip testing
        byte[] largeData = new byte[10000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        Files.write(testFile, largeData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel);

            // Initial position should be 0
            assertThat(dataInput.position()).isEqualTo(0);

            // Skip a large amount efficiently
            int skipped = dataInput.skipBytes(5000);
            assertThat(skipped).isEqualTo(5000);
            assertThat(dataInput.position()).isEqualTo(5000);

            // Read the byte at position 5000 to verify we're at the right place
            byte expectedByte = (byte) (5000 % 256);
            assertThat(dataInput.readByte()).isEqualTo(expectedByte);
            assertThat(dataInput.position()).isEqualTo(5001);

            // Skip more than available
            int remainingSkipped = dataInput.skipBytes(10000);
            assertThat(remainingSkipped).isEqualTo(4999); // 10000 total - 5001 already read = 4999 remaining
            assertThat(dataInput.position()).isEqualTo(10000);

            // Try to read at end - should throw EOFException
            assertThatThrownBy(() -> dataInput.readByte()).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void skipBytes_withZeroOrNegative() throws IOException {
        Files.write(testFile, "test".getBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel);

            assertThat(dataInput.skipBytes(0)).isEqualTo(0);
            assertThat(dataInput.skipBytes(-5)).isEqualTo(0);
            assertThat(dataInput.position()).isEqualTo(0);

            // Should still be able to read normally
            assertThat(dataInput.readByte()).isEqualTo((byte) 't');
        }
    }

    @Test
    void readPrimitiveTypes_afterSeeking() throws IOException {
        // Create test data with various primitive types
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(42); // bytes 0-3
            dos.writeLong(1234567890L); // bytes 4-11
            dos.writeDouble(3.14159); // bytes 12-19
            dos.writeFloat(2.71f); // bytes 20-23
        }

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel);

            // Read int at beginning
            assertThat(dataInput.readInt()).isEqualTo(42);

            // Skip to double (skip the long)
            dataInput.skipBytes(8);
            assertThat(dataInput.readDouble()).isEqualTo(3.14159);

            // Skip back to long would require repositioning to byte 4
            // Since we can't seek backwards efficiently, let's test forward seeking
            assertThat(dataInput.readFloat()).isEqualTo(2.71f);
        }
    }

    @Test
    void readFully_withValidData() throws IOException {
        byte[] testData = "Hello, SeekableByteChannel World!".getBytes();
        Files.write(testFile, testData);

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = SeekableByteChannelDataInput.of(channel);

            // Skip some bytes first
            dataInput.skipBytes(7); // Skip "Hello, "

            // Read remaining content
            byte[] buffer = new byte[testData.length - 7];
            dataInput.readFully(buffer);
            assertThat(new String(buffer)).isEqualTo("SeekableByteChannel World!");

            // Try to read more - should throw EOFException
            byte[] moreBuffer = new byte[1];
            assertThatThrownBy(() -> dataInput.readFully(moreBuffer)).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void readUTF_validStrings() throws IOException {
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeUTF("Hello, World!");
            dos.writeUTF("UTF-8: äöü€🚀");
            dos.writeUTF("Third string");
        }

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = SeekableByteChannelDataInput.of(channel);

            assertThat(dataInput.readUTF()).isEqualTo("Hello, World!");
            assertThat(dataInput.readUTF()).isEqualTo("UTF-8: äöü€🚀");
            assertThat(dataInput.readUTF()).isEqualTo("Third string");
        }
    }

    @Test
    void readLine_variousFormats() throws IOException {
        String testContent = "Line 1\nLine 2\r\nLine 3\rLine 4";
        Files.write(testFile, testContent.getBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = SeekableByteChannelDataInput.of(channel);

            assertThat(dataInput.readLine()).isEqualTo("Line 1");
            assertThat(dataInput.readLine()).isEqualTo("Line 2");
            assertThat(dataInput.readLine()).isEqualTo("Line 3");
            assertThat(dataInput.readLine()).isEqualTo("Line 4");
            assertThat(dataInput.readLine()).isNull(); // End of file
        }
    }

    @Test
    void eofBehavior() throws IOException {
        Files.write(testFile, new byte[] {1, 2}); // Only 2 bytes

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = SeekableByteChannelDataInput.of(channel);

            // Read the available bytes
            assertThat(dataInput.readByte()).isEqualTo((byte) 1);
            assertThat(dataInput.readByte()).isEqualTo((byte) 2);

            // Now trying to read more should throw EOFException
            assertThatThrownBy(() -> dataInput.readByte()).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void getChannel_returnsUnderlyingChannel() throws IOException {
        Files.write(testFile, "test".getBytes());
        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel);

            assertThat(dataInput.channel()).isSameAs(channel);
        }
    }

    @Test
    void positionAndSize_accessors() throws IOException {
        Files.write(testFile, "Hello World".getBytes());

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel);

            assertThat(dataInput.position()).isEqualTo(0);
            assertThat(dataInput.size()).isEqualTo(11);

            // Read some bytes
            dataInput.readByte();
            dataInput.readByte();
            assertThat(dataInput.position()).isEqualTo(2);

            // Skip some bytes
            dataInput.skipBytes(5);
            assertThat(dataInput.position()).isEqualTo(7);
        }
    }

    @Test
    void toString_includesChannelInfo() throws IOException {
        Files.write(testFile, "test".getBytes());
        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel, 1024);

            String result = dataInput.toString();
            assertThat(result)
                    .contains("SeekableByteChannelDataInput")
                    .contains("position=0")
                    .contains("size=4")
                    .contains("bufferSize=1024")
                    .contains("bufferRemaining=0");
        }
    }

    @Test
    void customBufferSize() throws IOException {
        byte[] largeData = new byte[5000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        Files.write(testFile, largeData);

        // Test with small buffer size
        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = SeekableByteChannelDataInput.of(channel, 64);

            // Skip efficiently and then read
            dataInput.skipBytes(2500);
            byte[] readData = new byte[100];
            dataInput.readFully(readData);

            // Verify we read the correct data from position 2500
            for (int i = 0; i < readData.length; i++) {
                assertThat(readData[i]).isEqualTo((byte) ((2500 + i) % 256));
            }
        }
    }

    @Test
    void integrationTest_mixedDataTypesWithSeeking() throws IOException {
        // Write mixed data types with known positions
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(100); // bytes 0-3
            dos.writeUTF("header"); // bytes 4-11 (2 bytes length + 6 bytes string)
            dos.writeLong(9999L); // bytes 12-19
            dos.writeDouble(1.23); // bytes 20-27
            dos.writeBoolean(true); // byte 28
            dos.writeShort(555); // bytes 29-30
        }

        try (SeekableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            SeekableByteChannelDataInput dataInput = SeekableByteChannelDataInput.of(channel);

            // Read header sequentially
            assertThat(dataInput.readInt()).isEqualTo(100);
            assertThat(dataInput.readUTF()).isEqualTo("header");

            // Skip to boolean (skip long and double)
            dataInput.skipBytes(16); // 8 bytes long + 8 bytes double
            assertThat(dataInput.readBoolean()).isTrue();

            // Read final short
            assertThat(dataInput.readShort()).isEqualTo((short) 555);
        }
    }
}
