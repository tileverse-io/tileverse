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
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReadableByteChannelDataInputTest {

    @TempDir
    Path tempDir;

    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        testFile = tempDir.resolve("test-data.bin");
    }

    @Test
    void of_withNullChannel_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> ReadableByteChannelDataInput.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("channel cannot be null");
    }

    @Test
    void of_withInvalidBufferSize_throwsIllegalArgumentException() throws IOException {
        Files.write(testFile, "test".getBytes()); // Create the file first
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            assertThatThrownBy(() -> ReadableByteChannelDataInput.of(channel, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("bufferSize must be positive");

            assertThatThrownBy(() -> ReadableByteChannelDataInput.of(channel, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("bufferSize must be positive");
        }
    }

    @Test
    void readPrimitiveTypes() throws IOException {
        // Create test data with various primitive types
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeBoolean(true);
            dos.writeBoolean(false);
            dos.writeByte(42);
            dos.writeByte(-128);
            dos.writeShort(12345);
            dos.writeShort(-32768);
            dos.writeChar('A');
            dos.writeChar('€');
            dos.writeInt(1234567890);
            dos.writeInt(-2147483648);
            dos.writeLong(9223372036854775807L);
            dos.writeLong(-9223372036854775808L);
            dos.writeFloat(3.14159f);
            dos.writeFloat(-Float.MAX_VALUE);
            dos.writeDouble(2.718281828459045);
            dos.writeDouble(-Double.MAX_VALUE);
        }

        // Read back the data using our adapter
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Read booleans
            assertThat(dataInput.readBoolean()).isTrue();
            assertThat(dataInput.readBoolean()).isFalse();

            // Read bytes
            assertThat(dataInput.readByte()).isEqualTo((byte) 42);
            assertThat(dataInput.readByte()).isEqualTo((byte) -128);

            // Reset position for next test
        }

        // Test unsigned byte reading with fresh channel
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Skip to the -128 byte (position 3)
            dataInput.skipBytes(3);
            assertThat(dataInput.readUnsignedByte()).isEqualTo(128); // -128 as unsigned
        }

        // Test again with fresh channel for remaining types
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Skip to shorts
            dataInput.skipBytes(4);

            // Read shorts
            assertThat(dataInput.readShort()).isEqualTo((short) 12345);
            assertThat(dataInput.readShort()).isEqualTo((short) -32768);
        }

        // Test unsigned short reading with fresh channel
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Skip to the -32768 short (position 6)
            dataInput.skipBytes(6);
            assertThat(dataInput.readUnsignedShort()).isEqualTo(32768); // -32768 as unsigned
        }

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Skip to chars (2 booleans + 2 bytes + 2 shorts = 8 bytes)
            dataInput.skipBytes(8);

            // Read chars
            assertThat(dataInput.readChar()).isEqualTo('A');
            assertThat(dataInput.readChar()).isEqualTo('€');

            // Read ints
            assertThat(dataInput.readInt()).isEqualTo(1234567890);
            assertThat(dataInput.readInt()).isEqualTo(-2147483648);

            // Read longs
            assertThat(dataInput.readLong()).isEqualTo(9223372036854775807L);
            assertThat(dataInput.readLong()).isEqualTo(-9223372036854775808L);

            // Read floats
            assertThat(dataInput.readFloat()).isEqualTo(3.14159f);
            assertThat(dataInput.readFloat()).isEqualTo(-Float.MAX_VALUE);

            // Read doubles
            assertThat(dataInput.readDouble()).isEqualTo(2.718281828459045);
            assertThat(dataInput.readDouble()).isEqualTo(-Double.MAX_VALUE);
        }
    }

    @Test
    void readFully_withValidData() throws IOException {
        byte[] testData = "Hello, DataInput World!".getBytes();
        Files.write(testFile, testData);

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Read entire content
            byte[] buffer = new byte[testData.length];
            dataInput.readFully(buffer);
            assertThat(buffer).isEqualTo(testData);

            // Try to read more - should throw EOFException
            byte[] moreBuffer = new byte[1];
            assertThatThrownBy(() -> dataInput.readFully(moreBuffer)).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void readFully_withOffset() throws IOException {
        byte[] testData = "0123456789".getBytes();
        Files.write(testFile, testData);

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            byte[] buffer = new byte[15];
            dataInput.readFully(buffer, 2, 5); // Read 5 bytes starting at offset 2

            // Check that only positions 2-6 are filled
            assertThat(buffer[0]).isEqualTo((byte) 0);
            assertThat(buffer[1]).isEqualTo((byte) 0);
            assertThat(buffer[2]).isEqualTo((byte) '0');
            assertThat(buffer[3]).isEqualTo((byte) '1');
            assertThat(buffer[4]).isEqualTo((byte) '2');
            assertThat(buffer[5]).isEqualTo((byte) '3');
            assertThat(buffer[6]).isEqualTo((byte) '4');
            assertThat(buffer[7]).isEqualTo((byte) 0);
        }
    }

    @Test
    void readFully_withInvalidParameters() throws IOException {
        Files.write(testFile, "test".getBytes());

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            byte[] buffer = new byte[10];

            // Null array
            assertThatThrownBy(() -> dataInput.readFully(null)).isInstanceOf(NullPointerException.class);

            // Invalid offset/length combinations
            assertThatThrownBy(() -> dataInput.readFully(buffer, -1, 5)).isInstanceOf(IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> dataInput.readFully(buffer, 0, -1)).isInstanceOf(IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> dataInput.readFully(buffer, 5, 10)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void skipBytes_validSkipping() throws IOException {
        byte[] testData = "0123456789ABCDEF".getBytes();
        Files.write(testFile, testData);

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Skip first 5 bytes
            int skipped = dataInput.skipBytes(5);
            assertThat(skipped).isEqualTo(5);

            // Read next byte should be '5'
            assertThat(dataInput.readByte()).isEqualTo((byte) '5');

            // Skip more than available
            int remainingSkipped = dataInput.skipBytes(100);
            assertThat(remainingSkipped).isEqualTo(10); // 16 total - 5 skipped - 1 read = 10 remaining

            // Try to read - should throw EOFException
            assertThatThrownBy(() -> dataInput.readByte()).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void skipBytes_withZeroOrNegative() throws IOException {
        Files.write(testFile, "test".getBytes());

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            assertThat(dataInput.skipBytes(0)).isEqualTo(0);
            assertThat(dataInput.skipBytes(-5)).isEqualTo(0);

            // Should still be able to read normally
            assertThat(dataInput.readByte()).isEqualTo((byte) 't');
        }
    }

    @Test
    void readUTF_validStrings() throws IOException {
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeUTF("Hello, World!");
            dos.writeUTF(""); // Empty string
            dos.writeUTF("UTF-8: äöü€🚀");
        }

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            assertThat(dataInput.readUTF()).isEqualTo("Hello, World!");
            assertThat(dataInput.readUTF()).isEqualTo("");
            assertThat(dataInput.readUTF()).isEqualTo("UTF-8: äöü€🚀");
        }
    }

    @Test
    void readLine_variousFormats() throws IOException {
        String testContent = "Line 1\nLine 2\r\nLine 3\rLine 4";
        Files.write(testFile, testContent.getBytes());

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

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

        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            // Read the available bytes
            assertThat(dataInput.readByte()).isEqualTo((byte) 1);
            assertThat(dataInput.readByte()).isEqualTo((byte) 2);

            // Now trying to read more should throw EOFException
            assertThatThrownBy(() -> dataInput.readByte()).isInstanceOf(EOFException.class);

            assertThatThrownBy(() -> dataInput.readInt()).isInstanceOf(EOFException.class);

            assertThatThrownBy(() -> dataInput.readBoolean()).isInstanceOf(EOFException.class);
        }
    }

    @Test
    void customBufferSize() throws IOException {
        byte[] largeData = new byte[10000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        Files.write(testFile, largeData);

        // Test with small buffer size
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel, 64);

            byte[] readData = new byte[largeData.length];
            dataInput.readFully(readData);

            assertThat(readData).isEqualTo(largeData);
        }
    }

    @Test
    void getChannel_returnsUnderlyingChannel() throws IOException {
        Files.write(testFile, "test".getBytes()); // Create the file first
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            ReadableByteChannelDataInput dataInput = ReadableByteChannelDataInput.of(channel);

            assertThat(dataInput.channel()).isSameAs(channel);
        }
    }

    @Test
    void toString_includesChannelInfo() throws IOException {
        Files.write(testFile, "test".getBytes()); // Create the file first
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            ReadableByteChannelDataInput dataInput = ReadableByteChannelDataInput.of(channel, 1024);

            String result = dataInput.toString();
            assertThat(result)
                    .contains("ReadableByteChannelDataInput")
                    .contains("bufferSize=1024")
                    .contains("bufferRemaining=0");
        }
    }

    @Test
    void integrationTest_mixedDataTypes() throws IOException {
        // Write mixed data types
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(testFile))) {
            dos.writeInt(42);
            dos.writeUTF("test string");
            dos.writeDouble(3.14159);
            dos.writeBoolean(true);
            dos.writeLong(1234567890L);
            dos.writeShort(999);
        }

        // Read back using our adapter
        try (ReadableByteChannel channel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
            DataInput dataInput = ReadableByteChannelDataInput.of(channel);

            assertThat(dataInput.readInt()).isEqualTo(42);
            assertThat(dataInput.readUTF()).isEqualTo("test string");
            assertThat(dataInput.readDouble()).isEqualTo(3.14159);
            assertThat(dataInput.readBoolean()).isTrue();
            assertThat(dataInput.readLong()).isEqualTo(1234567890L);
            assertThat(dataInput.readShort()).isEqualTo((short) 999);
        }
    }
}
