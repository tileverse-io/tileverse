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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

/**
 * A {@link DataInput} adapter that wraps a {@link ReadableByteChannel} to provide
 * standard Java DataInput semantics for reading primitive data types.
 * <p>
 * This adapter allows ReadableByteChannel instances to be used with any API that expects
 * a DataInput interface, enabling integration with serialization frameworks, data
 * processing libraries, and legacy code that works with DataInput.
 * <p>
 * <strong>Thread Safety:</strong> This class is not thread-safe. External synchronization
 * is required if multiple threads access the same instance concurrently.
 * <p>
 * <strong>Read-Only Adapter:</strong> This adapter only provides reading functionality.
 * It maintains an internal buffer for efficient reading of primitive types.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * try (ReadableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
 *     DataInput dataInput = ReadableByteChannelDataInput.of(channel);
 *
 *     // Read primitive types
 *     int value = dataInput.readInt();
 *     double price = dataInput.readDouble();
 *     String text = dataInput.readUTF();
 *
 *     // Read arrays
 *     byte[] data = new byte[100];
 *     dataInput.readFully(data);
 * }
 * }</pre>
 */
public class ReadableByteChannelDataInput implements DataInput {

    /** The underlying ReadableByteChannel that provides the data source. */
    protected final ReadableByteChannel channel;

    /** Internal buffer for efficient reading of primitive types. */
    protected final ByteBuffer buffer;

    /** Default buffer size for internal operations. */
    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel.
     *
     * @param channel the ReadableByteChannel to wrap
     * @throws IllegalArgumentException if channel is null
     */
    protected ReadableByteChannelDataInput(ReadableByteChannel channel) {
        this(channel, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel with specified buffer size.
     *
     * @param channel the ReadableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    protected ReadableByteChannelDataInput(ReadableByteChannel channel, int bufferSize) {
        this.channel = Objects.requireNonNull(channel, "channel cannot be null");
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive: " + bufferSize);
        }
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.buffer.limit(0); // Start with empty buffer
    }

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel.
     *
     * @param channel the ReadableByteChannel to wrap
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null
     */
    public static ReadableByteChannelDataInput of(ReadableByteChannel channel) {
        return new ReadableByteChannelDataInput(channel);
    }

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel with specified buffer size.
     *
     * @param channel the ReadableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    public static ReadableByteChannelDataInput of(ReadableByteChannel channel, int bufferSize) {
        return new ReadableByteChannelDataInput(channel, bufferSize);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        Objects.requireNonNull(b, "byte array cannot be null");
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException("off=" + off + ", len=" + len + ", array.length=" + b.length);
        }

        int totalRead = 0;
        while (totalRead < len) {
            int bytesRead = read(b, off + totalRead, len - totalRead);
            if (bytesRead < 0) {
                throw new EOFException("Unexpected end of stream after reading " + totalRead + " of " + len + " bytes");
            }
            totalRead += bytesRead;
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        int totalSkipped = 0;
        byte[] skipBuffer = new byte[Math.min(n, 8192)];

        while (totalSkipped < n) {
            int toSkip = Math.min(n - totalSkipped, skipBuffer.length);
            int bytesRead = read(skipBuffer, 0, toSkip);
            if (bytesRead < 0) {
                break; // End of stream
            }
            totalSkipped += bytesRead;
        }

        return totalSkipped;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        ensureAvailable(1);
        return buffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        ensureAvailable(2);
        return buffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException {
        ensureAvailable(2);
        return buffer.getChar();
    }

    @Override
    public int readInt() throws IOException {
        ensureAvailable(4);
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
        ensureAvailable(8);
        return buffer.getLong();
    }

    @Override
    public float readFloat() throws IOException {
        ensureAvailable(4);
        return buffer.getFloat();
    }

    @Override
    public double readDouble() throws IOException {
        ensureAvailable(8);
        return buffer.getDouble();
    }

    @Override
    public String readLine() throws IOException {
        StringBuilder line = new StringBuilder();
        int c;

        try {
            while ((c = readUnsignedByte()) != -1) {
                if (c == '\n') {
                    break;
                }
                if (c == '\r') {
                    // Peek ahead for \n
                    mark();
                    try {
                        int next = readUnsignedByte();
                        if (next != '\n' && next != -1) {
                            reset();
                        }
                    } catch (EOFException e) {
                        // End of stream after \r, that's fine
                    }
                    break;
                }
                line.append((char) c);
            }
        } catch (EOFException e) {
            // End of stream reached
            c = -1;
        }

        return line.length() > 0 || c != -1 ? line.toString() : null;
    }

    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] bytearr = new byte[utflen];
        readFully(bytearr);

        // Use DataInputStream's modified UTF-8 decoding for compatibility
        return readModifiedUTF8(bytearr);
    }

    /**
     * Reads a modified UTF-8 string as used by DataInputStream/DataOutputStream.
     * This handles the special encoding used by Java's writeUTF/readUTF methods.
     *
     * @param bytearr The byte array containing the modified UTF-8 data.
     * @return The decoded String.
     * @throws IOException If the byte array contains malformed modified UTF-8 data.
     */
    protected String readModifiedUTF8(byte[] bytearr) throws IOException {
        int utflen = bytearr.length;
        char[] chararr = new char[utflen];
        int c, char2, char3;
        int count = 0;
        int chararr_count = 0;

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) break;
            count++;
            chararr[chararr_count++] = (char) c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen) throw new IOException("malformed input: partial character at end");
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) throw new IOException("malformed input around byte " + count);
                    chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen) throw new IOException("malformed input: partial character at end");
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new IOException("malformed input around byte " + (count - 1));
                    chararr[chararr_count++] =
                            (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new IOException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }

    /**
     * Reads data into the provided byte array.
     *
     * @param b the byte array to read into
     * @param off the offset in the array
     * @param len the number of bytes to read
     * @return the number of bytes read, or -1 if end of stream
     * @throws IOException if an I/O error occurs
     */
    protected int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        // Try to read from buffer first
        int fromBuffer = Math.min(buffer.remaining(), len);
        if (fromBuffer > 0) {
            buffer.get(b, off, fromBuffer);
            return fromBuffer;
        }

        // Buffer is empty, fill it from channel
        if (!fillBuffer()) {
            return -1; // End of stream
        }

        // Read from newly filled buffer
        int available = Math.min(buffer.remaining(), len);
        buffer.get(b, off, available);
        return available;
    }

    /**
     * Ensures that at least the specified number of bytes are available in the buffer.
     *
     * @param bytes the number of bytes required
     * @throws EOFException if not enough bytes are available
     * @throws IOException if an I/O error occurs
     */
    protected void ensureAvailable(int bytes) throws IOException {
        while (buffer.remaining() < bytes) {
            if (!fillBuffer()) {
                throw new EOFException(
                        "Unexpected end of stream, needed " + bytes + " bytes, got " + buffer.remaining());
            }
        }
    }

    /**
     * Fills the internal buffer from the channel.
     *
     * @return true if data was read, false if end of stream
     * @throws IOException if an I/O error occurs
     */
    protected boolean fillBuffer() throws IOException {
        // If buffer has remaining data, compact it to preserve unread bytes
        if (buffer.hasRemaining()) {
            buffer.compact();
        } else {
            buffer.clear();
        }

        int bytesRead = channel.read(buffer);
        buffer.flip();

        return bytesRead > 0;
    }

    /**
     * Marks the current position for potential reset (simplified implementation).
     */
    protected void mark() {
        buffer.mark();
    }

    /**
     * Resets to the marked position (simplified implementation).
     */
    protected void reset() {
        buffer.reset();
    }

    /**
     * Gets the underlying ReadableByteChannel.
     *
     * @return the underlying channel
     */
    public ReadableByteChannel channel() {
        return channel;
    }

    @Override
    public String toString() {
        return String.format(
                "ReadableByteChannelDataInput[channel=%s, bufferSize=%d, bufferRemaining=%d]",
                channel, buffer.capacity(), buffer.remaining());
    }
}
