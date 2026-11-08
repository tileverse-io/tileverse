/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static io.tileverse.vectortile.mvt.WireReader.FIXED32;
import static io.tileverse.vectortile.mvt.WireReader.FIXED64;
import static io.tileverse.vectortile.mvt.WireReader.LENGTH_DELIMITED;
import static io.tileverse.vectortile.mvt.WireReader.VARINT;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * Writes the protobuf wire format into a growing byte array, one field at a time. The builders write the fields of a
 * tile with it, without building protobuf objects.
 *
 * <p>The write methods write a field as protobuf writes a field of the same type and value. Protobuf writes the fields
 * of a message in the order of their field numbers, and the caller writes them in this order. Protobuf also writes the
 * length of an embedded message before its bytes. The caller writes the message apart first, or computes its length
 * with {@link #varintFieldLength} and {@link #packedVarintsFieldLength}, then writes it in place after
 * {@link #writeFieldHeader}.
 *
 * @see WireReader
 */
final class WireWriter {

    private static final int INITIAL_CAPACITY = 256;

    /** The length of the longest varint: 64 bits in groups of 7. */
    private static final int MAX_VARINT_BYTES = 10;

    private byte[] bytes = new byte[INITIAL_CAPACITY];
    private int size;

    /** Writes a varint field holding the value as an unsigned 32-bit value, as protobuf writes a uint32. */
    void writeVarint32(int fieldNumber, int value) {
        writeVarint64(fieldNumber, Integer.toUnsignedLong(value));
    }

    /** Writes a varint field holding a 64-bit value, as protobuf writes a uint64. */
    void writeVarint64(int fieldNumber, long value) {
        writeTag(fieldNumber, VARINT);
        writeVarint(value);
    }

    private void writeTag(int fieldNumber, int wireType) {
        writeVarint(tag(fieldNumber, wireType));
    }

    // Protobuf writes a tag as an unsigned 32-bit value
    private static long tag(int fieldNumber, int wireType) {
        return Integer.toUnsignedLong(fieldNumber << 3 | wireType);
    }

    /** Writes the value 7 bits at a time, low bits first. Each byte but the last has its high bit set. */
    private void writeVarint(long value) {
        requireCapacity(MAX_VARINT_BYTES);
        long rest = value;
        while ((rest & ~0x7FL) != 0) {
            bytes[size++] = (byte) (rest & 0x7F | 0x80);
            rest >>>= 7;
        }
        bytes[size++] = (byte) rest;
    }

    private void requireCapacity(int byteCount) {
        if (byteCount > bytes.length - size) {
            bytes = Arrays.copyOf(bytes, Math.max(size + byteCount, 2 * bytes.length));
        }
    }

    /** Writes a fixed32 field: the four bytes of the value, low byte first. A float is written as its bits. */
    void writeFixed32(int fieldNumber, int value) {
        writeTag(fieldNumber, FIXED32);
        writeLittleEndian(value, Integer.BYTES);
    }

    /** Writes a fixed64 field: the eight bytes of the value, low byte first. A double is written as its bits. */
    void writeFixed64(int fieldNumber, long value) {
        writeTag(fieldNumber, FIXED64);
        writeLittleEndian(value, Long.BYTES);
    }

    private void writeLittleEndian(long value, int byteCount) {
        requireCapacity(byteCount);
        for (int i = 0; i < byteCount; i++) {
            bytes[size++] = (byte) (value >>> (8 * i));
        }
    }

    /**
     * Writes a length-delimited field holding the text in UTF-8. Protobuf writes the bytes returned by
     * {@code getBytes(UTF_8)}, also for a text with an unpaired surrogate.
     */
    void writeString(int fieldNumber, String value) {
        byte[] utf8 = value.getBytes(UTF_8);
        writeFieldHeader(fieldNumber, utf8.length);
        appendBytes(utf8, 0, utf8.length);
    }

    /** Writes a length-delimited field holding the bytes written by another writer: an embedded message. */
    void writeMessage(int fieldNumber, WireWriter message) {
        writeFieldHeader(fieldNumber, message.size);
        appendBytes(message.bytes, 0, message.size);
    }

    /**
     * Writes the tag and the length of a length-delimited field, such as an embedded message written in place. The
     * caller must then write exactly {@code length} bytes: the content of the field.
     */
    void writeFieldHeader(int fieldNumber, int length) {
        writeTag(fieldNumber, LENGTH_DELIMITED);
        writeVarint(length);
    }

    /**
     * Writes the bytes of {@code source} from {@code from} inclusive to {@code to} exclusive as they are, such as the
     * content of a field after {@link #writeFieldHeader}.
     */
    void appendBytes(byte[] source, int from, int to) {
        int length = to - from;
        requireCapacity(length);
        System.arraycopy(source, from, bytes, size, length);
        size += length;
    }

    /**
     * Writes the values in one packed field of varints, each as an unsigned 32-bit value. Protobuf writes no field for
     * an empty array.
     */
    void writePackedVarints(int fieldNumber, IntArray values) {
        if (values.isEmpty()) {
            return;
        }
        writeFieldHeader(fieldNumber, packedLength(values));
        int[] array = values.array();
        for (int i = 0; i < values.size(); i++) {
            writeVarint(Integer.toUnsignedLong(array[i]));
        }
    }

    /**
     * The length of the field written by {@link #writeVarint64} for the value. A field written by
     * {@link #writeVarint32} has the same length when its value is not negative.
     */
    static int varintFieldLength(int fieldNumber, long value) {
        return tagLength(fieldNumber, VARINT) + varintLength(value);
    }

    /** The length of the field written by {@link #writePackedVarints(int, IntArray)} for the values: 0 when empty. */
    static int packedVarintsFieldLength(int fieldNumber, IntArray values) {
        if (values.isEmpty()) {
            return 0;
        }
        return lengthDelimitedFieldLength(fieldNumber, packedLength(values));
    }

    /**
     * The length of a length-delimited field with {@code contentLength} bytes of content: the bytes written by
     * {@link #writeFieldHeader}, then the content.
     */
    static int lengthDelimitedFieldLength(int fieldNumber, int contentLength) {
        return tagLength(fieldNumber, LENGTH_DELIMITED) + varintLength(contentLength) + contentLength;
    }

    private static int tagLength(int fieldNumber, int wireType) {
        return varintLength(tag(fieldNumber, wireType));
    }

    private static int packedLength(IntArray values) {
        int[] array = values.array();
        int length = 0;
        for (int i = 0; i < values.size(); i++) {
            length += varintLength(Integer.toUnsignedLong(array[i]));
        }
        return length;
    }

    // Each byte of a varint holds 7 bits of the value
    private static int varintLength(long value) {
        int bits = Long.SIZE - Long.numberOfLeadingZeros(value);
        return Math.max(1, (bits + 6) / 7);
    }

    /** Writes the bytes written by another writer: fields written apart, in their place in the message. */
    void append(WireWriter fields) {
        appendBytes(fields.bytes, 0, fields.size);
    }

    /** Returns a copy of the bytes written. */
    byte[] toByteArray() {
        return Arrays.copyOf(bytes, size);
    }

    /** Writes the bytes of {@link #toByteArray()} to the stream, without copying them first. The writer keeps them. */
    void writeTo(OutputStream out) throws IOException {
        out.write(bytes, 0, size);
    }

    /** The number of bytes written since the writer was created or cleared. */
    int size() {
        return size;
    }

    /** The length of the array holding the bytes, at least {@link #size()}. {@link #clear()} keeps the array. */
    int capacity() {
        return bytes.length;
    }

    /** Whether no byte was written since the writer was created or cleared. */
    boolean isEmpty() {
        return size == 0;
    }

    /** Removes the bytes written. The writer keeps its array for the next message. */
    void clear() {
        size = 0;
    }

    /**
     * Keeps the first {@code size} bytes and removes the others, such as the fields of a feature that failed to write.
     *
     * @throws IndexOutOfBoundsException if {@code size} is negative or above {@link #size()}
     */
    void truncate(int size) {
        Objects.checkFromToIndex(0, size, this.size);
        this.size = size;
    }
}
