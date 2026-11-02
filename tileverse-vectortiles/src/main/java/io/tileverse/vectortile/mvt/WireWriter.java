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

import java.util.Arrays;
import java.util.List;

/**
 * Writes the protobuf wire format into a growing byte array, one field at a time. The builders write the fields of a
 * tile with it, without building protobuf objects.
 *
 * <p>The write methods write a field as protobuf writes a field of the same type and value. Protobuf writes the fields
 * of a message in the order of their field numbers, and the caller writes them in this order. An embedded message is
 * written apart first: protobuf writes the length of a message before its bytes.
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

    // Protobuf writes a tag as an unsigned 32-bit value
    private void writeTag(int fieldNumber, int wireType) {
        int tag = fieldNumber << 3 | wireType;
        writeVarint(Integer.toUnsignedLong(tag));
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
        writeTag(fieldNumber, LENGTH_DELIMITED);
        writeVarint(utf8.length);
        writeBytes(utf8, utf8.length);
    }

    /** Writes a length-delimited field holding the bytes written by another writer: an embedded message. */
    void writeMessage(int fieldNumber, WireWriter message) {
        writeTag(fieldNumber, LENGTH_DELIMITED);
        writeVarint(message.size);
        writeBytes(message.bytes, message.size);
    }

    private void writeBytes(byte[] source, int length) {
        requireCapacity(length);
        System.arraycopy(source, 0, bytes, size, length);
        size += length;
    }

    /**
     * Writes the values in one packed field of varints, each as an unsigned 32-bit value. Protobuf writes no field for
     * an empty list.
     */
    void writePackedVarints(int fieldNumber, List<Integer> values) {
        if (values.isEmpty()) {
            return;
        }
        writeTag(fieldNumber, LENGTH_DELIMITED);
        writeVarint(packedLength(values));
        for (int value : values) {
            writeVarint(Integer.toUnsignedLong(value));
        }
    }

    private static int packedLength(List<Integer> values) {
        int length = 0;
        for (int value : values) {
            length += varintLength(Integer.toUnsignedLong(value));
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
        writeBytes(fields.bytes, fields.size);
    }

    /** Returns a copy of the bytes written. */
    byte[] toByteArray() {
        return Arrays.copyOf(bytes, size);
    }

    /** Whether no byte was written since the writer was created or cleared. */
    boolean isEmpty() {
        return size == 0;
    }

    /** Removes the bytes written. The writer keeps its array for the next message. */
    void clear() {
        size = 0;
    }
}
