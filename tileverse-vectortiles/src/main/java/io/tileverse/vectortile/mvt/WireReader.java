/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Reads the protobuf wire format from a range of a byte array, one field at a time. The decoder walks the fields of a
 * tile with it, without building protobuf objects.
 *
 * <p>The reader follows the rules of protobuf's array parser, to decode the same values and to fail on the same tiles.
 * It throws a {@link MalformedTileException} on a read past the end of the range, a negative length, a field number 0,
 * wire types 6 and 7, a START_GROUP or END_GROUP tag without its match, and groups nested past protobuf's recursion
 * limit.
 *
 * @see <a href="https://protobuf.dev/programming-guides/encoding/">Protocol Buffers encoding</a>
 */
final class WireReader {

    static final int VARINT = 0;
    static final int FIXED64 = 1;
    static final int LENGTH_DELIMITED = 2;
    static final int START_GROUP = 3;
    static final int END_GROUP = 4;
    static final int FIXED32 = 5;

    /** The default recursion limit of protobuf's parser, for nested messages and nested groups together. */
    private static final int PROTOBUF_RECURSION_LIMIT = 100;

    private final byte[] bytes;
    private final int depth;
    private int position;
    private int end;

    // Offset of the last tag read, for the messages of skipField
    private int tagPosition;

    /**
     * Reads the bytes from {@code start} inclusive to {@code end} exclusive.
     *
     * @param depth the recursion depth of the message read, as counted by protobuf: 0 for the tile, 1 for a layer, 2
     *     for a feature or a value. Protobuf counts nested messages and nested groups against one limit.
     */
    WireReader(byte[] bytes, int start, int end, int depth) {
        this.bytes = bytes;
        this.depth = depth;
        this.position = start;
        this.end = end;
    }

    /** Moves the reader to another range of the same array, holding a message of the same depth. */
    void reset(int start, int end) {
        this.position = start;
        this.end = end;
    }

    boolean hasRemaining() {
        return position < end;
    }

    int position() {
        return position;
    }

    /**
     * Reads the tag of the next field: its field number and wire type, as {@code fieldNumber << 3 | wireType}.
     *
     * @throws MalformedTileException if the field number is 0
     */
    int readTag() {
        tagPosition = position;
        int tag = readVarint32();
        if (fieldNumber(tag) == 0) {
            throw malformed(tagPosition, "field number 0");
        }
        return tag;
    }

    /**
     * Reads a varint as a 64-bit value, as protobuf reads an int64, a uint64 or a bool. Protobuf accepts a varint of
     * any length. The shift of each byte grows by 7 bits, and Java takes it modulo 64: the bits of the bytes past the
     * tenth land in the low bits of the value.
     */
    long readVarint64() {
        int start = position;
        long value = 0;
        int shift = 0;
        byte b;
        do {
            b = readVarintByte(start);
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while (b < 0);
        return value;
    }

    /**
     * Reads a varint as a 32-bit value, as protobuf reads a tag, a length, an int32, a uint32 or an enum. The value is
     * made of the low 32 bits of the first five bytes. The bytes past the fifth are skipped up to the end of the
     * varint.
     */
    int readVarint32() {
        int start = position;
        int value = 0;
        for (int shift = 0; shift < 35; shift += 7) {
            byte b = readVarintByte(start);
            value |= (b & 0x7F) << shift;
            if (b >= 0) {
                return value;
            }
        }
        skipVarintTail(start);
        return value;
    }

    private byte readVarintByte(int varintStart) {
        if (position >= end) {
            throw malformed(varintStart, "varint cut by the end of the range");
        }
        return bytes[position++];
    }

    private void skipVarintTail(int varintStart) {
        byte b;
        do {
            b = readVarintByte(varintStart);
        } while (b < 0);
    }

    /** Reads four bytes as a little-endian 32-bit value. */
    int readFixed32() {
        requireBytes(4);
        int value = (bytes[position] & 0xFF)
                | (bytes[position + 1] & 0xFF) << 8
                | (bytes[position + 2] & 0xFF) << 16
                | (bytes[position + 3] & 0xFF) << 24;
        position += 4;
        return value;
    }

    /** Reads eight bytes as a little-endian 64-bit value. */
    long readFixed64() {
        requireBytes(8);
        long low = Integer.toUnsignedLong(readFixed32());
        long high = Integer.toUnsignedLong(readFixed32());
        return high << 32 | low;
    }

    private void requireBytes(int count) {
        if (count > end - position) {
            throw malformed(position, count + " bytes go past the end of the range");
        }
    }

    /**
     * Reads the length of a length-delimited field. The reader stays at the first content byte.
     *
     * @throws MalformedTileException if the length is negative or goes past the end of the range
     */
    int readLength() {
        int start = position;
        int length = readVarint32();
        if (length < 0) {
            throw malformed(start, "negative length " + length);
        }
        if (length > end - position) {
            throw malformed(start, "length " + length + " goes past the end of the range");
        }
        return length;
    }

    /**
     * Reads a length-delimited field as UTF-8 text. As in protobuf, malformed sequences become the replacement
     * character.
     */
    String readString() {
        int length = readLength();
        String value = new String(bytes, position, length, UTF_8);
        position += length;
        return value;
    }

    /** Moves the reader {@code length} bytes forward. */
    void skip(int length) {
        requireBytes(length);
        position += length;
    }

    /**
     * Skips the content of a field, right after {@link #readTag()} returned its tag. A group is skipped with its nested
     * fields, up to the END_GROUP tag of the same field number.
     *
     * @throws MalformedTileException on an END_GROUP tag outside its group, on wire types 6 and 7, on a field or a
     *     group cut by the end of the range, and on groups nested past protobuf's recursion limit
     */
    void skipField(int tag) {
        skipField(tag, depth);
    }

    // As in protobuf, each nested group adds one to the recursion depth
    private void skipField(int tag, int enclosingDepth) {
        switch (wireType(tag)) {
            case VARINT -> readVarint64();
            case FIXED64 -> skip(8);
            case LENGTH_DELIMITED -> skip(readLength());
            case START_GROUP -> skipGroup(tag, enclosingDepth + 1);
            case END_GROUP -> throw malformed(tagPosition, "end of group " + fieldNumber(tag) + " without its start");
            case FIXED32 -> skip(4);
            default -> throw malformed(tagPosition, "unknown wire type " + wireType(tag));
        }
    }

    private void skipGroup(int startTag, int groupDepth) {
        int groupStart = tagPosition;
        if (groupDepth >= PROTOBUF_RECURSION_LIMIT) {
            throw malformed(groupStart, "group " + fieldNumber(startTag) + " nested past protobuf's recursion limit");
        }
        int endTag = (startTag & ~7) | END_GROUP;
        while (hasRemaining()) {
            int tag = readTag();
            if (tag == endTag) {
                return;
            }
            skipField(tag, groupDepth);
        }
        throw malformed(groupStart, "group " + fieldNumber(startTag) + " cut by the end of the range");
    }

    /**
     * Skips a packed run of varints, right after the tag of its field, and returns the offset of its first byte.
     * Protobuf rejects a run cut in the middle of a varint. A varint ends on a byte with its high bit clear, and the
     * check reads the last byte of the run alone.
     */
    int skipPackedVarints() {
        int length = readLength();
        int runStart = position;
        int runEnd = runStart + length;
        if (length > 0 && bytes[runEnd - 1] < 0) {
            throw malformed(runEnd - 1, "packed varints cut by the end of their field");
        }
        position = runEnd;
        return runStart;
    }

    private static int fieldNumber(int tag) {
        return tag >>> 3;
    }

    private static int wireType(int tag) {
        return tag & 7;
    }

    private static MalformedTileException malformed(int offset, String problem) {
        return new MalformedTileException("At byte " + offset + ": " + problem);
    }
}
