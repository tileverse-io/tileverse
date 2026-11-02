/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;

/**
 * Writes protobuf messages byte by byte, for tests of hand-built tiles. The raw bytes and the lone tags let a test
 * write malformed fields too.
 */
final class ProtoWriter {

    // Kept apart from the constants of WireReader: a wrong constant in the reader fails the tests
    private static final int VARINT = 0;
    private static final int FIXED64 = 1;
    private static final int LENGTH_DELIMITED = 2;
    private static final int START_GROUP = 3;
    private static final int END_GROUP = 4;
    private static final int FIXED32 = 5;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    /** Writes the low 8 bits of each value. */
    ProtoWriter raw(int... bytes) {
        for (int b : bytes) {
            out.write(b);
        }
        return this;
    }

    /** Writes the value as an unsigned varint: a negative value takes ten bytes. */
    ProtoWriter varint(long value) {
        long rest = value;
        while ((rest & ~0x7FL) != 0) {
            out.write((int) (rest & 0x7F) | 0x80);
            rest >>>= 7;
        }
        out.write((int) rest);
        return this;
    }

    /** Writes a tag alone, for fields without content or with a wire type unknown to protobuf. */
    ProtoWriter tag(int field, int wireType) {
        int tag = field << 3 | wireType;
        return varint(Integer.toUnsignedLong(tag));
    }

    ProtoWriter varintField(int field, long value) {
        tag(field, VARINT);
        return varint(value);
    }

    ProtoWriter fixed32Field(int field, int value) {
        tag(field, FIXED32);
        writeLittleEndian(value, 4);
        return this;
    }

    ProtoWriter fixed64Field(int field, long value) {
        tag(field, FIXED64);
        writeLittleEndian(value, 8);
        return this;
    }

    private void writeLittleEndian(long value, int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            out.write((int) (value >>> (8 * i)));
        }
    }

    ProtoWriter bytesField(int field, byte[] content) {
        tag(field, LENGTH_DELIMITED);
        varint(content.length);
        out.writeBytes(content);
        return this;
    }

    ProtoWriter stringField(int field, String value) {
        return bytesField(field, value.getBytes(UTF_8));
    }

    /** Writes the values in one packed field, each as an unsigned 32-bit varint of one to five bytes. */
    ProtoWriter packedField(int field, int... values) {
        ProtoWriter run = new ProtoWriter();
        for (int value : values) {
            run.varint(Integer.toUnsignedLong(value));
        }
        return bytesField(field, run.toByteArray());
    }

    /** Writes the content between a START_GROUP and an END_GROUP tag of the field. */
    ProtoWriter group(int field, byte[] content) {
        tag(field, START_GROUP);
        out.writeBytes(content);
        return tag(field, END_GROUP);
    }

    byte[] toByteArray() {
        return out.toByteArray();
    }
}
