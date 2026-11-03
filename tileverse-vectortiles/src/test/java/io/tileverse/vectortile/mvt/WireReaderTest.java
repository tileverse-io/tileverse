/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WireReaderTest {

    @Test
    void readsMultiByteVarint() {
        WireReader reader = reader(new ProtoWriter().raw(0xAC, 0x02));

        assertThat(reader.readVarint64()).isEqualTo(300);
        assertThat(reader.position()).isEqualTo(2);
    }

    @Test
    void readsTenByteVarint() {
        WireReader reader = reader(new ProtoWriter().varint(-1L));

        assertThat(reader.readVarint64()).isEqualTo(-1L);
        reader.reset(0, 10);
        assertThat(reader.readVarint32()).isEqualTo(-1);
        assertThat(reader.position()).isEqualTo(10);
    }

    // Protobuf shifts the eleventh byte by 70 bits, taken modulo 64 by Java
    @Test
    void wrapsBitsPastSixtyFour() {
        WireReader reader = reader(new ProtoWriter().raw(repeated(0x80, 10)).raw(0x01));
        WireReader allOnes = reader(new ProtoWriter().raw(repeated(0xFF, 10)).raw(0x01));

        assertThat(reader.readVarint64()).isEqualTo(64);
        assertThat(reader.position()).isEqualTo(11);
        reader.reset(0, 11);
        assertThat(reader.readVarint32()).isZero();
        assertThat(reader.position()).isEqualTo(11);
        assertThat(allOnes.readVarint64()).isEqualTo(-1L);
    }

    @Test
    void rejectsCutVarint() {
        WireReader reader = reader(new ProtoWriter().raw(0x80));

        assertMalformed(reader::readVarint64);
        reader.reset(0, 1);
        assertMalformed(reader::readVarint32);
    }

    @Test
    void namesOffsetOfProblem() {
        WireReader reader = reader(new ProtoWriter().varintField(1, 300).raw(0x80));
        reader.readTag();
        reader.readVarint64();

        assertThatThrownBy(reader::readVarint64)
                .isInstanceOf(MalformedTileException.class)
                .hasMessageContaining("byte 3");
    }

    @Test
    void readsFixedWidthValues() {
        WireReader reader = reader(new ProtoWriter()
                .raw(0x04, 0x03, 0x02, 0x01)
                .raw(0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01)
                .raw(0xF4, 0xF3, 0xF2, 0xF1)
                .raw(0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1));

        assertThat(reader.readFixed32()).isEqualTo(0x01020304);
        assertThat(reader.readFixed64()).isEqualTo(0x0102030405060708L);
        assertThat(reader.readFixed32()).isEqualTo(0xF1F2F3F4);
        assertThat(reader.readFixed64()).isEqualTo(0xF1F2F3F4F5F6F7F8L);
    }

    @Test
    void rejectsCutFixedValues() {
        WireReader reader = reader(new ProtoWriter().raw(1, 2, 3, 4, 5, 6, 7));

        assertMalformed(reader::readFixed64);
        reader.reset(0, 3);
        assertMalformed(reader::readFixed32);
        reader.reset(0, 3);
        assertMalformed(() -> reader.skip(4));
    }

    @Test
    void rejectsLengthPastEnd() {
        WireReader reader = reader(new ProtoWriter().raw(0x05, 1, 2));

        assertMalformed(reader::readLength);
    }

    @Test
    void rejectsNegativeLength() {
        WireReader reader = reader(new ProtoWriter().varint(0xFFFFFFFFL));

        assertMalformed(reader::readLength);
    }

    // Each read fails at the end of its range, although the array holds the next byte
    @Test
    void stopsAtEndOfRange() {
        byte[] bytes = new ProtoWriter().raw(0xAC, 0x02, 0x01, 0x61).toByteArray();
        WireReader reader = new WireReader(bytes, 0, 1, 0);

        assertMalformed(reader::readVarint32);
        reader.reset(2, 3);
        assertMalformed(reader::readLength);
    }

    @Test
    void readsBadUtf8Leniently() {
        WireReader reader = reader(new ProtoWriter().varint(2).raw(0x61, 0xFF));

        assertThat(reader.readString()).isEqualTo("a\uFFFD");
    }

    @Test
    void rejectsFieldNumberZero() {
        WireReader varint = reader(new ProtoWriter().raw(0x00));
        WireReader lengthDelimited = reader(new ProtoWriter().raw(0x02));

        assertMalformed(varint::readTag);
        assertMalformed(lengthDelimited::readTag);
    }

    @Test
    void skipsKnownWireTypes() {
        WireReader reader = reader(new ProtoWriter()
                .varintField(1, 300)
                .fixed64Field(2, 7L)
                .bytesField(3, new byte[] {1, 2, 3})
                .fixed32Field(4, 7));

        List<Integer> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            int tag = reader.readTag();
            fields.add(tag >>> 3);
            reader.skipField(tag);
        }

        assertThat(fields).containsExactly(1, 2, 3, 4);
    }

    @Test
    void skipsNestedGroups() {
        byte[] innerField = new ProtoWriter().varintField(3, 1).toByteArray();
        byte[] innerGroup = new ProtoWriter().group(2, innerField).toByteArray();
        WireReader reader = reader(new ProtoWriter().group(1, innerGroup));

        reader.skipField(reader.readTag());

        assertThat(reader.hasRemaining()).isFalse();
    }

    @Test
    void rejectsCutGroup() {
        WireReader reader =
                reader(new ProtoWriter().tag(1, WireReader.START_GROUP).varintField(2, 1));
        int tag = reader.readTag();

        assertMalformed(() -> reader.skipField(tag));
    }

    @Test
    void rejectsMismatchedEndGroup() {
        WireReader reader =
                reader(new ProtoWriter().tag(1, WireReader.START_GROUP).tag(2, WireReader.END_GROUP));
        int tag = reader.readTag();

        assertMalformed(() -> reader.skipField(tag));
    }

    @Test
    void rejectsLoneEndGroup() {
        WireReader reader = reader(new ProtoWriter().tag(1, WireReader.END_GROUP));
        int tag = reader.readTag();

        assertMalformed(() -> reader.skipField(tag));
    }

    // Protobuf's recursion limit of 100 counts the depth of the message too: 0 for a tile, 2 for a feature
    @Test
    void skipsGroupsNestedToLimit() {
        WireReader tileReader = reader(nestedGroups(99), 0);
        WireReader featureReader = reader(nestedGroups(97), 2);

        tileReader.skipField(tileReader.readTag());
        featureReader.skipField(featureReader.readTag());

        assertThat(tileReader.hasRemaining()).isFalse();
        assertThat(featureReader.hasRemaining()).isFalse();
    }

    @Test
    void rejectsGroupsNestedTooDeep() {
        WireReader tileReader = reader(nestedGroups(100), 0);
        WireReader featureReader = reader(nestedGroups(98), 2);
        int tileTag = tileReader.readTag();
        int featureTag = featureReader.readTag();

        assertMalformed(() -> tileReader.skipField(tileTag));
        assertMalformed(() -> featureReader.skipField(featureTag));
    }

    // Eight content bytes follow the tag, enough for any field of a known fixed width
    @ParameterizedTest
    @ValueSource(ints = {6, 7})
    void rejectsUnknownWireTypes(int wireType) {
        WireReader reader = reader(new ProtoWriter().tag(1, wireType).raw(repeated(0x00, 8)));
        int tag = reader.readTag();

        assertMalformed(() -> reader.skipField(tag));
    }

    @Test
    void acceptsEmptyPackedRun() {
        WireReader reader = reader(new ProtoWriter().bytesField(2, new byte[0]));
        reader.readTag();

        reader.skipPackedVarints();

        assertThat(reader.hasRemaining()).isFalse();
    }

    // The run starts after the tag and the length, of one byte each
    @Test
    void skipsPackedRun() {
        WireReader reader = reader(new ProtoWriter().packedField(4, 9, 300).varintField(5, 1));
        reader.readTag();

        int runStart = reader.skipPackedVarints();

        assertThat(runStart).isEqualTo(2);
        assertThat(reader.readTag() >>> 3).isEqualTo(5);
    }

    // The field after the run could complete its last varint
    @Test
    void rejectsCutPackedRun() {
        WireReader reader = reader(
                new ProtoWriter().bytesField(2, new byte[] {0x01, (byte) 0x80}).varintField(3, 1));
        reader.readTag();

        assertMalformed(reader::skipPackedVarints);
    }

    private static WireReader reader(ProtoWriter writer) {
        return reader(writer.toByteArray(), 0);
    }

    private static WireReader reader(byte[] bytes, int depth) {
        return new WireReader(bytes, 0, bytes.length, depth);
    }

    /** Groups of field 1 nested in one another, {@code count} levels deep. */
    private static byte[] nestedGroups(int count) {
        byte[] groups = new byte[0];
        for (int i = 0; i < count; i++) {
            groups = new ProtoWriter().group(1, groups).toByteArray();
        }
        return groups;
    }

    private static void assertMalformed(ThrowingCallable read) {
        assertThatThrownBy(read).isInstanceOf(MalformedTileException.class);
    }

    private static int[] repeated(int value, int count) {
        int[] values = new int[count];
        Arrays.fill(values, value);
        return values;
    }
}
