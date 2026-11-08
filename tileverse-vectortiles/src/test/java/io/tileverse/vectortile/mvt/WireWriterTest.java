/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class WireWriterTest {

    private static final int FIELD = 5;

    /** The geometry of a feature: a packed field of varints. */
    private static final int GEOMETRY = 4;

    @Test
    void writesVarints() throws IOException {
        for (int value : new int[] {0, 127, 128, 16383, 16384, Integer.MAX_VALUE, -1}) {
            byte[] expected = protobuf(out -> out.writeUInt32(FIELD, value));
            assertThat(written(writer -> writer.writeVarint32(FIELD, value)))
                    .as("uint32 %d", value)
                    .isEqualTo(expected);
        }
        for (long value : new long[] {Long.MAX_VALUE, -1L}) {
            byte[] expected = protobuf(out -> out.writeUInt64(FIELD, value));
            assertThat(written(writer -> writer.writeVarint64(FIELD, value)))
                    .as("uint64 %d", value)
                    .isEqualTo(expected);
        }
    }

    @Test
    void writesFixedValues() throws IOException {
        for (float value : new float[] {1.5f, Float.NaN}) {
            byte[] expected = protobuf(out -> out.writeFloat(FIELD, value));
            int bits = Float.floatToRawIntBits(value);
            assertThat(written(writer -> writer.writeFixed32(FIELD, bits)))
                    .as("float %s", value)
                    .isEqualTo(expected);
        }
        for (double value : new double[] {-0.0, 1e300}) {
            byte[] expected = protobuf(out -> out.writeDouble(FIELD, value));
            long bits = Double.doubleToRawLongBits(value);
            assertThat(written(writer -> writer.writeFixed64(FIELD, bits)))
                    .as("double %s", value)
                    .isEqualTo(expected);
        }
    }

    @Test
    void writesStrings() throws IOException {
        for (String value : List.of("", "abc", "K\u00f6ln", "\u041c\u043e\u0441\u043a\u0432\u0430")) {
            byte[] expected = protobuf(out -> out.writeString(FIELD, value));
            assertThat(written(writer -> writer.writeString(FIELD, value)))
                    .as(value)
                    .isEqualTo(expected);
        }
    }

    // 300 and -1 take two and five bytes, as unsigned 32-bit values
    @Test
    void writesPackedVarintsFromArray() {
        for (List<Integer> values : List.of(List.of(9, 50, 34, 2, 18), List.of(300, -1))) {
            VectorTileProto.Tile.Feature feature = VectorTileProto.Tile.Feature.newBuilder()
                    .addAllGeometry(values)
                    .build();
            IntArray array = intArray(values);
            assertThat(written(writer -> writer.writePackedVarints(GEOMETRY, array)))
                    .as("%s", values)
                    .isEqualTo(feature.toByteArray());
        }
        assertThat(written(writer -> writer.writePackedVarints(GEOMETRY, new IntArray())))
                .isEmpty();
    }

    // The bytes come from the middle of a larger array, as the runs of a decoded tile
    @Test
    void writesFieldHeaderThenBytes() {
        byte[] source = {-1, 9, 50, 34, -1};
        IntArray commands = intArray(List.of(9, 50, 34));

        byte[] headerThenBytes = written(writer -> {
            writer.writeFieldHeader(GEOMETRY, 3);
            writer.appendBytes(source, 1, 4);
        });

        byte[] packed = written(writer -> writer.writePackedVarints(GEOMETRY, commands));
        assertThat(headerThenBytes).isEqualTo(packed);
    }

    @Test
    void computesFieldLengths() {
        IntArray commands = intArray(List.of(9, 50, 34));

        assertThat(WireWriter.varintFieldLength(1, 300)).isEqualTo(3);
        assertThat(WireWriter.packedVarintsFieldLength(GEOMETRY, commands)).isEqualTo(5);
        assertThat(WireWriter.packedVarintsFieldLength(GEOMETRY, new IntArray()))
                .isZero();
    }

    // A length of 300 takes two bytes
    @Test
    void computesLengthDelimitedFieldLength() {
        byte[] content = new byte[300];

        byte[] field = written(writer -> {
            writer.writeFieldHeader(GEOMETRY, content.length);
            writer.appendBytes(content, 0, content.length);
        });

        assertThat(WireWriter.lengthDelimitedFieldLength(GEOMETRY, content.length))
                .isEqualTo(field.length);
    }

    @Test
    void writesToStream() throws IOException {
        WireWriter writer = new WireWriter();
        writer.writeVarint32(FIELD, 300);
        writer.writeString(FIELD, "abc");
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        writer.writeTo(out);

        assertThat(out.toByteArray()).isEqualTo(writer.toByteArray());
    }

    // A field longer than the initial array: the writer grows
    @Test
    void reportsSizeAndCapacity() {
        WireWriter writer = new WireWriter();
        writer.writeString(FIELD, "x".repeat(300));

        assertThat(writer.size()).isEqualTo(writer.toByteArray().length);
        assertThat(writer.capacity()).isGreaterThanOrEqualTo(writer.size());
    }

    @Test
    void truncateKeepsPrefix() {
        WireWriter writer = new WireWriter();
        writer.writeVarint32(FIELD, 300);
        int size = writer.size();
        writer.writeString(FIELD, "abc");

        writer.truncate(size);

        assertThat(writer.toByteArray()).isEqualTo(written(field -> field.writeVarint32(FIELD, 300)));
        assertThatThrownBy(() -> writer.truncate(size + 1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void isEmptyUntilFieldWritten() {
        WireWriter writer = new WireWriter();
        boolean emptyBeforeWrite = writer.isEmpty();
        writer.writeVarint32(FIELD, 0);
        boolean emptyAfterWrite = writer.isEmpty();
        writer.clear();

        assertThat(emptyBeforeWrite).isTrue();
        assertThat(emptyAfterWrite).isFalse();
        assertThat(writer.isEmpty()).isTrue();
    }

    /** A write to the output stream of protobuf. */
    private interface ProtobufWrite {
        void writeTo(CodedOutputStream out) throws IOException;
    }

    private static byte[] protobuf(ProtobufWrite write) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        write.writeTo(out);
        out.flush();
        return bytes.toByteArray();
    }

    private static byte[] written(Consumer<WireWriter> write) {
        WireWriter writer = new WireWriter();
        write.accept(writer);
        return writer.toByteArray();
    }

    private static IntArray intArray(List<Integer> values) {
        IntArray array = new IntArray();
        values.forEach(array::add);
        return array;
    }
}
