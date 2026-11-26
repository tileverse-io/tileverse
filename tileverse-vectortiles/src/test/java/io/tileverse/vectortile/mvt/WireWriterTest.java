/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void writesPackedVarints() {
        List<Integer> commands = List.of(9, 50, 34, 2, 18);
        VectorTileProto.Tile.Feature feature = VectorTileProto.Tile.Feature.newBuilder()
                .addAllGeometry(commands)
                .build();

        assertThat(written(writer -> writer.writePackedVarints(GEOMETRY, commands)))
                .isEqualTo(feature.toByteArray());
        assertThat(written(writer -> writer.writePackedVarints(GEOMETRY, List.of())))
                .isEmpty();
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
}
