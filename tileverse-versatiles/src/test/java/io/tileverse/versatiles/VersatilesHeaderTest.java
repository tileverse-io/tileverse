/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

import io.tileverse.geom.BoundingBox2D;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VersatilesHeaderTest {

    @TempDir
    Path tmp;

    private ByteBuffer andorraHeader;

    @BeforeEach
    void readAndorraHeader() throws IOException {
        byte[] file = Files.readAllBytes(VersatilesTestData.andorra(tmp));
        andorraHeader = ByteBuffer.wrap(Arrays.copyOf(file, VersatilesHeader.HEADER_BYTES));
    }

    @Test
    void deserializeAndorraHeader() throws IOException {
        VersatilesHeader header = deserialize(andorraHeader.array());

        assertThat(header.tileFormat()).isEqualTo(TileFormat.PBF);
        assertThat(header.compression()).isEqualTo(Compression.GZIP);
        assertThat(header.minZoom()).isZero();
        assertThat(header.maxZoom()).isEqualTo(14);
        assertThat(header.minLon()).isCloseTo(1.40625, offset(1e-7));
        assertThat(header.minLat()).isCloseTo(42.4234565, offset(1e-7));
        assertThat(header.maxLon()).isCloseTo(1.801757, offset(1e-6));
        assertThat(header.maxLat()).isCloseTo(42.666281, offset(1e-6));
        assertThat(header.metadataOffset()).isEqualTo(VersatilesHeader.HEADER_BYTES);
        assertThat(header.metadataLength()).isEqualTo(1041L);
        assertThat(header.blockIndexOffset()).isEqualTo(0x312b12L);
        assertThat(header.blockIndexLength()).isEqualTo(278L);
    }

    @Test
    void rejectsBadMagic() {
        byte[] bogus = new byte[VersatilesHeader.HEADER_BYTES];
        byte[] magic = "nottheformat__".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(magic, 0, bogus, 0, magic.length);

        assertThatThrownBy(() -> deserialize(bogus))
                .isInstanceOf(InvalidHeaderException.class)
                .hasMessageContaining("Invalid magic bytes");
    }

    @Test
    void truncatedHeaderIsInvalid() {
        byte[] truncated = Arrays.copyOf(andorraHeader.array(), 30);

        assertThatThrownBy(() -> deserialize(truncated)).isInstanceOf(InvalidHeaderException.class);
    }

    @Test
    void unknownTileFormatIsInvalid() {
        andorraHeader.put(14, (byte) 0x7F);

        assertThatThrownBy(() -> deserialize(andorraHeader.array())).isInstanceOf(InvalidHeaderException.class);
    }

    @Test
    void unknownCompressionIsInvalid() {
        andorraHeader.put(15, (byte) 0x09);

        assertThatThrownBy(() -> deserialize(andorraHeader.array())).isInstanceOf(InvalidHeaderException.class);
    }

    @Test
    void reversedZoomRangeIsInvalid() {
        andorraHeader.put(16, (byte) 5);
        andorraHeader.put(17, (byte) 3);

        assertThatThrownBy(() -> deserialize(andorraHeader.array())).isInstanceOf(InvalidHeaderException.class);
    }

    // The versatiles tool writes an inverted box when it converts with a bounding box outside the data.
    @Test
    void invertedBoundsReadAsWorld() throws IOException {
        andorraHeader
                .putInt(18, 100_000_000)
                .putInt(22, 424_000_000)
                .putInt(26, 18_000_000)
                .putInt(30, 110_000_000);

        VersatilesHeader header = deserialize(andorraHeader.array());

        assertThat(header.geographicBoundingBox()).isEqualTo(BoundingBox2D.extent(-180, -90, 180, 90));
    }

    // A metadata blob cannot exceed the 2 GiB of a Java array.
    @Test
    void hugeMetadataIsInvalid() {
        andorraHeader.putLong(42, 1L << 40);

        assertThatThrownBy(() -> deserialize(andorraHeader.array())).isInstanceOf(InvalidHeaderException.class);
    }

    private static VersatilesHeader deserialize(byte[] bytes) throws IOException {
        return VersatilesHeader.deserialize(Channels.newChannel(new ByteArrayInputStream(bytes)));
    }
}
