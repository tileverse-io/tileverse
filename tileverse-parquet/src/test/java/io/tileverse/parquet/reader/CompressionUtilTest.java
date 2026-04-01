/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.parquet.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

class CompressionUtilTest {

    @Test
    void decompress_supportsUncompressedSnappyGzipAndLz4Raw() throws Exception {
        byte[] original = "tileverse-compression".getBytes(StandardCharsets.UTF_8);

        assertThat(CompressionUtil.decompress(CompressionCodec.UNCOMPRESSED, original, original.length))
                .isSameAs(original);

        byte[] snappy = Snappy.compress(original);
        assertThat(CompressionUtil.decompress(CompressionCodec.SNAPPY, snappy, original.length))
                .isEqualTo(original);

        byte[] gzip = gzip(original);
        assertThat(CompressionUtil.decompress(CompressionCodec.GZIP, gzip, original.length))
                .isEqualTo(original);

        byte[] lz4raw = lz4Raw(original);
        assertThat(CompressionUtil.decompress(CompressionCodec.LZ4_RAW, lz4raw, original.length))
                .isEqualTo(original);
    }

    @Test
    void decompress_supportsLegacyHadoopLz4Frames() throws Exception {
        byte[] original = "tileverse-hadoop-lz4".getBytes(StandardCharsets.UTF_8);

        byte[] framed = hadoopLz4Frame(original);

        assertThat(CompressionUtil.decompress(CompressionCodec.LZ4, framed, original.length))
                .isEqualTo(original);
    }

    @Test
    void decompress_lz4HadoopRejectsHeaderAndOutputSizeMismatches() throws Exception {
        byte[] original = "tileverse-hadoop-lz4".getBytes(StandardCharsets.UTF_8);
        byte[] framed = hadoopLz4Frame(original);

        ByteBuffer headerMismatch = ByteBuffer.wrap(framed.clone()).order(ByteOrder.BIG_ENDIAN);
        headerMismatch.putInt(0, original.length + 1);

        assertThatThrownBy(
                        () -> CompressionUtil.decompress(CompressionCodec.LZ4, headerMismatch.array(), original.length))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("LZ4 Hadoop frame header declares");

        ByteBuffer truncated = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        truncated.putInt(original.length);
        truncated.putInt(0);

        assertThatThrownBy(() -> CompressionUtil.decompress(CompressionCodec.LZ4, truncated.array(), original.length))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("LZ4 Hadoop decompressed 0 bytes");
    }

    private static byte[] gzip(byte[] original) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(original);
        }
        return out.toByteArray();
    }

    private static byte[] lz4Raw(byte[] original) {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int size = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);
        return java.util.Arrays.copyOf(compressed, size);
    }

    private static byte[] hadoopLz4Frame(byte[] original) {
        byte[] compressed = lz4Raw(original);
        ByteBuffer frame =
                ByteBuffer.allocate(4 + 4 + 4 + compressed.length + 4).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(original.length);
        frame.putInt(compressed.length);
        frame.putInt(original.length);
        frame.put(compressed);
        frame.putInt(0);
        return frame.array();
    }
}
