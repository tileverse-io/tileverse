/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquet.reader;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.parquet.format.CompressionCodec;
import org.xerial.snappy.Snappy;

/**
 * Decompresses Parquet page data for all supported codecs.
 *
 * <p>Supported codecs: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4_RAW, LZ4 (legacy Hadoop framing),
 * and BROTLI. LZO is not supported and will throw {@link IOException}.
 *
 * <p>Where possible, direct byte-array APIs are used for performance (Snappy, ZSTD, LZ4). Stream-
 * based decompression via commons-compress is used for GZIP and Brotli.
 */
final class CompressionUtil {

    private CompressionUtil() {}

    static byte[] decompress(CompressionCodec codec, byte[] payload, int uncompressedSize) throws IOException {
        return switch (codec) {
            case UNCOMPRESSED -> payload;
            case SNAPPY -> Snappy.uncompress(payload);
            case GZIP ->
                decompressStream(new GzipCompressorInputStream(new ByteArrayInputStream(payload)), uncompressedSize);
            case ZSTD -> decompressZstd(payload, uncompressedSize);
            case LZ4_RAW -> decompressLz4Raw(payload, uncompressedSize);
            case LZ4 -> decompressLz4Hadoop(payload, uncompressedSize);
            case BROTLI ->
                decompressStream(new BrotliCompressorInputStream(new ByteArrayInputStream(payload)), uncompressedSize);
            case LZO -> throw new IOException("Unsupported compression codec in core reader: " + codec);
            default -> throw new IllegalArgumentException("Unexpected compression codec: " + codec);
        };
    }

    private static byte[] decompressZstd(byte[] payload, int uncompressedSize) throws IOException {
        byte[] out = Zstd.decompress(payload, uncompressedSize);
        if (out.length != uncompressedSize) {
            throw new IOException("Unexpected ZSTD decompressed size: " + out.length + " expected " + uncompressedSize);
        }
        return out;
    }

    private static byte[] decompressLz4Raw(byte[] payload, int uncompressedSize) {
        byte[] out = new byte[uncompressedSize];
        LZ4Factory.fastestInstance().fastDecompressor().decompress(payload, 0, out, 0, uncompressedSize);
        return out;
    }

    /**
     * Decompresses data using the legacy Hadoop LZ4 block framing format.
     *
     * <p>Frame layout (all integers are big-endian):
     * <pre>
     * [4 bytes: total uncompressed size]
     * [4 bytes: compressed block size][4 bytes: uncompressed block size][compressed data]
     * ...repeated blocks...
     * [4 bytes: 0 — end marker]
     * </pre>
     */
    private static byte[] decompressLz4Hadoop(byte[] payload, int uncompressedSize) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int totalDecompressed = buf.getInt(); // total uncompressed size header
        if (totalDecompressed != uncompressedSize) {
            throw new IOException("LZ4 Hadoop frame header declares %d bytes but page header says %d"
                    .formatted(totalDecompressed, uncompressedSize));
        }
        byte[] out = new byte[uncompressedSize];
        int outOffset = 0;
        var decompressor = LZ4Factory.fastestInstance().fastDecompressor();
        while (buf.hasRemaining()) {
            int compressedBlockSize = buf.getInt();
            if (compressedBlockSize == 0) {
                break; // end marker
            }
            int uncompressedBlockSize = buf.getInt();
            int blockStart = buf.position();
            decompressor.decompress(payload, blockStart, out, outOffset, uncompressedBlockSize);
            buf.position(blockStart + compressedBlockSize);
            outOffset += uncompressedBlockSize;
        }
        if (outOffset != uncompressedSize) {
            throw new IOException(
                    "LZ4 Hadoop decompressed %d bytes, expected %d".formatted(outOffset, uncompressedSize));
        }
        return out;
    }

    private static byte[] decompressStream(InputStream in, int uncompressedSize) throws IOException {
        byte[] out = new byte[uncompressedSize];
        try (in) {
            int offset = 0;
            while (offset < uncompressedSize) {
                int n = in.read(out, offset, uncompressedSize - offset);
                if (n < 0) break;
                offset += n;
            }
        }
        return out;
    }
}
