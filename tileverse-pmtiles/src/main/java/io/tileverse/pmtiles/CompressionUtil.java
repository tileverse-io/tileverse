/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.pmtiles;

import io.tileverse.io.ByteRange;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.commons.io.input.BoundedInputStream;

/**
 * Utility class for compressing and decompressing data using various
 * compression algorithms.
 */
final class CompressionUtil {

    private CompressionUtil() {
        // Prevent instantiation
    }

    /**
     * Compresses data using the specified compression type.
     *
     * @param data            the data to compress
     * @param compressionType the compression type to use
     * @return the compressed data
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    public static byte[] compress(byte[] data, byte compressionType) throws IOException {
        if (compressionType == PMTilesHeader.COMPRESSION_NONE) {
            return data;
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (OutputStream compressor = createCompressor(outputStream, compressionType)) {
            compressor.write(data);
            compressor.flush();
        }

        return outputStream.toByteArray();
    }

    public static InputStream decompressingInputStream(
            SeekableByteChannel channel, ByteRange byteRange, byte compressionType) throws IOException {

        final int size = byteRange.length();

        SeekableByteChannel positionedChannel = channel.position(byteRange.offset());
        BoundedInputStream boundedInputStream = boundedInputStream(positionedChannel, size);
        return decompress(boundedInputStream, compressionType);
    }

    private static BoundedInputStream boundedInputStream(SeekableByteChannel positionedChannel, final int maxCount)
            throws IOException {
        return BoundedInputStream.builder()
                .setInputStream(Channels.newInputStream(positionedChannel))
                .setMaxCount(maxCount)
                .setPropagateClose(false) // we don't own the channel
                .get();
    }

    static InputStream decompress(InputStream compressed, byte compressionType) throws IOException {
        if (compressionType == PMTilesHeader.COMPRESSION_NONE) {
            return compressed;
        }
        InputStream decompressor = createDecompressor(compressed, compressionType);
        return new BufferedInputStream(decompressor, 4096);
    }

    /**
     * Creates a compressor for the specified compression type.
     *
     * @param outputStream    the output stream to write compressed data to
     * @param compressionType the compression type to use
     * @return a compressor output stream
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    private static OutputStream createCompressor(OutputStream outputStream, byte compressionType) throws IOException {
        return switch (compressionType) {
            case PMTilesHeader.COMPRESSION_NONE -> outputStream;
            case PMTilesHeader.COMPRESSION_GZIP -> new GzipCompressorOutputStream(outputStream);
            case PMTilesHeader.COMPRESSION_ZSTD -> new ZstdCompressorOutputStream(outputStream);
            case PMTilesHeader.COMPRESSION_BROTLI ->
                throw new UnsupportedCompressionException("Compression type not supported: " + compressionType);
            default -> throw new UnsupportedCompressionException("Compression type not supported: " + compressionType);
        };
    }

    /**
     * Creates a decompressor for the specified compression type.
     *
     * @param inputStream     the input stream containing compressed data
     * @param compressionType the compression type used
     * @return a decompressor input stream
     * @throws IOException                     if an I/O error occurs
     * @throws CompressorException             if the compressor creation fails
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    private static InputStream createDecompressor(InputStream inputStream, byte compressionType) throws IOException {
        return switch (compressionType) {
            case PMTilesHeader.COMPRESSION_NONE ->
                throw new IllegalArgumentException("Cannot create decompressor for COMPRESSION_NONE");
            case PMTilesHeader.COMPRESSION_GZIP -> new GzipCompressorInputStream(inputStream);
            case PMTilesHeader.COMPRESSION_ZSTD -> new ZstdCompressorInputStream(inputStream);
            case PMTilesHeader.COMPRESSION_BROTLI -> new BrotliCompressorInputStream(inputStream);
            default -> throw new UnsupportedCompressionException("Compression type not supported: " + compressionType);
        };
    }
}
