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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.commons.io.IOUtils;

/**
 * Utility class for compressing and decompressing data using various
 * compression algorithms.
 */
public final class CompressionUtil {

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
    public static byte[] compress(byte[] data, byte compressionType)
            throws IOException, UnsupportedCompressionException {
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

    public static ByteBuffer decompress(ByteBuffer buffer, byte compression)
            throws UnsupportedCompressionException, IOException {

        final int compressedLength = buffer.remaining();

        if (buffer.hasArray() && compression != PMTilesHeader.COMPRESSION_NONE) {
            // For compressed data, we can safely use the backing array since decompress will create new data
            final int offset = buffer.position();
            final byte[] compressed = buffer.array();
            return decompress(compressed, offset, compressedLength, compression);
        }

        // For uncompressed data or non-array buffers, create a copy to avoid sharing backing arrays
        byte[] compressed = new byte[compressedLength];
        buffer.get(compressed);
        return decompress(compressed, 0, compressedLength, compression);
    }

    /**
     * Decompresses data using the specified compression type.
     *
     * @param data            the data to decompress
     * @param compressionType the compression type used
     * @return the decompressed data
     * @throws IOException                     if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not
     *                                         supported
     */
    public static ByteBuffer decompress(byte[] data, int offset, int length, byte compressionType)
            throws IOException, UnsupportedCompressionException {
        if (compressionType == PMTilesHeader.COMPRESSION_NONE) {
            return ByteBuffer.wrap(data, offset, length);
        }

        ByteArrayInputStream inputStream = new ByteArrayInputStream(data, offset, length);
        ByteArrayOutputStreamInternal outputStream = new ByteArrayOutputStreamInternal(4 * length);

        try (InputStream decompressor = createDecompressor(inputStream, compressionType)) {
            IOUtils.copy(decompressor, outputStream);
        } catch (CompressorException e) {
            throw new IOException("Failed to create decompressor", e);
        }

        return ByteBuffer.wrap(outputStream.bytes()).limit(outputStream.size());
    }

    private static class ByteArrayOutputStreamInternal extends ByteArrayOutputStream {
        ByteArrayOutputStreamInternal(int initialSize) {
            super(initialSize);
        }

        public byte[] bytes() {
            return super.buf;
        }
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
    private static OutputStream createCompressor(OutputStream outputStream, byte compressionType)
            throws IOException, UnsupportedCompressionException {
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
    private static InputStream createDecompressor(InputStream inputStream, byte compressionType)
            throws IOException, CompressorException, UnsupportedCompressionException {
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
