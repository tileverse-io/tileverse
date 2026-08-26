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

import java.io.BufferedInputStream;
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

/** Utility class for compressing and decompressing data using various compression algorithms. */
final class CompressionUtil {

    private CompressionUtil() {
        // Prevent instantiation
    }

    /**
     * Compresses data using the specified compression type.
     *
     * @param data the data to compress
     * @param compressionType the compression type to use
     * @return the compressed data
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
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

    /**
     * Returns a stream that decompresses the readable bytes of {@code data} with the given compression type. The buffer
     * is consumed between its position and limit, advancing the position as the stream is read; the caller keeps
     * ownership and must keep it valid while the stream is read. Heap and direct buffers stream without copying.
     *
     * @param data the compressed bytes, position to limit
     * @param compressionType the compression type in use
     * @return a stream over the decompressed bytes
     * @throws IOException if the decompressor cannot be created
     * @throws UnsupportedCompressionException if the compression type is not supported
     */
    public static InputStream decompressingInputStream(ByteBuffer data, byte compressionType) throws IOException {
        return decompress(new ByteBufferInputStream(data), compressionType);
    }

    private static final class ByteBufferInputStream extends InputStream {

        private final ByteBuffer buffer;

        ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            if (buffer.hasRemaining()) {
                return buffer.get() & 0xff;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (buffer.hasRemaining()) {
                int toRead = Math.min(len, buffer.remaining());
                buffer.get(b, off, toRead);
                return toRead;
            }
            return -1;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }
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
     * @param outputStream the output stream to write compressed data to
     * @param compressionType the compression type to use
     * @return a compressor output stream
     * @throws IOException if an I/O error occurs
     * @throws UnsupportedCompressionException if the compression type is not supported
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
     * @param inputStream the input stream containing compressed data
     * @param compressionType the compression type used
     * @return a decompressor input stream
     * @throws IOException if an I/O error occurs
     * @throws CompressorException if the compressor creation fails
     * @throws UnsupportedCompressionException if the compression type is not supported
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
