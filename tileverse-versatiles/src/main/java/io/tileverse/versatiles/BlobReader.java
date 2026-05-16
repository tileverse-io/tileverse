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
package io.tileverse.versatiles;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import io.tileverse.io.IOFunction;
import io.tileverse.storage.RangeReader;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

/**
 * Reads the parts of a Versatiles container. A part is read with a single range request and decompressed in memory.
 *
 * <p>Decompressing straight from the storage would let the read buffer of the decompressor size the requests: a tile of
 * 180 KB would take a dozen round trips to a remote store.
 */
final class BlobReader {

    private static final int DECOMPRESSED_BUFFER_BYTES = 4096;

    private final RangeReader rangeReader;

    BlobReader(RangeReader rangeReader) {
        this.rangeReader = requireNonNull(rangeReader);
    }

    /**
     * Reads {@code range}, decompresses it, and returns the result of {@code decoder}. The stream handed to
     * {@code decoder} is only valid during the call.
     *
     * @throws EOFException if the file ends before the range does
     */
    <T> T read(ByteRange range, Compression compression, IOFunction<InputStream, T> decoder) throws IOException {
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(range.length())) {
            ByteBuffer bytes = readFully(range, pooled.buffer());
            try (InputStream decompressed = decompress(asInputStream(bytes), compression)) {
                return decoder.apply(decompressed);
            }
        }
    }

    /** Reads a part compressed with Brotli, as the block index and the tile indexes always are. */
    <T> T readBrotli(ByteRange range, IOFunction<InputStream, T> decoder) throws IOException {
        return read(range, Compression.BROTLI, decoder);
    }

    private ByteBuffer readFully(ByteRange range, ByteBuffer target) throws EOFException {
        int read = rangeReader.readRange(range, target);
        if (read < range.length()) {
            throw new EOFException("%s ends before byte %d: read %d of the %d bytes at offset %d"
                    .formatted(
                            rangeReader.getSourceIdentifier(),
                            range.offset() + range.length(),
                            read,
                            range.length(),
                            range.offset()));
        }
        return target.flip();
    }

    private static InputStream asInputStream(ByteBuffer heapBuffer) {
        int offset = heapBuffer.arrayOffset() + heapBuffer.position();
        return new ByteArrayInputStream(heapBuffer.array(), offset, heapBuffer.remaining());
    }

    private static InputStream decompress(InputStream compressed, Compression compression) throws IOException {
        return switch (compression) {
            case NONE -> compressed;
            case GZIP -> buffered(new GzipCompressorInputStream(compressed));
            case BROTLI -> buffered(new BrotliCompressorInputStream(compressed));
            case ZSTD -> buffered(new ZstdCompressorInputStream(compressed));
        };
    }

    /** Without a buffer, a decoder reading one byte at a time would run the decompressor once per byte. */
    private static InputStream buffered(InputStream decompressor) {
        return new BufferedInputStream(decompressor, DECOMPRESSED_BUFFER_BYTES);
    }
}
