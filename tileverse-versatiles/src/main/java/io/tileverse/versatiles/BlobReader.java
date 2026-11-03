/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import io.tileverse.io.IOFunction;
import io.tileverse.storage.RangeReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads the parts of a Versatiles container. A part is read with a single range request and decompressed in memory.
 *
 * <p>Decompressing straight from the storage would let the read buffer of the decompressor size the requests: a tile of
 * 180 KB would take a dozen round trips to a remote store.
 */
final class BlobReader {

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
            try (InputStream decompressed = compression.decompress(bytes)) {
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
}
