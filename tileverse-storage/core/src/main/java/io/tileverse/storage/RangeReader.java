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
package io.tileverse.storage;

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteRange;
import io.tileverse.io.SeekableByteChannelImageInputStream;
import io.tileverse.storage.adapters.RangeReaderSeekableByteChannel;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.OptionalLong;
import javax.imageio.stream.ImageInputStream;

/**
 * Interface for reading ranges of bytes from a source.
 *
 * <p>This abstraction allows data to be read from various sources such as local files, HTTP servers, or cloud storage
 * using range requests.
 *
 * <p>All implementations of this interface MUST be thread-safe to allow concurrent access from multiple threads without
 * interference. This is especially important in server environments where multiple requests may be accessing the same
 * reader.
 */
public interface RangeReader extends Closeable {

    /**
     * Reads bytes from the source at the specified offset.
     *
     * <p>This convenience method allocates a new ByteBuffer for each call. For better performance and reduced garbage
     * collection pressure, prefer using {@link #readRange(long, int, ByteBuffer)} with reusable buffers when possible
     * (see {@link ByteBufferPool}).
     *
     * @param offset The offset to read from
     * @param length The number of bytes to read
     * @return A ByteBuffer with its position set at the actual number of bytes read, needs flip() to be consumed
     * @throws StorageException If a storage error occurs
     * @throws IllegalArgumentException If offset or length is negative
     */
    default ByteBuffer readRange(long offset, int length) {
        // Allocate a new buffer and use our optimized implementation
        ByteBuffer buffer = ByteBuffer.allocate(length);
        int bytesRead = readRange(offset, length, buffer);
        assert bytesRead <= length;
        assert bytesRead == buffer.position(); // With NIO conventions: position should be advanced by bytesRead
        return buffer;
    }

    /**
     * Reads a byte range from the source.
     *
     * @param range The byte range to read.
     * @return A ByteBuffer containing the data.
     * @throws StorageException If a storage error occurs.
     */
    default ByteBuffer readRange(ByteRange range) {
        return readRange(requireNonNull(range).offset(), range.length());
    }

    /**
     * Reads bytes from the source at the specified offset into the provided target buffer.
     *
     * <p>This method allows callers to reuse ByteBuffers to reduce garbage collection pressure. Following standard NIO
     * conventions, after this method returns, the target buffer's position will be advanced by the number of bytes
     * read, and the caller must call {@code flip()} on the buffer to prepare it for reading. The caller is responsible
     * for ensuring that the target buffer has sufficient remaining capacity for the requested length.
     *
     * @param offset The offset to read from
     * @param length The number of bytes to read
     * @param target The ByteBuffer to read into, starting at its current position
     * @return The number of bytes actually read
     * @throws StorageException If a storage error occurs
     * @throws IllegalArgumentException If offset or length is negative, or if target is null
     * @throws IllegalArgumentException If target has insufficient remaining capacity
     * @throws java.nio.ReadOnlyBufferException If the target buffer is read-only
     */
    int readRange(long offset, int length, ByteBuffer target);

    /**
     * Reads a byte range from the source into the provided target buffer.
     *
     * @param range The byte range to read.
     * @param target The ByteBuffer to read into.
     * @return The number of bytes read.
     * @throws StorageException If a storage error occurs.
     */
    default int readRange(ByteRange range, ByteBuffer target) {
        return readRange(range.offset(), range.length(), target);
    }

    /**
     * Gets the total size of the source in bytes.
     *
     * <p>Implementations MUST ensure this method is thread-safe.
     *
     * @return The size in bytes, or empty if unknown
     * @throws StorageException If a storage error occurs
     */
    OptionalLong size();

    /**
     * Returns a stable, human-readable identifier for the underlying source - typically the URI
     * ({@code s3://bucket/key}, {@code https://...}, {@code /path/to/file}). Used in error messages, logging, and as a
     * cache-key seed by {@link io.tileverse.storage.cache.CachingRangeReader}.
     *
     * <p>Two readers pointing at the same source must return equal identifiers; two readers pointing at different
     * sources must not. The identifier is not required to round-trip back to a usable URI.
     *
     * @return a stable identifier for the underlying source
     */
    String getSourceIdentifier();

    /**
     * Closes this range reader and releases any underlying resource.This operation is idempotent.
     *
     * <p>The state of any view acquired through {@link #asByteChannel()} or {@link #asImageInputStream()} in use once
     * this method is closed becomes undetermined.
     */
    @Override
    public void close() throws IOException;

    /**
     * Returns a {@link SeekableByteChannel} view of this {@code RangeReader}.
     *
     * <p>Multiple calls to this method can be performed during the life time of the {@code RangeReader}.
     *
     * <p>The returned channel <strong>does not</strong> close the {@code RangeReader}. It is safe to call this method
     * several times and from multiple threads.
     *
     * @return a SeekableByteChannel view of this RangeReader
     */
    default SeekableByteChannel asByteChannel() {
        return RangeReaderSeekableByteChannel.of(this);
    }
    /**
     * Returns a {@link ImageInputStream} view of this {@code RangeReader}.
     *
     * <p>Multiple calls to this method can be performed during the life time of the {@code RangeReader}.
     *
     * <p>The returned input stream <strong>does not</strong> close the {@code RangeReader}
     *
     * @return an ImageInputStream view of this RangeReader
     */
    default ImageInputStream asImageInputStream() {
        return SeekableByteChannelImageInputStream.of(asByteChannel());
    }
}
