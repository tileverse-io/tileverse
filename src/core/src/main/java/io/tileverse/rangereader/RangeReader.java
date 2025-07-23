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
package io.tileverse.rangereader;

import io.tileverse.imageio.SeekableByteChannelImageInputStream;
import io.tileverse.rangereader.nio.ByteBufferPool;
import io.tileverse.rangereader.nio.channels.RangeReaderSeekableByteChannel;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import javax.imageio.stream.ImageInputStream;

/**
 * Interface for reading ranges of bytes from a source.
 * <p>
 * This abstraction allows data to be read from various sources such as local
 * files, HTTP servers, or cloud storage using range requests.
 * <p>
 * All implementations of this interface MUST be thread-safe to allow concurrent
 * access from multiple threads without interference. This is especially
 * important in server environments where multiple requests may
 * be accessing the same reader.
 */
public interface RangeReader extends Closeable {

    /**
     * Reads bytes from the source at the specified offset.
     * <p>
     * This convenience method allocates a new ByteBuffer for each call. For better
     * performance and reduced garbage collection pressure, prefer using
     * {@link #readRange(long, int, ByteBuffer)} with reusable buffers when possible
     * (see {@link ByteBufferPool}).
     *
     * @param offset The offset to read from
     * @param length The number of bytes to read
     * @return A ByteBuffer containing the read bytes, positioned for immediate
     *         consumption
     * @throws IOException              If an I/O error occurs
     * @throws IllegalArgumentException If offset or length is negative
     */
    default ByteBuffer readRange(long offset, int length) throws IOException {
        // Allocate a new buffer and use our optimized implementation
        ByteBuffer buffer = ByteBuffer.allocate(length);
        int bytesRead = readRange(offset, length, buffer);
        assert bytesRead <= length;
        assert bytesRead == buffer.remaining();
        return buffer;
    }

    /**
     * Reads bytes from the source at the specified offset into the provided target
     * buffer.
     * <p>
     * This method allows callers to reuse ByteBuffers to reduce garbage collection
     * pressure. The target buffer will be prepared for immediate consumption with
     * its position unchanged and limit set to position + bytes read. The caller is
     * responsible for ensuring that the target buffer has sufficient remaining
     * capacity for the requested length.
     *
     * @param offset The offset to read from
     * @param length The number of bytes to read
     * @param target The ByteBuffer to read into, starting at its current position
     * @return The number of bytes actually read
     * @throws IOException                      If an I/O error occurs
     * @throws IllegalArgumentException         If offset or length is negative, or
     *                                          if target is null
     * @throws IllegalArgumentException         If target has insufficient remaining
     *                                          capacity
     * @throws java.nio.ReadOnlyBufferException If the target buffer is read-only
     */
    int readRange(long offset, int length, ByteBuffer target) throws IOException;

    /**
     * Gets the total size of the source in bytes.
     * <p>
     * Implementations MUST ensure this method is thread-safe.
     *
     * @return The size in bytes
     * @throws IOException If an I/O error occurs
     */
    long size() throws IOException;

    /**
     * Gets a unique identifier for the source being read.
     * <p>
     * This identifier is used for caching and debugging purposes. For base readers,
     * this typically returns the natural identifier (file path, URL, etc.). For
     * decorator readers, this should include information about the decoration
     * (e.g., {@literal memory-cached:file:///path/to/file.dat}).
     *
     * @return A unique identifier for this source
     */
    String getSourceIdentifier();

    /**
     * Returns a {@link SeekableByteChannel} view of this {@code RangeReader}.
     * <p>
     * Multiple calls to this method can be performed during the life time of the {@code RangeReader}.
     * <p>
     * The returned channel <strong>does not</strong> close the {@code RangeReader}
     *
     * @return a SeekableByteChannel view of this RangeReader
     */
    default SeekableByteChannel asByteChannel() {
        return RangeReaderSeekableByteChannel.of(this);
    }

    /**
     * Returns a {@link ImageInputStream} view of this {@code RangeReader}.
     * <p>
     * Multiple calls to this method can be performed during the life time of the {@code RangeReader}.
     * <p>
     * The returned input stream <strong>does not</strong> close the {@code RangeReader}
     *
     * @return an ImageInputStream view of this RangeReader
     */
    default ImageInputStream asImageInputStream() {
        return SeekableByteChannelImageInputStream.of(asByteChannel());
    }
}
