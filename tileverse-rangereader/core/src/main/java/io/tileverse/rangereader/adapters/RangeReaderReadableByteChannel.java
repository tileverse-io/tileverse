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
package io.tileverse.rangereader.adapters;

import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ReadableByteChannel} adapter that wraps a {@link RangeReader} to provide
 * standard Java NIO channel semantics for sequential reading.
 * <p>
 * This adapter allows RangeReader instances to be used with any API that expects
 * a ReadableByteChannel, enabling integration with standard Java NIO operations,
 * file systems, and frameworks that work with channels.
 * <p>
 * <strong>Thread Safety:</strong> This class is thread-safe if the underlying
 * RangeReader is thread-safe. The position state is managed internally with
 * proper synchronization.
 * <p>
 * <strong>Read-Only Channel:</strong> This channel is read-only. It provides sequential
 * access to the underlying RangeReader starting from position 0.
 * <p>
 * <strong>Lifecycle Management:</strong> This channel does not take ownership of the
 * underlying RangeReader. Closing this channel will not close the underlying RangeReader.
 * Note that while multiple channels could technically share the same RangeReader,
 * this is not recommended as they would interfere with each other's position state.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * RangeReader reader = FileRangeReader.of(path);
 * try (ReadableByteChannel channel = RangeReaderReadableByteChannel.of(reader)) {
 *     // Use with any NIO-based API that expects ReadableByteChannel
 *     ByteBuffer buffer = ByteBuffer.allocate(1024);
 *     int bytesRead = channel.read(buffer);
 *
 *     // Continue reading sequentially
 *     buffer.clear();
 *     bytesRead = channel.read(buffer);
 * }
 * }</pre>
 */
public class RangeReaderReadableByteChannel implements ReadableByteChannel {

    /** The underlying RangeReader that provides the data source. */
    protected final RangeReader rangeReader;

    /** Atomic flag indicating whether this channel is open. */
    protected final AtomicBoolean open = new AtomicBoolean(true);

    /** Current read position in the source, managed atomically for thread safety. */
    protected final AtomicLong position = new AtomicLong(0);

    /**
     * Creates a new ReadableByteChannel adapter for the given RangeReader.
     *
     * @param rangeReader the RangeReader to wrap
     * @throws IllegalArgumentException if rangeReader is null
     */
    protected RangeReaderReadableByteChannel(RangeReader rangeReader) {
        this.rangeReader = Objects.requireNonNull(rangeReader, "rangeReader cannot be null");
    }

    /**
     * Creates a new ReadableByteChannel adapter for the given RangeReader.
     *
     * @param rangeReader the RangeReader to wrap
     * @return a new ReadableByteChannel adapter
     * @throws IllegalArgumentException if rangeReader is null
     */
    public static RangeReaderReadableByteChannel of(RangeReader rangeReader) {
        return new RangeReaderReadableByteChannel(rangeReader);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Reads bytes from the current position in the underlying RangeReader into the
     * provided buffer. The position is advanced by the number of bytes read.
     *
     * @param dst the buffer to read into
     * @return the number of bytes read, or -1 if the end of the source is reached
     * @throws ClosedChannelException if this channel is closed
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        ensureOpen();
        Objects.requireNonNull(dst, "destination buffer cannot be null");

        if (!dst.hasRemaining()) {
            return 0;
        }

        final long currentPosition = position.get();
        final OptionalLong sourceSize = rangeReader.size();

        // Check if we're at or beyond the end of the source
        if (sourceSize.isPresent() && currentPosition >= sourceSize.getAsLong()) {
            return -1;
        }

        // Calculate how many bytes we can actually read
        final int requestedBytes = dst.remaining();
        final long availableBytes = sourceSize.isEmpty() ? requestedBytes : (sourceSize.getAsLong() - currentPosition);
        final int bytesToRead = (int) Math.min(requestedBytes, availableBytes);

        if (bytesToRead <= 0) {
            return -1;
        }

        // Read from the RangeReader
        final int bytesRead = rangeReader.readRange(currentPosition, bytesToRead, dst);

        // Advance the position
        position.addAndGet(bytesRead);

        return bytesRead;
    }

    /**
     * Returns the current position in the source. The position is advanced
     * by read operations.
     *
     * @return the current position
     * @throws ClosedChannelException if this channel is closed
     * @throws IOException if an I/O error occurs
     */
    public long position() throws IOException {
        ensureOpen();
        return position.get();
    }

    /**
     * Returns the size of the underlying source from the RangeReader.
     *
     * @return the size of the source in bytes, or {@code -1} if uknown
     * @throws ClosedChannelException if this channel is closed
     * @throws IOException if an I/O error occurs
     */
    public long size() throws IOException {
        ensureOpen();
        OptionalLong size = rangeReader.size();
        return size.isPresent() ? size.getAsLong() : -1L;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns whether this channel is currently open.
     *
     * @return true if this channel is open, false otherwise
     */
    @Override
    public boolean isOpen() {
        return open.get();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Closes this channel. After calling this method, all other operations on this
     * channel will throw {@link ClosedChannelException}.
     * <p>
     * <strong>Note:</strong> This method does not close the underlying RangeReader,
     * as this channel does not take ownership of it. The RangeReader should be
     * closed separately by the caller if needed.
     *
     * @throws IOException this implementation never throws IOException
     */
    @Override
    public void close() throws IOException {
        open.set(false);
    }

    /**
     * Gets the underlying RangeReader that this channel wraps.
     * <p>
     * This method provides access to the underlying RangeReader for cases where
     * RangeReader-specific functionality is needed. Care should be taken when
     * using the returned RangeReader directly, as it may affect the state of
     * this channel.
     *
     * @return the underlying RangeReader
     * @throws ClosedChannelException if this channel is closed
     */
    public RangeReader getRangeReader() throws ClosedChannelException {
        ensureOpen();
        return rangeReader;
    }

    /**
     * Gets the source identifier from the underlying RangeReader.
     * <p>
     * This is a convenience method that delegates to the underlying RangeReader's
     * {@link RangeReader#getSourceIdentifier()} method.
     *
     * @return the source identifier
     * @throws ClosedChannelException if this channel is closed
     */
    public String getSourceIdentifier() throws ClosedChannelException {
        ensureOpen();
        return rangeReader.getSourceIdentifier();
    }

    /**
     * Ensures that this channel is open.
     *
     * @throws ClosedChannelException if this channel is closed
     */
    protected void ensureOpen() throws ClosedChannelException {
        if (!open.get()) {
            throw new ClosedChannelException();
        }
    }

    @Override
    public String toString() {
        if (!open.get()) {
            return "RangeReaderReadableByteChannel[closed]";
        }

        try {
            return String.format(
                    "RangeReaderReadableByteChannel[source=%s, position=%d, size=%d]",
                    rangeReader.getSourceIdentifier(),
                    position.get(),
                    rangeReader.size().orElse(-1));
        } catch (IOException e) {
            return String.format(
                    "RangeReaderReadableByteChannel[source=%s, position=%d, size=unknown]",
                    rangeReader.getSourceIdentifier(), position.get());
        }
    }
}
