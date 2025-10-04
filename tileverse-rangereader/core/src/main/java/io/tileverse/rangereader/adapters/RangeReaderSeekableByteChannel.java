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
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;

/**
 * A {@link SeekableByteChannel} adapter that wraps a {@link RangeReader} to provide
 * standard Java NIO channel semantics with seeking capabilities.
 * <p>
 * This adapter extends {@link RangeReaderReadableByteChannel} to add seeking functionality,
 * allowing RangeReader instances to be used with any API that expects a SeekableByteChannel,
 * enabling integration with standard Java NIO operations, file systems, and frameworks
 * that work with seekable channels.
 * <p>
 * <strong>Thread Safety:</strong> This class is thread-safe if the underlying
 * RangeReader is thread-safe. The position state is managed internally with
 * proper synchronization.
 * <p>
 * <strong>Read-Only Channel:</strong> This channel is read-only. All write operations
 * will throw {@link NonWritableChannelException}.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * RangeReader reader = FileRangeReader.of(path);
 * try (SeekableByteChannel channel = RangeReaderSeekableByteChannel.of(reader)) {
 *     // Use with any NIO-based API
 *     ByteBuffer buffer = ByteBuffer.allocate(1024);
 *     int bytesRead = channel.read(buffer);
 *
 *     // Seek to a specific position
 *     channel.position(1000);
 *     buffer.clear();
 *     bytesRead = channel.read(buffer);
 * }
 * }</pre>
 */
public final class RangeReaderSeekableByteChannel extends RangeReaderReadableByteChannel
        implements SeekableByteChannel {

    /**
     * Creates a new SeekableByteChannel adapter for the given RangeReader.
     *
     * @param rangeReader the RangeReader to wrap
     * @throws IllegalArgumentException if rangeReader is null
     */
    private RangeReaderSeekableByteChannel(RangeReader rangeReader) {
        super(rangeReader);
    }

    /**
     * Creates a new SeekableByteChannel adapter for the given RangeReader.
     *
     * @param rangeReader the RangeReader to wrap
     * @return a new SeekableByteChannel adapter
     * @throws IllegalArgumentException if rangeReader is null
     */
    public static RangeReaderSeekableByteChannel of(RangeReader rangeReader) {
        return new RangeReaderSeekableByteChannel(rangeReader);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>This operation is not supported.</strong> RangeReader is read-only.
     *
     * @param src the buffer containing data to write
     * @return never returns normally
     * @throws NonWritableChannelException always, as this channel is read-only
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets the current position in the source. Subsequent read operations will
     * start from this position.
     *
     * @param newPosition the new position, must be non-negative
     * @return this channel
     * @throws ClosedChannelException if this channel is closed
     * @throws IllegalArgumentException if newPosition is negative
     * @throws IOException if an I/O error occurs
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        ensureOpen();
        if (newPosition < 0) {
            throw new IllegalArgumentException("position cannot be negative: " + newPosition);
        }

        position.set(newPosition);
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>This operation is not supported.</strong> RangeReader sources cannot be truncated.
     *
     * @param size the new size
     * @return never returns normally
     * @throws NonWritableChannelException always, as this channel is read-only
     */
    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public String toString() {
        if (!open.get()) {
            return "RangeReaderSeekableByteChannel[closed]";
        }

        try {
            return String.format(
                    "RangeReaderSeekableByteChannel[source=%s, position=%d, size=%d]",
                    rangeReader.getSourceIdentifier(),
                    position.get(),
                    rangeReader.size().orElse(-1));
        } catch (IOException e) {
            return String.format(
                    "RangeReaderSeekableByteChannel[source=%s, position=%d, size=unknown]",
                    rangeReader.getSourceIdentifier(), position.get());
        }
    }
}
