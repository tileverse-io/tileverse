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
package io.tileverse.io;

import java.io.DataInput;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/**
 * A {@link DataInput} adapter that wraps a {@link SeekableByteChannel} to provide
 * standard Java DataInput semantics with optimized seeking capabilities.
 * <p>
 * This adapter provides the same functionality as {@link ReadableByteChannelDataInput}
 * but with an optimized {@link #skipBytes(int)} implementation that uses the channel's
 * seeking capability instead of reading and discarding data, making it much more
 * efficient for large skip operations.
 * <p>
 * <strong>Thread Safety:</strong> This class is not thread-safe. External synchronization
 * is required if multiple threads access the same instance concurrently.
 * <p>
 * <strong>Read-Only Adapter:</strong> This adapter only provides reading functionality.
 * It maintains an internal buffer for efficient reading of primitive types.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
 *     DataInput dataInput = SeekableByteChannelDataInput.of(channel);
 *
 *     // Read header at the beginning
 *     int version = dataInput.readInt();
 *
 *     // Efficiently skip to data section (no actual reading)
 *     dataInput.skipBytes(1024);
 *
 *     // Read data after the skip
 *     double value = dataInput.readDouble();
 * }
 * }</pre>
 *
 * @see ReadableByteChannelDataInput
 */
public final class SeekableByteChannelDataInput extends ReadableByteChannelDataInput implements DataInput {

    /**
     * Creates a new DataInput adapter for the given SeekableByteChannel.
     *
     * @param channel the SeekableByteChannel to wrap
     * @throws IllegalArgumentException if channel is null
     */
    private SeekableByteChannelDataInput(SeekableByteChannel channel) {
        super(channel);
    }

    /**
     * Creates a new DataInput adapter for the given SeekableByteChannel with specified buffer size.
     *
     * @param channel the SeekableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    private SeekableByteChannelDataInput(SeekableByteChannel channel, int bufferSize) {
        super(channel, bufferSize);
    }

    /**
     * Creates a new DataInput adapter for the given SeekableByteChannel.
     *
     * @param channel the SeekableByteChannel to wrap
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null
     */
    public static SeekableByteChannelDataInput of(SeekableByteChannel channel) {
        return new SeekableByteChannelDataInput(channel);
    }

    /**
     * Creates a new DataInput adapter for the given SeekableByteChannel with specified buffer size.
     *
     * @param channel the SeekableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    public static SeekableByteChannelDataInput of(SeekableByteChannel channel, int bufferSize) {
        return new SeekableByteChannelDataInput(channel, bufferSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableByteChannel channel() {
        return (SeekableByteChannel) super.channel();
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation uses the channel's seeking capability to efficiently skip
     * over data without reading it, making it much faster than reading and discarding
     * for large skip operations.
     *
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int skipBytes(int n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        // Calculate the current logical position (accounting for buffered data)
        final long currentLogicalPosition = position();
        final long channelSize = channel().size();

        // Calculate how many bytes we can actually skip
        final long maxSkip = channelSize - currentLogicalPosition;
        final int actualSkip = (int) Math.min(n, maxSkip);

        if (actualSkip <= 0) {
            return 0;
        }

        // Clear the buffer since we're changing position
        buffer.clear();
        buffer.limit(0);

        // Seek to new logical position
        channel().position(currentLogicalPosition + actualSkip);

        return actualSkip;
    }

    /**
     * Gets the current logical position for reading, accounting for buffered data.
     *
     * @return the current logical position in bytes
     * @throws IOException if an I/O error occurs
     */
    public long position() throws IOException {
        // The logical position is the channel position minus any remaining buffer data
        return channel().position() - buffer.remaining();
    }

    /**
     * Gets the size of the channel.
     *
     * @return the size of the channel in bytes
     * @throws IOException if an I/O error occurs
     */
    public long size() throws IOException {
        return channel().size();
    }

    @Override
    public String toString() {
        try {
            return String.format(
                    "SeekableByteChannelDataInput[channel=%s, position=%d, size=%d, bufferSize=%d, bufferRemaining=%d]",
                    channel, channel().position(), channel().size(), buffer.capacity(), buffer.remaining());
        } catch (IOException e) {
            return String.format(
                    "SeekableByteChannelDataInput[channel=%s, position=unknown, size=unknown, bufferSize=%d, bufferRemaining=%d]",
                    channel, buffer.capacity(), buffer.remaining());
        }
    }
}
