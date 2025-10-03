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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SelectableChannel;
import java.util.Objects;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageInputStreamImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ImageInputStream} implementation that wraps a
 * {@link SeekableByteChannel} to provide ImageIO integration with efficient
 * seeking capabilities.
 * <p>
 * This adapter allows SeekableByteChannel instances to be used with Java's
 * ImageIO framework, providing both big-endian and little-endian byte order
 * support through the standard ImageInputStream interface. It's particularly
 * useful for reading image data from cloud storage, HTTP sources, or any other
 * channel-based data source.
 * <p>
 * <strong>Thread Safety:</strong> This class is not thread-safe. External
 * synchronization is required if multiple threads access the same instance
 * concurrently.
 * <p>
 * <strong>Seeking Performance:</strong> This implementation uses the channel's
 * seeking capability for efficient positioning, making it suitable for
 * random-access image formats and large image files.
 * <p>
 * <strong>Lifecycle Management:</strong> This adapter does not take ownership
 * of the underlying channel. Closing this adapter will not close the underlying
 * channel. Note that while multiple adapters could technically share the same
 * channel, this is not recommended as they would interfere with each other's
 * position state.
 * <p>
 * <strong>Position Synchronization:</strong> This implementation ensures that
 * the channel position and stream position are always synchronized by reading
 * exactly the requested number of bytes without internal buffering.
 *
 * <p>
 * <strong>Usage Example:</strong>
 * </p>
 *
 * <pre>{@code
 * try (SeekableByteChannel channel = Files.newByteChannel(imagePath, StandardOpenOption.READ)) {
 * 	ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel);
 *
 * 	// Use with ImageIO readers
 * 	Iterator<ImageReader> readers = ImageIO.getImageReaders(imageStream);
 * 	if (readers.hasNext()) {
 * 		ImageReader reader = readers.next();
 * 		reader.setInput(imageStream);
 * 		BufferedImage image = reader.read(0);
 * 	}
 * }
 * }</pre>
 *
 * <p>
 * <strong>Byte Order Configuration:</strong>
 * </p>
 *
 * <pre>{@code
 * try (SeekableByteChannel channel = openChannel()) {
 * 	ImageInputStream imageStream = SeekableByteChannelImageInputStream.of(channel);
 *
 * 	// Configure for little-endian data (e.g., TIFF with Intel byte order)
 * 	imageStream.setByteOrder(ByteOrder.LITTLE_ENDIAN);
 *
 * 	// Read primitive types in the configured byte order
 * 	int header = imageStream.readInt();
 * 	short version = imageStream.readShort();
 * }
 * }</pre>
 */
public final class SeekableByteChannelImageInputStream extends ImageInputStreamImpl {

    private static final Logger logger = LoggerFactory.getLogger(SeekableByteChannelImageInputStream.class);

    /** The underlying SeekableByteChannel that provides the data source. */
    private final SeekableByteChannel channel;

    /** Reusable single-byte buffer for {@link #read()} to reduce GC pressure. */
    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

    /**
     * Creates a new ImageInputStream adapter for the given SeekableByteChannel.
     *
     * @param channel the SeekableByteChannel to wrap
     * @throws IllegalArgumentException if channel is null
     */
    private SeekableByteChannelImageInputStream(SeekableByteChannel channel) {
        this.channel = Objects.requireNonNull(channel, "channel cannot be null");

        // Warn if channel is SelectableChannel as it might switch to non-blocking mode
        if (channel instanceof SelectableChannel) {
            logger.warn("Channel is a SelectableChannel and may switch to non-blocking mode, "
                    + "which could cause compatibility issues with ImageInputStream blocking semantics. "
                    + "Ensure the channel remains in blocking mode for proper operation.");
        }
    }

    /**
     * Creates a new ImageInputStream adapter for the given SeekableByteChannel.
     *
     * @param channel the SeekableByteChannel to wrap
     * @return a new ImageInputStream adapter
     * @throws IllegalArgumentException if channel is null
     */
    public static SeekableByteChannelImageInputStream of(SeekableByteChannel channel) {
        return new SeekableByteChannelImageInputStream(channel);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Reads a single byte from the underlying channel.
     *
     * @return the next byte, or -1 if the end of the stream is reached
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read() throws IOException {
        checkClosed();

        // Read exactly 1 byte
        singleByteBuffer.clear(); // Reset position and limit for reuse
        int bytesRead = channel.read(singleByteBuffer);

        if (bytesRead <= 0) {
            warnIfZeroBytesRead(bytesRead);
            return -1; // End of stream or no bytes available
        }

        // Update streamPos to match channel position
        super.streamPos = channel.position();

        return singleByteBuffer.get(0) & 0xFF;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Reads bytes into the provided array.
     *
     * @param b   the byte array to read into
     * @param off the offset in the array
     * @param len the number of bytes to read
     * @return the number of bytes read, or -1 if end of stream
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        Objects.requireNonNull(b, "byte array cannot be null");
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException("off=" + off + ", len=" + len + ", array.length=" + b.length);
        }

        if (len == 0) {
            return 0;
        }

        // Read exactly the requested number of bytes
        ByteBuffer requestBuffer = ByteBuffer.wrap(b, off, len);
        int bytesRead = channel.read(requestBuffer);

        if (bytesRead == -1) {
            return -1; // End of stream
        }

        warnIfZeroBytesRead(bytesRead);

        // Update streamPos to match channel position (even if 0 bytes read)
        super.streamPos = channel.position();

        return bytesRead; // Return actual bytes read (could be 0)
    }

    private void warnIfZeroBytesRead(int bytesRead) {
        if (bytesRead == 0 && channel instanceof SelectableChannel) {
            SelectableChannel selectableChannel = (SelectableChannel) channel;
            if (!selectableChannel.isBlocking()) {
                logger.warn("SelectableChannel returned 0 bytes and is in non-blocking mode. "
                        + "This may cause ImageInputStream to behave unexpectedly. "
                        + "Consider using blocking mode for proper ImageInputStream semantics.");
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Seeks to the specified position in the stream. This implementation uses the
     * channel's seeking capability for efficient positioning.
     *
     * @param pos the position to seek to
     * @throws IOException if an I/O error occurs or pos is negative
     */
    @Override
    public void seek(long pos) throws IOException {
        checkClosed();

        // Check for negative position first (our custom validation)
        if (pos < 0) {
            throw new IOException("seek position cannot be negative: " + pos);
        }

        // Validate position against flushedPos (following FileImageInputStream pattern)
        if (pos < flushedPos) {
            throw new IndexOutOfBoundsException("pos < flushedPos!");
        }

        // Reset bit offset (required by ImageInputStream contract)
        bitOffset = 0;

        // Seek to new position in channel
        channel.position(pos);

        // Update stream position to match channel position
        super.streamPos = channel.position();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns the length of the stream if known.
     *
     * @return the length of the stream, or -1 if unknown
     */
    @Override
    public long length() {
        if (channel == null) {
            return -1;
        }
        try {
            return channel.size();
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Closes this stream. The underlying channel is not closed since this adapter
     * does not take ownership of it.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        super.close();
        // Do not close the channel - we don't own it
    }

    @Override
    public String toString() {
        return "SeekableByteChannelImageInputStream[channel=%s, position=%d, size=%d, byteOrder=%s]"
                .formatted(channel, streamPos, length(), byteOrder);
    }
}
