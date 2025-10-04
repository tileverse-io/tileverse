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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class providing common implementation for {@link RangeReader}.
 * <p>
 * This class implements the core range reading logic with parameter validation,
 * boundary checking, and buffer management. Concrete implementations only need
 * to override {@link #readRangeNoFlip(long, int, ByteBuffer)} to provide the
 * actual data reading mechanism.
 * <p>
 * The template method pattern is used where {@link #readRange(long, int, ByteBuffer)}
 * handles all the common concerns (validation, EOF handling, buffer preparation)
 * and delegates the actual reading to the abstract {@code readRangeNoFlip} method.
 */
@Slf4j
public abstract class AbstractRangeReader implements RangeReader {

    /**
     * Creates a new AbstractRangeReader.
     * <p>
     * This constructor is provided for subclasses to extend. Subclasses should
     * implement the {@link #readRangeNoFlip(long, int, ByteBuffer)} method to
     * provide the actual reading implementation.
     */
    protected AbstractRangeReader() {
        // Default constructor for subclasses
    }

    /**
     * Reads bytes from the source at the specified offset into the provided target buffer.
     * <p>
     * This method provides the core implementation of range reading with comprehensive
     * parameter validation, boundary checking, and buffer management following standard
     * NIO conventions. It delegates the actual data reading to {@link #readRangeNoFlip(long, int, ByteBuffer)}.
     * <p>
     * <strong>Parameter Validation:</strong>
     * <ul>
     * <li>Validates that offset and length are non-negative</li>
     * <li>Ensures target buffer is not null and not read-only</li>
     * <li>Checks that target buffer has sufficient remaining capacity</li>
     * </ul>
     * <p>
     * <strong>Boundary Handling:</strong>
     * <ul>
     * <li>For zero-length reads, returns immediately with 0 bytes read</li>
     * <li>For reads starting beyond EOF, returns 0 bytes read</li>
     * <li>For reads extending beyond EOF, truncates to available data</li>
     * </ul>
     * <p>
     * <strong>Buffer Management (NIO Conventions):</strong>
     * Following standard Java NIO conventions, this method writes data into the buffer
     * and advances the position:
     * <ul>
     * <li>Data is written starting at the buffer's current position</li>
     * <li>Position is advanced by the number of bytes written</li>
     * <li>Limit remains unchanged</li>
     * <li>Caller must call {@code flip()} to prepare buffer for reading</li>
     * </ul>
     * <p>
     * <strong>Thread Safety:</strong>
     * This method is thread-safe when the underlying {@link #readRangeNoFlip(long, int, ByteBuffer)}
     * implementation is thread-safe.
     *
     * @param offset The byte offset to read from (must be >= 0)
     * @param length The number of bytes to read (must be >= 0)
     * @param target The ByteBuffer to read into, starting at its current position
     * @return The number of bytes actually read (may be less than requested if EOF is reached)
     * @throws IOException                      If an I/O error occurs during reading
     * @throws IllegalArgumentException         If offset or length is negative, target is null,
     *                                          or target has insufficient remaining capacity
     * @throws java.nio.ReadOnlyBufferException If the target buffer is read-only
     */
    @Override
    public final int readRange(long offset, int length, ByteBuffer target) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative");
        }
        if (target == null) {
            throw new IllegalArgumentException("Target buffer cannot be null");
        }
        if (target.isReadOnly()) {
            throw new java.nio.ReadOnlyBufferException();
        }

        if (length == 0) {
            // For zero-length reads, return 0 bytes read (position unchanged)
            return 0;
        }

        final int remainingBefore = target.remaining();

        // Check if target has enough remaining space
        if (remainingBefore < length) {
            throw new IllegalArgumentException(
                    "Target buffer has insufficient remaining capacity: " + remainingBefore + " < " + length);
        }

        int actualLength = length;

        final OptionalLong fileSize = size();
        if (fileSize.isPresent()) {
            long size = fileSize.getAsLong();
            if (offset >= size) {
                // Offset is beyond EOF, return 0 bytes read (position unchanged)
                return 0;
            }
            if (offset + length > size) {
                // Read extends beyond EOF, truncate it
                actualLength = (int) (size - offset);
            }
        }

        final int readCount = readRangeNoFlip(offset, actualLength, target);

        return readCount;
    }

    /**
     * Reads bytes from the source into the target buffer without preparing the buffer for consumption.
     * <p>
     * This is the core template method that concrete implementations must override to provide
     * the actual data reading logic. Unlike {@link #readRange(long, int, ByteBuffer)}, this
     * method does not perform parameter validation or buffer preparation - those responsibilities
     * are handled by the calling {@code readRange} method.
     * <p>
     * <strong>Implementation Requirements:</strong>
     * <ul>
     * <li>Read exactly {@code actualLength} bytes from the source at {@code offset}</li>
     * <li>Write the data into {@code target} starting at its current position</li>
     * <li>Advance the target buffer's position by the number of bytes written</li>
     * <li>Do NOT modify the target buffer's limit</li>
     * <li>Return the actual number of bytes read (may be less than requested if EOF reached)</li>
     * <li>Must be thread-safe for concurrent access</li>
     * </ul>
     * <p>
     * <strong>Parameter Guarantees:</strong>
     * When this method is called by {@link #readRange(long, int, ByteBuffer)}, the following
     * conditions are guaranteed:
     * <ul>
     * <li>{@literal offset >= 0}</li>
     * <li>{@literal actualLength > 0} (zero-length reads are handled by the caller)</li>
     * <li>{@code target} is not null and not read-only</li>
     * <li>{@literal target.remaining() >= actualLength}</li>
     * <li>{@literal offset < source.size()} (reads starting beyond EOF are handled by caller)</li>
     * <li>{@code actualLength} may be less than originally requested if the read would extend beyond EOF</li>
     * </ul>
     * <p>
     * <strong>Buffer State Contract:</strong>
     * <pre>
     * {@code
     * // Before calling readRangeNoFlip:
     * int initialPosition = target.position();
     * int initialLimit = target.limit();
     *
     * // After readRangeNoFlip returns:
     * int bytesRead = readRangeNoFlip(offset, actualLength, target);
     * assert target.position() == initialPosition + bytesRead;
     * assert target.limit() == initialLimit; // unchanged
     * }
     * </pre>
     * <p>
     * The calling {@code readRange} method will then reset the position and adjust the limit
     * to prepare the buffer for immediate consumption by the caller.
     *
     * @param offset       The byte offset to read from (guaranteed to be {@literal >= 0 and < source size})
     * @param actualLength The number of bytes to read (guaranteed to be {@literal > 0} and within buffer capacity)
     * @param target       The ByteBuffer to write into (guaranteed to be non-null, writable, and have sufficient capacity)
     * @return The number of bytes actually read and written to the target buffer
     * @throws IOException If an I/O error occurs during reading from the underlying source
     */
    protected abstract int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException;
}
