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
package io.tileverse.rangereader.file;

import io.tileverse.rangereader.AbstractRangeReader;
import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * A thread-safe file-based implementation of {@link RangeReader} that provides efficient random access to local files.
 *
 * <p>This implementation uses NIO {@link FileChannel} with position-based reads ({@link FileChannel#read(ByteBuffer, long)})
 * to ensure thread safety. Unlike traditional stream-based file access, position-based reads allow multiple threads
 * to read from different parts of the same file concurrently without interference, as each read operation specifies
 * its absolute position in the file.
 *
 * <h2>Thread Safety</h2>
 * <p>This class is fully thread-safe for concurrent read operations. The underlying {@link FileChannel} supports
 * simultaneous reads from multiple threads as long as each operation uses absolute positioning, which this
 * implementation guarantees.
 *
 * <h2>Performance Characteristics</h2>
 * <p>FileRangeReader provides excellent performance for random access patterns typical in tiled data access:
 * <ul>
 * <li>Zero-copy operations where possible through direct ByteBuffer usage</li>
 * <li>Efficient random access without seek overhead</li>
 * <li>OS-level caching benefits for frequently accessed file regions</li>
 * <li>No synchronization overhead between concurrent read operations</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create reader for a PMTiles file
 * Path pmtilesFile = Paths.get("data/world.pmtiles");
 * try (FileRangeReader reader = FileRangeReader.of(pmtilesFile)) {
 *     // Read tile data from different threads concurrently
 *     ByteBuffer tileData = reader.readRange(offset, length);
 * }
 * }</pre>
 *
 * @see FileChannel#read(ByteBuffer, long)
 */
public class FileRangeReader extends AbstractRangeReader implements RangeReader {

    private final FileChannel channel;
    private final Path path;

    /**
     * Creates a new FileRangeReader for the specified file path.
     *
     * <p>Opens the file with read-only access using {@link StandardOpenOption#READ}. The file
     * must exist and be readable, otherwise an {@link IOException} will be thrown.
     *
     * @param path the path to the file to read from (must not be null)
     * @throws IOException if the file cannot be opened for reading (e.g., file doesn't exist,
     *                     insufficient permissions, or other I/O errors)
     * @throws NullPointerException if path is null
     */
    public FileRangeReader(Path path) throws IOException {
        Objects.requireNonNull(path, "Path cannot be null");
        this.path = path;
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
    }

    /**
     * Performs the actual range read operation using thread-safe positioned reads.
     *
     * <p>This method implements the core reading logic using {@link FileChannel#read(ByteBuffer, long)}
     * which provides thread-safe access by specifying absolute file positions. Multiple threads can
     * call this method concurrently without synchronization, as each read operation is independent
     * and doesn't affect the channel's position.
     *
     * <p>The method handles partial reads that may occur with large requests or when approaching
     * end-of-file, ensuring the requested range is read completely or until EOF is reached.
     *
     * @param offset the absolute position in the file to start reading from
     * @param actualLength the number of bytes to read
     * @param target the ByteBuffer to read data into (limit will be adjusted)
     * @return the actual number of bytes read, which may be less than actualLength if EOF is reached
     * @throws IOException if an I/O error occurs during reading
     */
    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException {

        // Following NIO conventions: position is advanced, limit remains unchanged
        // We need to track the initial position and limit for restoration
        final int initialPosition = target.position();
        final int initialLimit = target.limit();

        // Temporarily set limit to prevent reading beyond the intended range
        target.limit(initialPosition + actualLength);

        // Read until buffer is full or end of file
        int totalRead = 0;
        long currentPosition = offset;

        while (totalRead < actualLength) {
            // Use the position-based read method for thread safety
            // This allows concurrent reads without interference from other threads
            int read = channel.read(target, currentPosition);
            if (read == -1) {
                // End of file reached
                break;
            }
            totalRead += read;
            currentPosition += read;
        }

        // Restore original limit (NIO contract: limit remains unchanged)
        target.limit(initialLimit);

        // Position is now at initialPosition + totalRead (NIO contract: position advanced by bytes written)
        return totalRead;
    }

    /**
     * Returns the size of the file in bytes.
     *
     * <p>This method is thread-safe as {@link FileChannel#size()} is inherently thread-safe
     * and doesn't modify any shared state.
     *
     * @return the size of the file in bytes
     * @throws IOException if an I/O error occurs while determining the file size
     */
    @Override
    public OptionalLong size() throws IOException {
        return OptionalLong.of(channel.size());
    }

    /**
     * Returns a string identifier for this file source.
     *
     * <p>The identifier is the absolute path of the file, which uniquely identifies
     * the data source for logging, caching, and debugging purposes.
     *
     * @return the absolute path of the file as a string
     */
    @Override
    public String getSourceIdentifier() {
        return path.toAbsolutePath().toString();
    }

    /**
     * Closes the underlying file channel and releases any associated system resources.
     *
     * <p>This method is thread-safe and idempotent - it can be called multiple times
     * without harm. After closing, any further attempts to read from this FileRangeReader
     * will result in a {@link java.nio.channels.ClosedChannelException}.
     *
     * <p>It is recommended to use this FileRangeReader in a try-with-resources statement
     * to ensure proper resource cleanup.
     *
     * @throws IOException if an I/O error occurs while closing the channel
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * Creates a new builder for constructing FileRangeReader instances.
     *
     * <p>The builder pattern provides a flexible way to configure FileRangeReader
     * construction with various path specification methods (Path, String, URI).
     *
     * @return a new builder instance for configuring FileRangeReader construction
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a new FileRangeReader for the specified file path.
     * <p>
     * This is a convenience method equivalent to:
     * {@code FileRangeReader.builder().path(path).build()}
     *
     * @param path the file path
     * @return a new FileRangeReader instance
     * @throws IOException if an error occurs during construction
     */
    public static FileRangeReader of(Path path) throws IOException {
        return builder().path(path).build();
    }

    /**
     * Builder for FileRangeReader.
     */
    public static class Builder {
        private Path path;

        private Builder() {}

        /**
         * Sets the file path.
         *
         * @param path the file path
         * @return this builder
         */
        public Builder path(Path path) {
            this.path = Objects.requireNonNull(path, "Path cannot be null");
            return this;
        }

        /**
         * Sets the file path from a string.
         *
         * @param pathString the file path as a string
         * @return this builder
         */
        public Builder path(String pathString) {
            return path(Paths.get(pathString));
        }

        /**
         * Sets the file path from a URI.
         *
         * @param uri the file URI
         * @return this builder
         */
        public Builder uri(URI uri) {
            Objects.requireNonNull(uri, "URI cannot be null");
            if (null == uri.getScheme()) {
                uri = URI.create("file:" + uri.toString());
            }
            try {
                this.path = Paths.get(uri);
            } catch (IllegalArgumentException | FileSystemNotFoundException ex) {
                throw new IllegalArgumentException(
                        "Unable to create reader for URI %s: %s".formatted(uri, ex.getMessage()), ex);
            }
            return this;
        }

        /**
         * Builds the FileRangeReader.
         *
         * @return a new FileRangeReader instance
         * @throws IOException if an error occurs during construction
         */
        public FileRangeReader build() throws IOException {
            if (path == null) {
                throw new IllegalStateException("Path must be set");
            }

            return new FileRangeReader(path);
        }
    }
}
