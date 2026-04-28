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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * A thread-safe file-based implementation of {@link RangeReader} that provides efficient random access to local files.
 *
 * <p>This implementation uses NIO {@link FileChannel} with position-based reads ({@link FileChannel#read(ByteBuffer,
 * long)}) to ensure thread safety. Unlike traditional stream-based file access, position-based reads allow multiple
 * threads to read from different parts of the same file concurrently without interference, as each read operation
 * specifies its absolute position in the file.
 *
 * <h2>NFS Resilience</h2>
 *
 * <p>This reader is designed for long-running server workloads where files may reside on NFS. It uses lazy channel
 * management with configurable idle timeout to prevent stale NFS file handles:
 *
 * <ul>
 *   <li>The file channel is opened lazily on first read, not at construction time
 *   <li>After a configurable idle period (default 60 seconds), the channel is automatically closed
 *   <li>Subsequent reads transparently reopen the channel
 *   <li>If a read encounters a stale file handle or closed channel, it retries once with a fresh channel
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is fully thread-safe for concurrent read operations. The underlying {@link FileChannel} supports
 * simultaneous reads from multiple threads as long as each operation uses absolute positioning, which this
 * implementation guarantees. Channel open/close transitions are protected by a lock, while the read hot path is
 * lock-free.
 *
 * <h2>Performance Characteristics</h2>
 *
 * <p>FileRangeReader provides excellent performance for random access patterns typical in tiled data access:
 *
 * <ul>
 *   <li>Zero-copy operations where possible through direct ByteBuffer usage
 *   <li>Efficient random access without seek overhead
 *   <li>OS-level caching benefits for frequently accessed file regions
 *   <li>No synchronization overhead between concurrent read operations
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Create reader for a PMTiles file
 * Path pmtilesFile = Paths.get("data/world.pmtiles");
 * try (FileRangeReader reader = FileRangeReader.of(pmtilesFile)) {
 *     // Read tile data from different threads concurrently
 *     ByteBuffer tileData = reader.readRange(offset, length);
 * }
 *
 * // With custom idle timeout for NFS
 * try (FileRangeReader reader = FileRangeReader.builder()
 *         .path(pmtilesFile)
 *         .idleTimeout(Duration.ofSeconds(30))
 *         .build()) {
 *     ByteBuffer tileData = reader.readRange(offset, length);
 * }
 * }</pre>
 *
 * @see FileChannel#read(ByteBuffer, long)
 */
@Slf4j
public class FileRangeReader extends AbstractRangeReader implements RangeReader {

    static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(60);

    static final ScheduledExecutorService IDLE_CLOSER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "FileRangeReader-idle-closer");
        t.setDaemon(true);
        return t;
    });

    private final Path path;
    private final long size;
    private final Duration idleTimeout;

    private final ReentrantLock channelLock = new ReentrantLock();
    private volatile FileChannel channel;
    private volatile boolean permanentlyClosed;
    private volatile long lastAccessNanos;
    private ScheduledFuture<?> idleCheckFuture; // guarded by channelLock

    /**
     * Creates a new FileRangeReader for the specified file path with the default idle timeout.
     *
     * <p>The file must exist and be readable, otherwise an {@link IOException} will be thrown. The file channel is
     * opened lazily on the first read operation and automatically closed after the default idle timeout of 60 seconds.
     *
     * @param path the path to the file to read from (must not be null)
     * @throws IOException if the file does not exist or cannot be accessed
     * @throws NullPointerException if path is null
     */
    public FileRangeReader(Path path) throws IOException {
        this(path, DEFAULT_IDLE_TIMEOUT);
    }

    FileRangeReader(Path path, Duration idleTimeout) throws IOException {
        Objects.requireNonNull(path, "Path cannot be null");
        this.path = path;
        this.idleTimeout = idleTimeout;
        this.size = Files.size(path); // validates file exists and is accessible
    }

    /**
     * Performs the actual range read operation using thread-safe positioned reads, with automatic retry on recoverable
     * errors (stale NFS file handles, closed channels).
     *
     * <p>This method implements the core reading logic using {@link FileChannel#read(ByteBuffer, long)} which provides
     * thread-safe access by specifying absolute file positions. Multiple threads can call this method concurrently
     * without synchronization, as each read operation is independent and doesn't affect the channel's position.
     *
     * <p>If the underlying channel has been closed (by the idle timeout or an NFS stale file handle), the method
     * transparently reopens it and retries the read once.
     *
     * @param offset the absolute position in the file to start reading from
     * @param actualLength the number of bytes to read
     * @param target the ByteBuffer to read data into (limit will be adjusted)
     * @return the actual number of bytes read, which may be less than actualLength if EOF is reached
     * @throws IOException if an I/O error occurs during reading
     */
    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) throws IOException {
        final int initialPosition = target.position();
        final int initialLimit = target.limit();
        target.limit(initialPosition + actualLength);

        try {
            return doRead(offset, actualLength, target);
        } catch (IOException e) {
            if (!permanentlyClosed && isRecoverableError(e)) {
                log.info("Recoverable I/O error on {}, retrying with fresh channel: {}", path, e.getMessage());
                closeStaleChannel();
                // Reset buffer state for retry
                target.position(initialPosition);
                target.limit(initialPosition + actualLength);
                return doRead(offset, actualLength, target);
            }
            throw e;
        } finally {
            target.limit(initialLimit);
        }
    }

    private int doRead(long offset, int actualLength, ByteBuffer target) throws IOException {
        FileChannel ch = ensureOpen();
        lastAccessNanos = System.nanoTime();

        int totalRead = 0;
        long pos = offset;
        while (totalRead < actualLength) {
            int read = ch.read(target, pos);
            if (read == -1) {
                break;
            }
            totalRead += read;
            pos += read;
        }
        return totalRead;
    }

    private FileChannel ensureOpen() throws IOException {
        if (permanentlyClosed) {
            throw new ClosedChannelException();
        }
        FileChannel ch = this.channel;
        if (ch != null && ch.isOpen()) {
            return ch;
        }
        channelLock.lock();
        try {
            if (permanentlyClosed) {
                throw new ClosedChannelException();
            }
            ch = this.channel;
            if (ch != null && ch.isOpen()) {
                return ch;
            }
            ch = openChannel(path);
            this.channel = ch;
            this.lastAccessNanos = System.nanoTime();
            scheduleIdleCheck();
            log.debug("Opened file channel for {}", path);
            return ch;
        } finally {
            channelLock.unlock();
        }
    }

    // visible for testing: allows subclasses to return a spy/mock channel
    FileChannel openChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private void scheduleIdleCheck() {
        // must be called under channelLock
        cancelIdleCheck();
        if (idleTimeout != null && !idleTimeout.isZero()) {
            idleCheckFuture = IDLE_CLOSER.scheduleAtFixedRate(
                    this::checkIdleAndClose, idleTimeout.toMillis(), idleTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void cancelIdleCheck() {
        // must be called under channelLock
        if (idleCheckFuture != null) {
            idleCheckFuture.cancel(false);
            idleCheckFuture = null;
        }
    }

    private void checkIdleAndClose() {
        if (permanentlyClosed) {
            channelLock.lock();
            try {
                cancelIdleCheck();
            } finally {
                channelLock.unlock();
            }
            return;
        }
        long elapsed = System.nanoTime() - lastAccessNanos;
        if (elapsed >= idleTimeout.toNanos()) {
            idleClose();
        }
    }

    private void idleClose() {
        channelLock.lock();
        try {
            if (permanentlyClosed) {
                return;
            }
            FileChannel ch = this.channel;
            this.channel = null;
            cancelIdleCheck();
            closeQuietly(ch);
            if (ch != null) {
                log.debug("Idle-closed file channel for {}", path);
            }
        } finally {
            channelLock.unlock();
        }
    }

    private void closeStaleChannel() {
        channelLock.lock();
        try {
            FileChannel ch = this.channel;
            this.channel = null;
            closeQuietly(ch);
        } finally {
            channelLock.unlock();
        }
    }

    static boolean isRecoverableError(IOException e) {
        if (e instanceof ClosedChannelException) {
            return true;
        }
        String msg = e.getMessage();
        return msg != null && msg.toLowerCase().contains("stale file handle");
    }

    private static void closeQuietly(FileChannel ch) {
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException ignored) {
                // intentionally swallowed
            }
        }
    }

    /**
     * Returns the size of the file in bytes.
     *
     * <p>The file size is cached at construction time using {@link Files#size(Path)} and does not depend on the file
     * channel being open. This means size queries work even when the channel has been idle-closed.
     *
     * @return the size of the file in bytes, never {@link OptionalLong#empty() empty}
     * @throws ClosedChannelException if this reader has been permanently closed via {@link #close()}
     */
    @Override
    public OptionalLong size() throws ClosedChannelException {
        if (permanentlyClosed) {
            throw new ClosedChannelException();
        }
        return OptionalLong.of(this.size);
    }

    /**
     * Returns a string identifier for this file source.
     *
     * <p>The identifier is the absolute path of the file, which uniquely identifies the data source for logging,
     * caching, and debugging purposes.
     *
     * @return the absolute path of the file as a string
     */
    @Override
    public String getSourceIdentifier() {
        return path.toAbsolutePath().toString();
    }

    /**
     * Closes this reader and releases any associated system resources.
     *
     * <p>This method is thread-safe and idempotent - it can be called multiple times without harm. After closing, any
     * further attempts to read from this FileRangeReader will result in a {@link ClosedChannelException}.
     *
     * <p>Any errors encountered while closing the underlying file channel are silently ignored, as this reader may be
     * closing a stale or already-closed channel.
     *
     * <p>It is recommended to use this FileRangeReader in a try-with-resources statement to ensure proper resource
     * cleanup.
     */
    @Override
    public void close() {
        channelLock.lock();
        try {
            permanentlyClosed = true;
            cancelIdleCheck();
            FileChannel ch = this.channel;
            this.channel = null;
            closeQuietly(ch);
        } finally {
            channelLock.unlock();
        }
    }

    /**
     * Creates a new builder for constructing FileRangeReader instances.
     *
     * <p>The builder pattern provides a flexible way to configure FileRangeReader construction with various path
     * specification methods (Path, String, URI) and an optional idle timeout for NFS resilience.
     *
     * @return a new builder instance for configuring FileRangeReader construction
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a new FileRangeReader for the specified file path.
     *
     * <p>This is a convenience method equivalent to: {@code FileRangeReader.builder().path(path).build()}
     *
     * @param path the file path
     * @return a new FileRangeReader instance
     * @throws IOException if an error occurs during construction
     */
    public static FileRangeReader of(Path path) throws IOException {
        return builder().path(path).build();
    }

    /** Builder for FileRangeReader. */
    public static class Builder {
        private Path path;
        private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

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
         * Sets the idle timeout after which the underlying file channel will be automatically closed to prevent stale
         * NFS file handles.
         *
         * <p>The channel will be transparently reopened on the next read operation. Set to {@link Duration#ZERO} to
         * disable idle timeout (channel stays open until explicitly closed).
         *
         * @param idleTimeout the idle timeout duration (default: 60 seconds)
         * @return this builder
         * @throws IllegalArgumentException if idleTimeout is negative
         * @throws NullPointerException if idleTimeout is null
         */
        public Builder idleTimeout(Duration idleTimeout) {
            Objects.requireNonNull(idleTimeout, "idleTimeout cannot be null");
            if (idleTimeout.isNegative()) {
                throw new IllegalArgumentException("idleTimeout cannot be negative");
            }
            this.idleTimeout = idleTimeout;
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

            return new FileRangeReader(path, idleTimeout);
        }
    }
}
