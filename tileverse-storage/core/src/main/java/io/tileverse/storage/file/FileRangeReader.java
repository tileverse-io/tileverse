/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.file;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.AbstractRangeReader;
import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.batch.FetchTargets;
import io.tileverse.storage.batch.FetchTargets.Region;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
 * <h2>Batched Reads</h2>
 *
 * <p>{@link #readRanges} joins abutting requests into runs of at most 32 MiB and bridges no gap. Vectored reads
 * ({@link FileChannel#read(ByteBuffer[], int, int)}, at most {@value #MAX_VIEWS_PER_READ} buffers per call) land a run
 * straight into the targets of its requests when they are direct buffers. Java 17 has no positional vectored read: a
 * vectored read sets the position of its channel first. It runs on the shared channel when no other vectored read holds
 * that channel's position. Otherwise it takes an extra channel from a pool of the reader, or opens one when the pool is
 * empty, and gives it back afterwards. The pool keeps at most {@value #MAX_IDLE_EXTRA_CHANNELS} idle channels.
 *
 * <p>A run holding a heap buffer is read request by request with positional reads. A vectored read would make the JDK
 * cache a temporary direct buffer per heap buffer on the reading thread, up to 1,024 of them. Single reads, and runs of
 * one request, keep the positional reads of the shared channel too. A batch reads one run at a time.
 *
 * <h2>NFS Resilience</h2>
 *
 * <p>This reader is designed for long-running server workloads where files may reside on NFS. It uses lazy channel
 * management with configurable idle timeout to prevent stale NFS file handles:
 *
 * <ul>
 *   <li>The file channel is opened lazily on first read, not at construction time
 *   <li>After a configurable idle period (default 60 seconds) with no read in progress, the channel is closed, with the
 *       idle extra channels of the vectored reads
 *   <li>An idle timeout of zero disables the idle check: the channel and up to {@value #MAX_IDLE_EXTRA_CHANNELS} idle
 *       extra channels stay open until {@link #close()}, five file descriptors per reader at rest
 *   <li>Subsequent reads transparently reopen the channel
 *   <li>A read that hits a stale file handle or a closed channel retires that channel and resumes on a fresh one, after
 *       the bytes already read; a call gets {@value #MAX_ATTEMPTS} attempts in total
 * </ul>
 *
 * <h2>Interrupts</h2>
 *
 * <p>{@link FileChannel} is interruptible: a read on an interrupted thread closes the channel for every other reader. A
 * read that starts on a thread whose interrupt flag is set therefore fails at once, without touching the channel, with
 * a {@link StorageException} caused by an {@link InterruptedIOException}; the flag stays set. A read interrupted midway
 * fails with the JDK's {@link ClosedByInterruptException} as the cause and is never retried, while the other readers
 * recover on a fresh channel.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is fully thread-safe for concurrent read operations. The underlying {@link FileChannel} supports
 * simultaneous reads from multiple threads as long as each operation uses absolute positioning, as single reads do. A
 * vectored read owns the position of its channel: concurrent vectored reads of one reader run on channels of their own
 * instead of waiting for each other. Channel open/close transitions are protected by a lock, while the hot path of
 * single reads is lock-free. A vectored read tries the lock of the shared channel's position and never waits on it.
 * When it gives an extra channel back, it takes the channel lock briefly to schedule the idle check. On Windows the JDK
 * serializes positional reads with the reads at the channel's position, since both move the file pointer there: a
 * vectored read on the shared channel delays the single reads of the reader while it runs.
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
 *   <li>One system call per run of abutting requests into direct buffers, for up to {@value #MAX_VIEWS_PER_READ}
 *       requests
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <p>Readers come from a {@link FileStorage}, or from {@link FileStorageProvider#openRangeReader(Path)} for a single
 * file outside any Storage:
 *
 * <pre>{@code
 * Path pmtilesFile = Paths.get("data/world.pmtiles");
 * try (RangeReader reader = FileStorageProvider.openRangeReader(pmtilesFile)) {
 *     // Read tile data from different threads concurrently
 *     ByteBuffer tileData = reader.readRange(offset, length);
 * }
 *
 * // With a custom idle timeout for NFS
 * try (RangeReader reader = FileStorageProvider.openRangeReader(pmtilesFile, Duration.ofSeconds(30))) {
 *     ByteBuffer tileData = reader.readRange(offset, length);
 * }
 * }</pre>
 *
 * @see FileChannel#read(ByteBuffer, long)
 */
@Slf4j
class FileRangeReader extends AbstractRangeReader implements RangeReader {

    static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(60);

    /** Attempts per read call, the first one included; every retry runs on a fresh channel. */
    static final int MAX_ATTEMPTS = 3;

    /** Joins abutting requests into runs of at most 32 MiB; a gap between two requests is never read. */
    static final CoalescingPolicy JOIN_ABUTTING = new CoalescingPolicy(0, CoalescingPolicy.DEFAULT_MAX_FETCH_BYTES);

    /** Most buffers handed to one vectored read: the {@code IOV_MAX} of Linux and macOS. */
    static final int MAX_VIEWS_PER_READ = 1024;

    /** Most extra channels kept open for the vectored reads finding the shared channel taken. */
    static final int MAX_IDLE_EXTRA_CHANNELS = 4;

    static final ScheduledThreadPoolExecutor IDLE_CLOSER = newIdleCloser();

    private final Path path;
    private final long size;
    private final Duration idleTimeout;

    private final ReentrantLock channelLock = new ReentrantLock();
    private final AtomicReference<FileChannel> channel = new AtomicReference<>();
    private final AtomicBoolean permanentlyClosed = new AtomicBoolean(false);

    /** Held by the vectored read running on the shared channel: the channel's position is its own while it runs. */
    private final ReentrantLock sharedPosition = new ReentrantLock();

    /** Extra channels kept open between vectored reads; the idle check and {@link #close()} close them. */
    private final ArrayBlockingQueue<FileChannel> idleExtraChannels = new ArrayBlockingQueue<>(MAX_IDLE_EXTRA_CHANNELS);

    /** Reads and batches in progress; the idle check never closes the channel while it is above zero. */
    private final AtomicInteger inFlight = new AtomicInteger();

    private volatile long lastAccessNanos;
    private ScheduledFuture<?> idleCheckFuture; // guarded by channelLock

    /**
     * Creates a new FileRangeReader for the specified file path with the default idle timeout.
     *
     * <p>The file must exist and be a regular file. Construction reads its size and resolves its real path, which names
     * the reader and opens every channel; the channel itself opens lazily on the first read and closes after the
     * default idle timeout of 60 seconds.
     *
     * @param path the path to the file to read from (must not be null)
     * @throws java.nio.file.NoSuchFileException if the file does not exist
     * @throws IOException if the file cannot be accessed or its real path cannot be resolved
     * @throws IllegalArgumentException if the path is a directory
     * @throws NullPointerException if path is null
     */
    FileRangeReader(Path path) throws IOException {
        this(path, DEFAULT_IDLE_TIMEOUT);
    }

    /**
     * Creates a new FileRangeReader with the given idle timeout; see {@link #FileRangeReader(Path)}.
     *
     * @param path the path to the file to read from (must not be null)
     * @param idleTimeout how long the channels may sit idle before they close; zero keeps them open until
     *     {@link #close()}, a value above zero must be at least 1 millisecond
     * @throws IOException if the file does not exist, cannot be accessed or its real path cannot be resolved
     * @throws IllegalArgumentException if the path is a directory or the timeout is negative or sub-millisecond
     */
    FileRangeReader(Path path, Duration idleTimeout) throws IOException {
        Objects.requireNonNull(path, "Path cannot be null");
        Objects.requireNonNull(idleTimeout, "idleTimeout cannot be null");
        if (idleTimeout.isNegative()) {
            throw new IllegalArgumentException("idleTimeout cannot be negative");
        }
        if (!idleTimeout.isZero() && idleTimeout.toMillis() == 0) {
            throw new IllegalArgumentException(
                    "idleTimeout must be zero (disabled) or at least 1 millisecond: " + idleTimeout);
        }
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        if (attributes.isDirectory()) {
            throw new IllegalArgumentException("Not a regular file: " + path);
        }
        this.path = path.toRealPath();
        this.idleTimeout = idleTimeout;
        this.size = attributes.size();
    }

    /**
     * Returns the size of the file in bytes.
     *
     * <p>The file size is read once at construction, from the file's attributes, and does not depend on the file
     * channel being open. This means size queries work even when the channel has been idle-closed.
     *
     * @return the size of the file in bytes, never {@link OptionalLong#empty() empty}
     * @throws IllegalStateException if this reader has been permanently closed via {@link #close()}
     */
    @Override
    public OptionalLong size() {
        checkNotClosed();
        return OptionalLong.of(this.size);
    }

    private void checkNotClosed() {
        if (permanentlyClosed.get()) {
            throw new IllegalStateException("FileRangeReader is closed");
        }
    }

    Duration idleTimeout() {
        return idleTimeout;
    }

    /**
     * Returns the real path of the file, resolved once at construction: symlinks followed, case and {@code ..} segments
     * normalized. Two readers over one file reached by different spellings therefore share an identifier, a range-cache
     * partition and a footer-cache entry. Performs no I/O.
     *
     * @return the real path of the file as a string
     */
    @Override
    public String getSourceIdentifier() {
        return path.toString();
    }

    /**
     * Closes this reader and releases any associated system resources.
     *
     * <p>This method is thread-safe and idempotent - it can be called multiple times without harm. After closing, any
     * further attempt to read from or query this FileRangeReader throws {@link IllegalStateException}, and the channel
     * is never reopened. The idle extra channels close too. An extra channel in use by a running vectored read closes
     * when that read ends.
     *
     * <p>Any errors encountered while closing the underlying file channel are silently ignored, as this reader may be
     * closing a stale or already-closed channel.
     *
     * <p>It is recommended to use this FileRangeReader in a try-with-resources statement to ensure proper resource
     * cleanup.
     */
    @Override
    public void close() {
        if (permanentlyClosed.compareAndSet(false, true)) {
            channelLock.lock();
            try {
                cancelIdleCheck();
                closeQuietly(channel.getAndSet(null));
                closeIdleExtraChannels();
            } finally {
                channelLock.unlock();
            }
        }
    }

    // visible for testing: simulates an idle close
    void closeChannel() {
        channelLock.lock();
        try {
            closeQuietly(channel.getAndSet(null));
        } finally {
            channelLock.unlock();
        }
    }

    /**
     * Reads the range with thread-safe positioned reads, recovering from stale NFS file handles and closed channels on
     * a fresh channel. See the class javadoc for the recovery and interrupt rules.
     *
     * @param offset the absolute position in the file to start reading from
     * @param actualLength the number of bytes to read
     * @param target the ByteBuffer to read data into (limit will be adjusted)
     * @return the actual number of bytes read, which may be less than actualLength if EOF is reached
     * @throws StorageException if an I/O error occurs during reading
     * @throws IllegalStateException if this reader has been permanently closed via {@link #close()}
     */
    @Override
    protected int readRangeNoFlip(long offset, int actualLength, ByteBuffer target) {
        checkNotClosed();
        final int initialLimit = target.limit();
        target.limit(target.position() + actualLength);
        inFlight.incrementAndGet();
        try {
            return readWithRecovery(offset, target);
        } finally {
            target.limit(initialLimit);
            recordAccessEnd();
        }
    }

    /** Keeps the channels open for the whole batch, however long its reads take. */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        checkNotClosed();
        inFlight.incrementAndGet();
        try {
            return super.readRanges(requests);
        } finally {
            recordAccessEnd();
        }
    }

    /** Joins abutting requests and bridges no gap: a local read costs no round trip worth saving. */
    @Override
    protected CoalescingPolicy coalescingPolicy() {
        return JOIN_ABUTTING;
    }

    /**
     * Reads a run of abutting requests straight into their targets: with vectored reads when the targets are direct
     * buffers, else request by request with positional reads. Both recover from stale NFS file handles and closed
     * channels as single reads do. See the class javadoc for the reason and for the channels used by vectored reads.
     */
    @Override
    protected int readJoinedFetch(ByteRange range, FetchTargets targets) {
        checkNotClosed();
        inFlight.incrementAndGet();
        try {
            List<Region> regions = targets.regions();
            if (areDirect(regions)) {
                return readVectoredWithRecovery(range.offset(), new VectoredViews(regions));
            }
            return readRegionByRegion(range.offset(), regions);
        } finally {
            recordAccessEnd();
        }
    }

    /** Whether the targets of the run are direct buffers: a vectored read takes direct buffers only. */
    private static boolean areDirect(List<Region> regions) {
        for (Region region : regions) {
            if (!region.view().isDirect()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads the regions one by one with the positional reads of single reads, and stops after a region read short: the
     * file ended there. Returns the offset in the fetch just past the last byte landed.
     */
    private int readRegionByRegion(long fetchOffset, List<Region> regions) {
        int landed = 0;
        for (Region region : regions) {
            int read = readWithRecovery(fetchOffset + region.offsetInFetch(), region.view());
            if (read > 0) {
                landed = region.offsetInFetch() + read;
            }
            if (read < region.length()) {
                return landed;
            }
        }
        return landed;
    }

    private void recordAccessEnd() {
        lastAccessNanos = System.nanoTime();
        inFlight.decrementAndGet();
    }

    /**
     * Reads the range in up to {@value #MAX_ATTEMPTS} attempts. A recoverable failure retires the channel it happened
     * on, and the next attempt resumes on a fresh channel after the bytes already landed: no byte is read twice.
     */
    private int readWithRecovery(long offset, ByteBuffer target) {
        final int start = target.position();
        for (int attempt = 1; ; attempt++) {
            failIfInterrupted();
            FileChannel ch = ensureOpen();
            int done = target.position() - start;
            try {
                readChunks(ch, target, offset + done);
                return target.position() - start;
            } catch (IOException failure) {
                if (!isRecoverable(failure)) {
                    throw new StorageException("Read failed for " + path, failure);
                }
                retire(ch);
                if (attempt == MAX_ATTEMPTS) {
                    throw new StorageException("Read failed after " + MAX_ATTEMPTS + " attempts for " + path, failure);
                }
                log.info("Recoverable I/O error on {}, retrying on a fresh channel: {}", path, failure.getMessage());
            }
        }
    }

    /**
     * Fills the views in up to {@value #MAX_ATTEMPTS} attempts, as {@link #readWithRecovery} does for a single read. A
     * recoverable failure retires the failed channel, and the next attempt resumes after the bytes already landed, on
     * an extra channel opened for it: a channel opened before the failure may be stale too.
     */
    private int readVectoredWithRecovery(long fetchOffset, VectoredViews views) {
        for (int attempt = 1; ; attempt++) {
            failIfInterrupted();
            try {
                if (attempt == 1) {
                    readVectoredOnAFreeChannel(fetchOffset, views);
                } else {
                    readVectoredOnExtraChannel(openExtraChannel(), fetchOffset, views);
                }
                return views.bytesLanded();
            } catch (IOException failure) {
                if (!isRecoverable(failure)) {
                    throw new StorageException("Read failed for " + path, failure);
                }
                if (attempt == MAX_ATTEMPTS) {
                    throw new StorageException("Read failed after " + MAX_ATTEMPTS + " attempts for " + path, failure);
                }
                log.info("Recoverable I/O error on {}, retrying on a fresh channel: {}", path, failure.getMessage());
            }
        }
    }

    /** Runs a vectored read on the shared channel when no other one holds its position, else on an extra channel. */
    private void readVectoredOnAFreeChannel(long fetchOffset, VectoredViews views) throws IOException {
        if (!sharedPosition.tryLock()) {
            readVectoredOnExtraChannel(takeExtraChannel(), fetchOffset, views);
            return;
        }
        try {
            readVectoredOnSharedChannel(fetchOffset, views);
        } finally {
            sharedPosition.unlock();
        }
    }

    /** A recoverable failure retires the shared channel, as it does for a single read. */
    private void readVectoredOnSharedChannel(long fetchOffset, VectoredViews views) throws IOException {
        FileChannel shared = ensureOpen();
        try {
            views.readFrom(shared, fetchOffset);
        } catch (IOException failure) {
            if (isRecoverable(failure)) {
                retire(shared);
            }
            throw failure;
        }
    }

    /** An extra channel goes back to the pool after a read, and is closed after any failure. */
    private void readVectoredOnExtraChannel(FileChannel extra, long fetchOffset, VectoredViews views)
            throws IOException {
        boolean read = false;
        try {
            views.readFrom(extra, fetchOffset);
            read = true;
        } finally {
            if (read) {
                giveBack(extra);
            } else {
                closeQuietly(extra);
            }
        }
    }

    /** Takes an idle extra channel, or opens one when none is idle. */
    private FileChannel takeExtraChannel() {
        FileChannel idle = idleExtraChannels.poll();
        if (idle != null && idle.isOpen()) {
            return idle;
        }
        return openExtraChannel();
    }

    private FileChannel openExtraChannel() {
        checkNotClosed();
        try {
            FileChannel extra = openChannel(path);
            log.debug("Opened an extra file channel for {}", path);
            return extra;
        } catch (IOException e) {
            throw new StorageException("Failed to open " + path, e);
        }
    }

    /**
     * Keeps an extra channel for the next vectored read, or closes it when the pool is full. The channel is closed when
     * the reader closed before it came back, since {@link #close()} may have emptied the pool already.
     */
    private void giveBack(FileChannel extra) {
        if (!extra.isOpen() || !idleExtraChannels.offer(extra)) {
            closeQuietly(extra);
            return;
        }
        if (permanentlyClosed.get()) {
            closeIdleExtraChannels();
            return;
        }
        ensureIdleCheckScheduled();
    }

    private void closeIdleExtraChannels() {
        List<FileChannel> idle = new ArrayList<>(MAX_IDLE_EXTRA_CHANNELS);
        idleExtraChannels.drainTo(idle);
        idle.forEach(FileRangeReader::closeQuietly);
    }

    // visible for testing
    int idleExtraChannelCount() {
        return idleExtraChannels.size();
    }

    /** An interrupted thread must not touch the channel: the JDK would close it for every other reader. */
    private void failIfInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new StorageException("Read interrupted for " + path, new InterruptedIOException());
        }
    }

    /**
     * Fills the target up to its limit from {@code position}, stopping at EOF. Every chunk advances the target's
     * position by exactly what landed, which is how a retry knows where to resume. A chunk of zero bytes from an open
     * channel is a failure, not a reason to read again in place.
     */
    private static void readChunks(FileChannel ch, ByteBuffer target, long position) throws IOException {
        long next = position;
        while (target.hasRemaining()) {
            int chunk = ch.read(target, next);
            if (chunk == -1) {
                return;
            }
            if (chunk == 0) {
                throw new NoProgressException(next);
            }
            next += chunk;
        }
    }

    /**
     * Drops a channel a read failed on. The compare-and-set makes the first thread to notice the failure close it; a
     * thread whose channel was already replaced closes nothing and retries on the replacement.
     */
    private void retire(FileChannel failed) {
        if (channel.compareAndSet(failed, null)) {
            closeQuietly(failed);
        }
    }

    FileChannel channel() {
        return channel.get();
    }

    private FileChannel ensureOpen() {
        FileChannel ch = channel.get();
        if (ch != null && ch.isOpen()) {
            return ch;
        }
        channelLock.lock();
        try {
            checkNotClosed();
            ch = channel.get();
            if (ch != null && ch.isOpen()) {
                return ch;
            }
            ch = openChannel(path);
            channel.set(ch);
            scheduleIdleCheck();
            log.debug("Opened file channel for {}", path);
            return ch;
        } catch (IOException e) {
            throw new StorageException("Failed to open " + path, e);
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
        if (!idleTimeout.isZero()) {
            long period = idleTimeout.toMillis();
            idleCheckFuture =
                    IDLE_CLOSER.scheduleAtFixedRate(this::checkIdleAndClose, period, period, TimeUnit.MILLISECONDS);
        }
    }

    /** Schedules the idle check when none is scheduled: an idle extra channel must close like the shared one. */
    private void ensureIdleCheckScheduled() {
        channelLock.lock();
        try {
            if (idleCheckFuture == null && !permanentlyClosed.get()) {
                scheduleIdleCheck();
            }
        } finally {
            channelLock.unlock();
        }
    }

    private void cancelIdleCheck() {
        // must be called under channelLock
        if (idleCheckFuture != null) {
            idleCheckFuture.cancel(false);
            idleCheckFuture = null;
        }
    }

    /**
     * Closes the channel and the idle extra channels once no read is in progress and the timeout has elapsed since the
     * last one ended.
     */
    private void checkIdleAndClose() {
        if (!idleLongEnough()) {
            return;
        }
        channelLock.lock();
        try {
            if (!idleLongEnough()) {
                return;
            }
            cancelIdleCheck();
            FileChannel ch = channel.getAndSet(null);
            closeQuietly(ch);
            closeIdleExtraChannels();
            if (ch != null) {
                log.debug("Idle-closed file channel for {}", path);
            }
        } finally {
            channelLock.unlock();
        }
    }

    private boolean idleLongEnough() {
        return inFlight.get() == 0 && System.nanoTime() - lastAccessNanos >= idleTimeout.toNanos();
    }

    /**
     * Whether a read may be retried on a fresh channel: any closed channel except one closed by an interrupt, a
     * zero-byte chunk, or a stale NFS handle in glibc's ("Stale file handle") or BSD's ("Stale NFS file handle") words.
     */
    static boolean isRecoverable(IOException e) {
        if (e instanceof ClosedByInterruptException) {
            return false;
        }
        if (e instanceof ClosedChannelException || e instanceof NoProgressException) {
            return true;
        }
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        String lowerCase = message.toLowerCase(Locale.ROOT);
        return lowerCase.contains("stale file handle") || lowerCase.contains("stale nfs file handle");
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
     * One daemon thread for every reader's idle check. Cancelled checks leave the queue at once instead of pinning
     * their reader until the next tick, and the thread keeps the system class loader rather than the loader of
     * whichever caller scheduled first.
     */
    private static ScheduledThreadPoolExecutor newIdleCloser() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, FileRangeReader::newIdleCloserThread);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private static Thread newIdleCloserThread(Runnable task) {
        Thread thread = new Thread(task, "FileRangeReader-idle-closer");
        thread.setDaemon(true);
        thread.setContextClassLoader(ClassLoader.getSystemClassLoader());
        return thread;
    }

    /**
     * The views of a joined fetch, filled from a file with vectored reads at the channel's position. A vectored read
     * takes abutting views only, at most {@value #MAX_VIEWS_PER_READ} of them: the bytes between two requests are never
     * read. The views fill in order, and their positions tell where a retry resumes.
     */
    private static final class VectoredViews {
        private final List<Region> regions;
        private final ByteBuffer[] views;

        VectoredViews(List<Region> regions) {
            this.regions = regions;
            this.views = new ByteBuffer[regions.size()];
            for (int i = 0; i < views.length; i++) {
                views[i] = regions.get(i).view();
            }
        }

        /** Fills the views from the first one with room left, stopping at EOF. */
        void readFrom(FileChannel channel, long fetchOffset) throws IOException {
            int first = firstWithRoom(0);
            while (first < views.length) {
                long position = fetchOffset + nextOffsetInFetch(first);
                channel.position(position);
                long read = channel.read(views, first, abuttingRun(first));
                if (read == -1) {
                    return;
                }
                if (read == 0) {
                    throw new NoProgressException(position);
                }
                first = firstWithRoom(first);
            }
        }

        /** Returns the offset in the fetch just past the last byte landed so far. */
        int bytesLanded() {
            for (int i = views.length - 1; i >= 0; i--) {
                int filled = filled(i);
                if (filled > 0) {
                    return regions.get(i).offsetInFetch() + filled;
                }
            }
            return 0;
        }

        private int firstWithRoom(int from) {
            int index = from;
            while (index < views.length && !views[index].hasRemaining()) {
                index++;
            }
            return index;
        }

        private int nextOffsetInFetch(int index) {
            return regions.get(index).offsetInFetch() + filled(index);
        }

        private int filled(int index) {
            return regions.get(index).length() - views[index].remaining();
        }

        /** Counts the abutting views from {@code first} on, at most {@value #MAX_VIEWS_PER_READ} of them. */
        private int abuttingRun(int first) {
            int count = 1;
            while (count < MAX_VIEWS_PER_READ && first + count < views.length && abuts(first + count - 1)) {
                count++;
            }
            return count;
        }

        private boolean abuts(int index) {
            Region region = regions.get(index);
            return region.offsetInFetch() + region.length()
                    == regions.get(index + 1).offsetInFetch();
        }
    }

    /** A positional read that returned zero bytes on an open channel with room left in the target. */
    private static final class NoProgressException extends IOException {
        private static final long serialVersionUID = 1L;

        NoProgressException(long position) {
            super("Read returned no bytes at position " + position);
        }
    }
}
