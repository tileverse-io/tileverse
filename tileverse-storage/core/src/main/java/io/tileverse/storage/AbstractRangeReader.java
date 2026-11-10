/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import io.tileverse.io.ByteRange;
import io.tileverse.storage.batch.BatchExecutors;
import io.tileverse.storage.batch.BatchPlanner;
import io.tileverse.storage.batch.BatchRunner;
import io.tileverse.storage.batch.CoalescingPolicy;
import io.tileverse.storage.batch.FetchReader;
import io.tileverse.storage.batch.FetchTargets;
import io.tileverse.storage.batch.PlannedFetch;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class providing common implementation for {@link RangeReader}.
 *
 * <p>This class implements the core range reading logic with parameter validation, boundary checking, and buffer
 * management. Concrete implementations only need to override {@link #readRangeNoFlip(long, int, ByteBuffer)} to provide
 * the actual data reading mechanism.
 *
 * <p>The template method pattern is used where {@link #readRange(long, int, ByteBuffer)} handles all the common
 * concerns (validation, EOF handling, buffer preparation) and delegates the actual reading to the abstract
 * {@code readRangeNoFlip} method.
 *
 * <p>Batched reads ({@link #readRanges(List)}) run through the same {@code readRange} template, and through
 * {@link #readJoinedFetch} for a fetch joining several requests: subclasses tune them with {@link #coalescingPolicy()}
 * and {@link #maxConcurrentFetches()}, and write joined fetches straight into their targets by overriding
 * {@code readJoinedFetch}, instead of reimplementing the batch loop.
 */
@Slf4j
public abstract class AbstractRangeReader implements RangeReader {

    private final FetchReader fetchReader = new TemplateFetchReader();

    /**
     * Creates a new AbstractRangeReader.
     *
     * <p>This constructor is provided for subclasses to extend. Subclasses should implement the
     * {@link #readRangeNoFlip(long, int, ByteBuffer)} method to provide the actual reading implementation.
     */
    protected AbstractRangeReader() {
        // Default constructor for subclasses
    }

    /**
     * Reads bytes from the source at the specified offset into the provided target buffer.
     *
     * <p>This method provides the core implementation of range reading with comprehensive parameter validation,
     * boundary checking, and buffer management following standard NIO conventions. It delegates the actual data reading
     * to {@link #readRangeNoFlip(long, int, ByteBuffer)}.
     *
     * <p><strong>Parameter Validation:</strong>
     *
     * <ul>
     *   <li>Validates that offset and length are non-negative
     *   <li>Ensures target buffer is not null and not read-only
     *   <li>Checks that target buffer has sufficient remaining capacity
     * </ul>
     *
     * <p><strong>Boundary Handling:</strong>
     *
     * <ul>
     *   <li>For zero-length reads, returns immediately with 0 bytes read
     *   <li>For reads starting at or beyond EOF, returns 0 bytes read (backends signal this case with
     *       {@link RangeNotSatisfiableException}, translated here)
     *   <li>For reads extending beyond EOF, the backend returns only the available bytes
     * </ul>
     *
     * <p><strong>Buffer Management (NIO Conventions):</strong> Following standard Java NIO conventions, this method
     * writes data into the buffer and advances the position:
     *
     * <ul>
     *   <li>Data is written starting at the buffer's current position
     *   <li>Position is advanced by the number of bytes written
     *   <li>Limit remains unchanged
     *   <li>Caller must call {@code flip()} to prepare buffer for reading
     * </ul>
     *
     * <p><strong>Thread Safety:</strong> This method is thread-safe when the underlying {@link #readRangeNoFlip(long,
     * int, ByteBuffer)} implementation is thread-safe. The target may be written from a thread other than the caller's;
     * see {@link RangeReader#readRange(long, int, ByteBuffer)}.
     *
     * @param offset The byte offset to read from (must be >= 0)
     * @param length The number of bytes to read (must be >= 0)
     * @param target The ByteBuffer to read into, starting at its current position
     * @return The number of bytes actually read (may be less than requested if EOF is reached)
     * @throws StorageException If a storage error occurs during reading
     * @throws IllegalArgumentException If offset or length is negative, target is null, or target has insufficient
     *     remaining capacity
     * @throws java.nio.ReadOnlyBufferException If the target buffer is read-only
     */
    @Override
    @SuppressWarnings("java:S2583") // if target==null we throw IAE
    public final int readRange(long offset, int length, ByteBuffer target) {
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
            throw new ReadOnlyBufferException();
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

        try {
            return readRangeNoFlip(offset, length, target);
        } catch (RangeNotSatisfiableException pastEof) {
            // The backend answered 416: the offset is at or past EOF. Preserve the documented
            // contract of returning 0 bytes read with the buffer position unchanged.
            return 0;
        }
    }

    /**
     * Reads a batch by planning fetches with {@link #coalescingPolicy()} and running them with up to
     * {@link #maxConcurrentFetches()} concurrent fetches on the shared batch executor.
     *
     * <p>A fetch of one request goes through {@link #readRange(long, int, ByteBuffer)}, and a joined fetch, of requests
     * following one another without overlapping, through {@link #readJoinedFetch}, with the same translation of
     * {@link RangeNotSatisfiableException} into 0 bytes read. Backend exception mapping and any response metadata
     * captures apply to batched reads exactly as to single reads. A fetch of overlapping or repeated requests reads
     * into pooled heap scratch through {@code readRange} and is copied into its targets. With the default hooks
     * ({@link CoalescingPolicy#NONE}, one fetch at a time) the behavior is the interface default: each range read
     * exactly, in request order, nothing amplified.
     *
     * <p>Worst-case amplification: the requested bytes plus the gaps the policy merges, at most
     * {@link CoalescingPolicy#maxFetchBytes()} per fetch.
     *
     * <p>An override that bypasses this template, and with it {@code readRange} and {@link #fetchReader()}, must
     * translate {@link RangeNotSatisfiableException} into 0-byte results itself.
     *
     * @param requests the ranges to read and the buffers they land in
     * @return the bytes read per request, in request order, and what the call cost
     */
    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        if (requests.isEmpty()) {
            return BatchReadResult.EMPTY;
        }
        List<PlannedFetch> fetches = BatchPlanner.plan(requests, coalescingPolicy());
        return BatchRunner.run(requests, fetches, fetchReader(), maxConcurrentFetches(), BatchExecutors::shared);
    }

    /**
     * The fetch reader of the batched-read template, for a subclass running {@link BatchRunner} itself: a fetch of one
     * request, or of overlapping requests, goes through {@link #readRange(long, int, ByteBuffer)}, a joined fetch
     * through {@link #readJoinedFetch}, both translating {@link RangeNotSatisfiableException} into 0 bytes read.
     *
     * @return the reader to hand to {@link BatchRunner}
     */
    protected final FetchReader fetchReader() {
        return fetchReader;
    }

    /**
     * Reads a joined fetch: a fetch of several requests following one another without overlapping, planned under
     * {@link #coalescingPolicy()}. The bytes of the requests land in their targets through {@code targets}, with
     * {@link FetchTargets#write}, {@link FetchTargets#read} or the views of {@link FetchTargets#regions()}, and the
     * bytes between two requests are dropped. The caller settles the counts and target positions from the returned
     * count.
     *
     * <p>The default reads the fetch into pooled heap scratch through {@link #readRangeNoFlip(long, int, ByteBuffer)}
     * and copies the bytes of the requests into their targets. Backends override it to write the bytes straight into
     * the targets.
     *
     * <p><strong>Implementation requirements:</strong> return how many bytes of the fetch were read from its start, gap
     * bytes included, short of the range only at EOF; leave the targets' positions alone; write nothing once the method
     * has returned or thrown. A range starting at or past EOF may read 0 bytes or throw
     * {@link RangeNotSatisfiableException}. The template translates the exception into 0 bytes read. Backend exception
     * mapping and response metadata captures apply as in {@code readRangeNoFlip}.
     *
     * @param range the byte range of the fetch; guaranteed to start at its first request and to hold at least one byte
     * @param targets where the requests' bytes land
     * @return the number of bytes of the fetch read
     */
    protected int readJoinedFetch(ByteRange range, FetchTargets targets) {
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(range.length())) {
            ByteBuffer scratch = pooled.buffer();
            int read = readRangeNoFlip(range.offset(), range.length(), scratch);
            targets.write(0, scratch, 0, read);
            return read;
        }
    }

    /**
     * The merge policy for batched reads. The default, {@link CoalescingPolicy#NONE}, reads every range exactly;
     * backends where a round trip has a real cost override this with a network-derived policy. The local file reader
     * joins abutting ranges and bridges no gap.
     *
     * @return the policy handed to the batch planner
     */
    protected CoalescingPolicy coalescingPolicy() {
        return CoalescingPolicy.NONE;
    }

    /**
     * How many planned fetches may run at once for one batched read. The default of 1 keeps batches sequential on the
     * calling thread; backends that profit from parallel fetches raise it.
     *
     * @return the fetch parallelism cap, at least 1
     */
    protected int maxConcurrentFetches() {
        return 1;
    }

    /**
     * Reads bytes from the source into the target buffer without preparing the buffer for consumption.
     *
     * <p>This is the core template method that concrete implementations must override to provide the actual data
     * reading logic. Unlike {@link #readRange(long, int, ByteBuffer)}, this method does not perform parameter
     * validation or buffer preparation - those responsibilities are handled by the calling {@code readRange} method.
     *
     * <p><strong>Implementation Requirements:</strong>
     *
     * <ul>
     *   <li>Read exactly {@code actualLength} bytes from the source at {@code offset}
     *   <li>Write the data into {@code target} starting at its current position
     *   <li>Advance the target buffer's position by the number of bytes written
     *   <li>Do NOT modify the target buffer's limit
     *   <li>Return the actual number of bytes read (may be less than requested if EOF reached)
     *   <li>Must be thread-safe for concurrent access
     *   <li>The requested range may start at or past EOF, or extend beyond it. Return 0 or a short count in that case,
     *       or throw {@link RangeNotSatisfiableException} for a past-EOF offset; the calling {@code readRange}
     *       translates it to 0 bytes read.
     * </ul>
     *
     * <p><strong>Parameter Guarantees:</strong> When this method is called by {@link #readRange(long, int,
     * ByteBuffer)}, the following conditions are guaranteed:
     *
     * <ul>
     *   <li>{@literal offset >= 0}
     *   <li>{@literal actualLength > 0} (zero-length reads are handled by the caller)
     *   <li>{@code target} is not null and not read-only
     *   <li>{@literal target.remaining() >= actualLength}
     * </ul>
     *
     * <p><strong>Buffer State Contract:</strong>
     *
     * <pre>{@code
     * // Before calling readRangeNoFlip:
     * int initialPosition = target.position();
     * int initialLimit = target.limit();
     *
     * // After readRangeNoFlip returns:
     * int bytesRead = readRangeNoFlip(offset, actualLength, target);
     * assert target.position() == initialPosition + bytesRead;
     * assert target.limit() == initialLimit; // unchanged
     * }</pre>
     *
     * <p>The calling {@code readRange} method will then reset the position and adjust the limit to prepare the buffer
     * for immediate consumption by the caller.
     *
     * @param offset The byte offset to read from (guaranteed to be {@literal >= 0}; may be at or past EOF)
     * @param actualLength The number of bytes to read (guaranteed to be {@literal > 0} and within buffer capacity)
     * @param target The ByteBuffer to write into (guaranteed to be non-null, writable, and have sufficient capacity)
     * @return The number of bytes actually read and written to the target buffer
     * @throws StorageException If a storage error occurs during reading from the underlying source
     */
    protected abstract int readRangeNoFlip(long offset, int actualLength, ByteBuffer target);

    /**
     * The reader of the batched-read template: one request or overlapping requests through readRange, joined requests
     * through the hook.
     */
    private final class TemplateFetchReader implements FetchReader {

        @Override
        public int read(ByteRange range, ByteBuffer target) {
            return readRange(range, target);
        }

        /** Translates a fetch answered 416 into 0 bytes read, as readRange does: the fetch starts at or past EOF. */
        @Override
        public int readJoined(ByteRange range, FetchTargets targets) {
            try {
                return readJoinedFetch(range, targets);
            } catch (RangeNotSatisfiableException pastEof) {
                return 0;
            }
        }
    }
}
