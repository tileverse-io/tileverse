/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.batch;

import static java.util.Objects.requireNonNull;

import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.batch.PlannedFetch.Slice;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The targets of a joined fetch: a fetch of requests following one another without overlapping. The bytes of the
 * requests are written straight into their callers' buffers instead of a scratch buffer.
 *
 * <p>Each request owns a {@link Region} of the fetch, from its offset in the fetch for its length. The bytes between
 * two regions belong to no request: they are read and dropped. Writes land at absolute positions, counted from the
 * target positions captured by {@link #of}, and never move a target's position. Once the fetch has completed,
 * {@link #settle} records the requests' counts and advances their targets' positions.
 *
 * <p>An executor fills the targets in one of three ways: it pushes chunks with {@link #write}, pulls bytes from a
 * channel with {@link #read}, or reads into the {@link Region#view() views} of the regions. Instances are not
 * thread-safe: a fetch has one writer at a time, and an executor writing from several threads serializes the writes
 * itself.
 */
public final class FetchTargets {

    /**
     * One request's part of a joined fetch.
     *
     * @param offsetInFetch where the request's bytes start in the fetch
     * @param length the request's length in bytes
     * @param view a duplicate of the request's target, from the target's position when the targets were built, with
     *     room for {@code length} bytes; reading into it advances the view, never the target
     */
    public record Region(int offsetInFetch, int length, ByteBuffer view) {}

    /**
     * Receives the gap bytes dropped by {@link #read}. Concurrent reads share it because nobody reads these bytes back.
     */
    private static final byte[] DROPPED_BYTES = new byte[8192];

    private final int length;
    private final List<Region> regions;
    private final int[] offsets;
    private final int[] starts;
    private final ByteBuffer[] targets;
    private final int[] requestIndexes;

    private FetchTargets(int length, List<Slice> slices, List<RangeRequest> requests) {
        int count = slices.size();
        List<Region> built = new ArrayList<>(count);
        this.length = length;
        this.offsets = new int[count];
        this.starts = new int[count];
        this.targets = new ByteBuffer[count];
        this.requestIndexes = new int[count];
        for (int i = 0; i < count; i++) {
            Slice slice = slices.get(i);
            ByteBuffer target = requests.get(slice.requestIndex()).target();
            checkFits(slice, length, target);
            int start = target.position();
            ByteBuffer view = target.duplicate();
            view.limit(start + slice.length());
            built.add(new Region(slice.offsetInFetch(), slice.length(), view));
            offsets[i] = slice.offsetInFetch();
            starts[i] = start;
            targets[i] = target;
            requestIndexes[i] = slice.requestIndex();
        }
        this.regions = Collections.unmodifiableList(built);
    }

    private static void checkFits(Slice slice, int fetchLength, ByteBuffer target) {
        if (slice.offsetInFetch() + slice.length() > fetchLength) {
            throw new IllegalArgumentException("slice " + slice + " ends past the fetch of " + fetchLength + " bytes");
        }
        if (target.remaining() < slice.length()) {
            throw new IllegalArgumentException("target of request " + slice.requestIndex()
                    + " has insufficient remaining capacity: " + target.remaining() + " < " + slice.length());
        }
    }

    /**
     * Builds the targets of a fetch of slices following one another without overlapping.
     *
     * @param fetch the planned fetch
     * @param requests the requests of the batch, indexed by the slices of the fetch
     * @return the targets, capturing their positions
     * @throws IllegalArgumentException if the slices overlap, a slice ends past the fetch, or a target has no room for
     *     its request
     */
    public static FetchTargets of(PlannedFetch fetch, List<RangeRequest> requests) {
        requireNonNull(fetch, "fetch cannot be null");
        requireNonNull(requests, "requests cannot be null");
        if (!fetch.hasDisjointSlices()) {
            throw new IllegalArgumentException(
                    "the slices of a joined fetch must follow one another without overlapping");
        }
        return new FetchTargets(fetch.range().length(), fetch.slices(), requests);
    }

    /**
     * Returns the length of the fetch: the number of bytes accepted by {@link #write} and {@link #read}, from offset 0.
     *
     * @return the fetch length in bytes
     */
    public int length() {
        return length;
    }

    /**
     * Returns the regions of the requests, in ascending offset order.
     *
     * @return an unmodifiable list of the regions
     */
    public List<Region> regions() {
        return regions;
    }

    /**
     * Writes {@code count} bytes of {@code source}, from its index {@code sourceIndex}, at {@code offsetInFetch}. Each
     * byte lands in the region covering its offset, and the bytes outside the regions are dropped. Neither the source's
     * position nor a target's moves, and a later write at the same offset overwrites, as a retried download does.
     *
     * @param offsetInFetch where the bytes start in the fetch
     * @param source the bytes to write
     * @param sourceIndex the index of the first byte in {@code source}
     * @param count how many bytes to write
     * @throws IndexOutOfBoundsException if the bytes lie outside the fetch or the source
     */
    public void write(int offsetInFetch, ByteBuffer source, int sourceIndex, int count) {
        Objects.checkFromIndexSize(offsetInFetch, count, length);
        Objects.checkFromIndexSize(sourceIndex, count, source.limit());
        int end = offsetInFetch + count;
        int index = firstRegionEndingAfter(offsetInFetch);
        while (index < regions.size() && offsets[index] < end) {
            int from = Math.max(offsetInFetch, offsets[index]);
            int to = Math.min(end, regionEnd(index));
            int targetIndex = starts[index] + from - offsets[index];
            regions.get(index).view().put(targetIndex, source, sourceIndex + from - offsetInFetch, to - from);
            index++;
        }
    }

    /**
     * Reads the bytes of the fetch from {@code offsetInFetch}, for {@code count} bytes, out of {@code channel}. Each
     * byte lands in the region covering its offset, and the bytes outside the regions are read and dropped. Stops early
     * at the end of the stream. The views of the regions advance as they fill; the targets' positions do not move.
     *
     * <p>The dropped bytes land in one array shared by concurrent reads. The channel must not read back the bytes
     * written into a buffer, for instance to checksum them, because another read may overwrite them in the meantime.
     *
     * @param channel the stream of the fetch bytes, positioned at {@code offsetInFetch}
     * @param offsetInFetch where the bytes start in the fetch
     * @param count how many bytes to read
     * @return the bytes consumed from the channel, the dropped ones included
     * @throws IOException if the channel fails
     * @throws IndexOutOfBoundsException if the bytes lie outside the fetch
     */
    public int read(ReadableByteChannel channel, int offsetInFetch, int count) throws IOException {
        Objects.checkFromIndexSize(offsetInFetch, count, length);
        int end = offsetInFetch + count;
        ChannelCursor cursor = new ChannelCursor(channel, offsetInFetch);
        int index = firstRegionEndingAfter(offsetInFetch);
        while (index < regions.size() && cursor.isBefore(end)) {
            cursor.dropUntil(Math.min(offsets[index], end));
            cursor.fillUntil(index, Math.min(regionEnd(index), end));
            index++;
        }
        cursor.dropUntil(end);
        return cursor.position() - offsetInFetch;
    }

    /**
     * Records each request's count at its index in {@code counts} and advances its target's position by that count. The
     * count of a request is what the fetch read of its region: {@code bytesRead - offsetInFetch}, clamped between 0 and
     * the request's length.
     *
     * @param bytesRead how many bytes of the fetch were read from its start, gap bytes included
     * @param counts the per-request counts of the batch
     */
    public void settle(int bytesRead, int[] counts) {
        for (int i = 0; i < regions.size(); i++) {
            int count = Math.min(regions.get(i).length(), Math.max(0, bytesRead - offsets[i]));
            counts[requestIndexes[i]] = count;
            targets[i].position(starts[i] + count);
        }
    }

    /** Returns the index of the first region ending after {@code offsetInFetch}: the region holding it, or the next. */
    private int firstRegionEndingAfter(int offsetInFetch) {
        int found = Arrays.binarySearch(offsets, offsetInFetch);
        if (found >= 0) {
            return found;
        }
        int next = -found - 1;
        if (next > 0 && regionEnd(next - 1) > offsetInFetch) {
            return next - 1;
        }
        return next;
    }

    private int regionEnd(int index) {
        return offsets[index] + regions.get(index).length();
    }

    /** Where a read from a channel stands in the fetch, and whether the stream has ended. */
    private final class ChannelCursor {
        private final ReadableByteChannel channel;
        private int position;
        private boolean ended;
        private ByteBuffer dropBuffer;

        ChannelCursor(ReadableByteChannel channel, int position) {
            this.channel = requireNonNull(channel, "channel cannot be null");
            this.position = position;
        }

        int position() {
            return position;
        }

        boolean isBefore(int offsetInFetch) {
            return !ended && position < offsetInFetch;
        }

        /** Reads and drops the bytes up to {@code offsetInFetch}. */
        void dropUntil(int offsetInFetch) throws IOException {
            while (isBefore(offsetInFetch)) {
                ByteBuffer dropped = dropBuffer();
                dropped.clear().limit(Math.min(dropped.capacity(), offsetInFetch - position));
                advance(channel.read(dropped));
            }
        }

        /** Reads the bytes up to {@code offsetInFetch} into the view of region {@code index}. */
        void fillUntil(int index, int offsetInFetch) throws IOException {
            if (!isBefore(offsetInFetch)) {
                return;
            }
            ByteBuffer view = regions.get(index).view();
            int fullLimit = view.limit();
            view.limit(starts[index] + offsetInFetch - offsets[index]);
            view.position(starts[index] + position - offsets[index]);
            try {
                while (isBefore(offsetInFetch)) {
                    advance(channel.read(view));
                }
            } finally {
                view.limit(fullLimit);
            }
        }

        private ByteBuffer dropBuffer() {
            if (dropBuffer == null) {
                dropBuffer = ByteBuffer.wrap(DROPPED_BYTES);
            }
            return dropBuffer;
        }

        private void advance(int read) {
            if (read < 0) {
                ended = true;
            } else {
                position += read;
            }
        }
    }
}
