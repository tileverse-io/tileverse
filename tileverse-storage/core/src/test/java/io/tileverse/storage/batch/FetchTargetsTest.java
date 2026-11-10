/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.batch.FetchTargets.Region;
import io.tileverse.storage.batch.PlannedFetch.Slice;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FetchTargetsTest {

    /** The bytes of a fetch: byte {@code i} of the fetch is {@code i + 1}, never 0. */
    private static byte[] fetchBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i + 1);
        }
        return bytes;
    }

    private static byte[] contents(ByteBuffer buffer, int from, int length) {
        byte[] out = new byte[length];
        buffer.get(from, out);
        return out;
    }

    private static ReadableByteChannel channelOf(byte[] bytes, int from, int length) {
        return Channels.newChannel(new ByteArrayInputStream(bytes, from, length));
    }

    /** Wraps {@code channel}, collecting the backing array of each heap buffer handed to it. */
    private static ReadableByteChannel recordingArrays(ReadableByteChannel channel, Set<byte[]> arrays) {
        return new ReadableByteChannel() {
            @Override
            public int read(ByteBuffer destination) throws IOException {
                if (destination.hasArray()) {
                    arrays.add(destination.array());
                }
                return channel.read(destination);
            }

            @Override
            public boolean isOpen() {
                return channel.isOpen();
            }

            @Override
            public void close() throws IOException {
                channel.close();
            }
        };
    }

    /** Requests of 10 bytes at 100 and 120 of the object: one fetch of 30 bytes with a gap of 10 in the middle. */
    private final List<RangeRequest> requests = List.of(
            RangeRequest.of(100, 10, ByteBuffer.allocate(10)), RangeRequest.of(120, 10, ByteBuffer.allocateDirect(10)));

    private final PlannedFetch fetchWithAGap =
            new PlannedFetch(ByteRange.of(100, 30), List.of(new Slice(0, 0, 10), new Slice(1, 20, 10)));

    @Test
    void rejectsOverlappingSlices() {
        PlannedFetch overlapping =
                new PlannedFetch(ByteRange.of(100, 30), List.of(new Slice(0, 0, 20), new Slice(1, 10, 20)));

        assertThatThrownBy(() -> FetchTargets.of(overlapping, requests)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsASliceEndingPastTheFetch() {
        PlannedFetch tooShort =
                new PlannedFetch(ByteRange.of(100, 25), List.of(new Slice(0, 0, 10), new Slice(1, 20, 10)));

        assertThatThrownBy(() -> FetchTargets.of(tooShort, requests)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsATargetWithoutRoom() {
        requests.get(1).target().position(5);

        assertThatThrownBy(() -> FetchTargets.of(fetchWithAGap, requests))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("request 1");
    }

    @Test
    void viewsStartAtTheTargetPositions() {
        ByteBuffer target = ByteBuffer.allocate(64);
        target.position(8);
        List<RangeRequest> batch = List.of(RangeRequest.of(0, 16, target));
        PlannedFetch direct = new PlannedFetch(ByteRange.of(0, 16), List.of(new Slice(0, 0, 16)));

        Region region = FetchTargets.of(direct, batch).regions().get(0);

        assertThat(region.offsetInFetch()).isZero();
        assertThat(region.length()).isEqualTo(16);
        assertThat(region.view().position()).isEqualTo(8);
        assertThat(region.view().limit()).isEqualTo(24);
    }

    @Test
    void writeDropsTheGapBytes() {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        byte[] fetched = fetchBytes(30);

        targets.write(0, ByteBuffer.wrap(fetched), 0, 30);

        assertThat(contents(requests.get(0).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 0, 10));
        assertThat(contents(requests.get(1).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 20, 30));
    }

    @Test
    void writeCutsChunksAtRegionEnds() {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        byte[] fetched = fetchBytes(30);
        ByteBuffer source = ByteBuffer.wrap(fetched);

        for (int offset = 0; offset < 30; offset += 7) {
            targets.write(offset, source, offset, Math.min(7, 30 - offset));
        }

        assertThat(contents(requests.get(0).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 0, 10));
        assertThat(contents(requests.get(1).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 20, 30));
        assertThat(source.position()).isZero();
    }

    @Test
    void laterWriteOverwrites() {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        byte[] retried = fetchBytes(30);

        targets.write(0, ByteBuffer.wrap(new byte[30]), 0, 30);
        targets.write(0, ByteBuffer.wrap(retried), 0, 30);

        assertThat(contents(requests.get(1).target(), 0, 10)).containsExactly(Arrays.copyOfRange(retried, 20, 30));
    }

    @Test
    void writeRejectsBytesPastTheFetch() {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        ByteBuffer source = ByteBuffer.wrap(fetchBytes(30));

        assertThatThrownBy(() -> targets.write(25, source, 0, 10)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void settleMovesPositionsOnce() {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        int[] counts = new int[2];

        targets.write(0, ByteBuffer.wrap(fetchBytes(30)), 0, 30);
        assertThat(requests.get(0).target().position()).isZero();
        targets.settle(30, counts);

        assertThat(counts).containsExactly(10, 10);
        assertThat(requests.get(0).target().position()).isEqualTo(10);
        assertThat(requests.get(1).target().position()).isEqualTo(10);
    }

    @Test
    void settleClampsAShortFetch() {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        int[] counts = new int[2];

        targets.settle(15, counts);

        assertThat(counts).containsExactly(10, 0);
        assertThat(requests.get(1).target().position()).isZero();
    }

    @Test
    void writesFromTheTargetPositions() {
        ByteBuffer first = ByteBuffer.allocate(20);
        first.position(4);
        ByteBuffer second = ByteBuffer.allocateDirect(20);
        second.position(7);
        List<RangeRequest> batch = List.of(RangeRequest.of(0, 5, first), RangeRequest.of(5, 5, second));
        PlannedFetch abutting = new PlannedFetch(ByteRange.of(0, 10), List.of(new Slice(0, 0, 5), new Slice(1, 5, 5)));
        FetchTargets targets = FetchTargets.of(abutting, batch);
        byte[] fetched = fetchBytes(10);
        int[] counts = new int[2];

        targets.write(0, ByteBuffer.wrap(fetched), 0, 10);
        targets.settle(10, counts);

        assertThat(contents(first, 4, 5)).containsExactly(Arrays.copyOfRange(fetched, 0, 5));
        assertThat(contents(second, 7, 5)).containsExactly(Arrays.copyOfRange(fetched, 5, 10));
        assertThat(first.position()).isEqualTo(9);
        assertThat(second.position()).isEqualTo(12);
    }

    @Test
    void readDropsTheGapBytes() throws IOException {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        byte[] fetched = fetchBytes(30);

        int consumed = targets.read(channelOf(fetched, 0, 30), 0, 30);

        assertThat(consumed).isEqualTo(30);
        assertThat(contents(requests.get(0).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 0, 10));
        assertThat(contents(requests.get(1).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 20, 30));
        assertThat(requests.get(0).target().position()).isZero();
    }

    @Test
    void readStopsAtTheEndOfStream() throws IOException {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        byte[] fetched = fetchBytes(30);
        int[] counts = new int[2];

        int consumed = targets.read(channelOf(fetched, 0, 25), 0, 30);
        targets.settle(consumed, counts);

        assertThat(consumed).isEqualTo(25);
        assertThat(counts).containsExactly(10, 5);
        assertThat(contents(requests.get(1).target(), 0, 5)).containsExactly(Arrays.copyOfRange(fetched, 20, 25));
    }

    @Test
    void readResumesAtAnOffset() throws IOException {
        FetchTargets targets = FetchTargets.of(fetchWithAGap, requests);
        byte[] fetched = fetchBytes(30);

        int first = targets.read(channelOf(fetched, 0, 24), 0, 30);
        int rest = targets.read(channelOf(fetched, first, 30 - first), first, 30 - first);

        assertThat(first + rest).isEqualTo(30);
        assertThat(contents(requests.get(1).target(), 0, 10)).containsExactly(Arrays.copyOfRange(fetched, 20, 30));
    }

    @Test
    void readDropsAGapLongerThanItsBuffer() throws IOException {
        int gap = 20_000;
        List<RangeRequest> batch = List.of(
                RangeRequest.of(0, 10, ByteBuffer.allocate(10)),
                RangeRequest.of(10 + gap, 10, ByteBuffer.allocate(10)));
        PlannedFetch wideGap =
                new PlannedFetch(ByteRange.of(0, 20 + gap), List.of(new Slice(0, 0, 10), new Slice(1, 10 + gap, 10)));
        FetchTargets targets = FetchTargets.of(wideGap, batch);
        byte[] fetched = fetchBytes(20 + gap);

        int consumed = targets.read(channelOf(fetched, 0, fetched.length), 0, fetched.length);

        assertThat(consumed).isEqualTo(fetched.length);
        assertThat(contents(batch.get(1).target(), 0, 10))
                .containsExactly(Arrays.copyOfRange(fetched, 10 + gap, 20 + gap));
    }

    // The targets are direct: the only heap buffers handed to the channel receive the dropped gap bytes.
    @Test
    void readsShareTheDropArray() throws IOException {
        Set<byte[]> dropArrays = new HashSet<>();
        byte[] fetched = fetchBytes(30);

        for (int read = 0; read < 2; read++) {
            List<RangeRequest> batch = List.of(
                    RangeRequest.of(100, 10, ByteBuffer.allocateDirect(10)),
                    RangeRequest.of(120, 10, ByteBuffer.allocateDirect(10)));
            FetchTargets targets = FetchTargets.of(fetchWithAGap, batch);
            targets.read(recordingArrays(channelOf(fetched, 0, 30), dropArrays), 0, 30);
        }

        assertThat(dropArrays).hasSize(1);
    }

    /** Targets carved out of one larger buffer: the bytes around each region must stay as they were. */
    @Test
    void writesStayInsideTheRegions() {
        ByteBuffer backing = ByteBuffer.allocate(40);
        for (int i = 0; i < 40; i++) {
            backing.put(i, (byte) -1);
        }
        ByteBuffer first = backing.slice(5, 15).position(2).limit(12);
        ByteBuffer second = backing.slice(25, 15).position(3).limit(13);
        List<RangeRequest> batch = List.of(RangeRequest.of(100, 10, first), RangeRequest.of(120, 10, second));
        FetchTargets targets = FetchTargets.of(fetchWithAGap, batch);

        targets.write(0, ByteBuffer.wrap(fetchBytes(30)), 0, 30);

        assertThat(contents(backing, 0, 7)).containsOnly((byte) -1);
        assertThat(contents(backing, 17, 11)).containsOnly((byte) -1);
        assertThat(contents(backing, 38, 2)).containsOnly((byte) -1);
    }
}
