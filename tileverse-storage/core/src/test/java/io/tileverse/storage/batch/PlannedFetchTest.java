/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.batch.PlannedFetch.Slice;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class PlannedFetchTest {

    @Test
    void validatesRangeAndSlices() {
        List<Slice> oneSlice = List.of(new Slice(0, 0, 1));
        assertThatThrownBy(() -> new PlannedFetch(null, oneSlice)).isInstanceOf(NullPointerException.class);
        ByteRange range = ByteRange.of(0, 10);
        List<Slice> noSlices = List.of();
        assertThatThrownBy(() -> new PlannedFetch(range, noSlices)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void directMeansOneSliceCoveringTheWholeFetch() {
        assertThat(new PlannedFetch(ByteRange.of(0, 10), List.of(new Slice(0, 0, 10))).isDirect())
                .isTrue();
        assertThat(new PlannedFetch(ByteRange.of(0, 30), List.of(new Slice(0, 0, 10), new Slice(1, 20, 10))).isDirect())
                .isFalse();
        assertThat(new PlannedFetch(ByteRange.of(0, 30), List.of(new Slice(0, 10, 20))).isDirect())
                .isFalse();
    }

    @Test
    void scatterCopiesEachSliceIntoItsTarget() {
        byte[] fetchedBytes = new byte[30];
        for (int i = 0; i < fetchedBytes.length; i++) {
            fetchedBytes[i] = (byte) i;
        }
        List<RangeRequest> requests = List.of(
                RangeRequest.of(100, 10, ByteBuffer.allocate(10)), RangeRequest.of(120, 10, ByteBuffer.allocate(10)));
        PlannedFetch fetch =
                new PlannedFetch(ByteRange.of(100, 30), List.of(new Slice(0, 0, 10), new Slice(1, 20, 10)));
        int[] counts = new int[2];

        fetch.scatter(ByteBuffer.wrap(fetchedBytes), 30, requests, counts);

        assertThat(counts).containsExactly(10, 10);
        assertThat(requests.get(0).target().flip())
                .isEqualTo(ByteBuffer.wrap(fetchedBytes, 0, 10).slice());
        assertThat(requests.get(1).target().flip())
                .isEqualTo(ByteBuffer.wrap(fetchedBytes, 20, 10).slice());
    }

    @Test
    void scatterClampsSlicesToTheShortFetch() {
        List<RangeRequest> requests = List.of(
                RangeRequest.of(0, 10, ByteBuffer.allocate(10)),
                RangeRequest.of(10, 10, ByteBuffer.allocate(10)),
                RangeRequest.of(25, 5, ByteBuffer.allocate(5)));
        PlannedFetch fetch = new PlannedFetch(
                ByteRange.of(0, 30), List.of(new Slice(0, 0, 10), new Slice(1, 10, 10), new Slice(2, 25, 5)));
        int[] counts = new int[3];

        fetch.scatter(ByteBuffer.wrap(new byte[30]), 15, requests, counts);

        assertThat(counts).containsExactly(10, 5, 0);
        assertThat(requests.get(0).target().position()).isEqualTo(10);
        assertThat(requests.get(1).target().position()).isEqualTo(5);
        assertThat(requests.get(2).target().position()).isZero();
    }

    @Test
    void scatterAppendsAtTheTargetPositionAndReadsReadOnlySources() {
        ByteBuffer target = ByteBuffer.allocate(20);
        target.position(4);
        List<RangeRequest> requests = List.of(RangeRequest.of(0, 10, target));
        PlannedFetch fetch = new PlannedFetch(ByteRange.of(0, 10), List.of(new Slice(0, 0, 10)));
        int[] counts = new int[1];
        ByteBuffer fetched =
                ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).asReadOnlyBuffer();

        fetch.scatter(fetched, 10, requests, counts);

        assertThat(counts[0]).isEqualTo(10);
        assertThat(target.position()).isEqualTo(14);
        assertThat(target.get(4)).isEqualTo((byte) 1);
        assertThat(fetched.position()).isZero();
    }
}
