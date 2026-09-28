/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchReadResultTest {

    private static final List<RangeRequest> THREE = List.of(
            RangeRequest.of(0, 100, ByteBuffer.allocate(100)),
            RangeRequest.of(500, 0, ByteBuffer.allocate(10)),
            RangeRequest.of(1000, 50, ByteBuffer.allocate(50)));

    @Test
    void reportsThePerRequestCountsAndTheCost() {
        BatchReadResult result = BatchReadResult.of(THREE, new int[] {100, 0, 30}, 2, 180, 0);

        assertThat(result.requests()).isEqualTo(3);
        assertThat(result.bytesRead(0)).isEqualTo(100);
        assertThat(result.bytesRead(1)).isZero();
        assertThat(result.bytesRead(2)).isEqualTo(30);
        assertThat(result.bytesRequested()).isEqualTo(150);
        assertThat(result.fetches()).isEqualTo(2);
        assertThat(result.bytesTransferred()).isEqualTo(180);
        assertThat(result.bytesFromCache()).isZero();
    }

    @Test
    void countsAreCopiedNotShared() {
        int[] counts = {100, 0, 50};
        BatchReadResult result = BatchReadResult.of(THREE, counts, 2, 150, 0);

        counts[0] = 7;

        assertThat(result.bytesRead(0)).isEqualTo(100);
    }

    @Test
    void perRangeCountsOneFetchPerNonEmptyRangeAndTransfersWhatItRead() {
        BatchReadResult result = BatchReadResult.perRange(THREE, new int[] {100, 0, 30});

        assertThat(result.fetches()).isEqualTo(2);
        assertThat(result.bytesTransferred()).isEqualTo(130);
        assertThat(result.bytesRequested()).isEqualTo(150);
        assertThat(result.bytesFromCache()).isZero();
    }

    @Test
    void mergeKeepsThisViewAndSumsTheTransportNumbers() {
        BatchReadResult above = BatchReadResult.of(THREE, new int[] {100, 0, 50}, 0, 0, 100);
        BatchReadResult below = BatchReadResult.of(
                List.of(RangeRequest.of(1000, 50, ByteBuffer.allocate(50))), new int[] {50}, 1, 60, 5);

        BatchReadResult merged = above.merge(below);

        assertThat(merged.requests()).isEqualTo(3);
        assertThat(merged.bytesRead(2)).isEqualTo(50);
        assertThat(merged.bytesRequested()).isEqualTo(150);
        assertThat(merged.fetches()).isEqualTo(1);
        assertThat(merged.bytesTransferred()).isEqualTo(60);
        assertThat(merged.bytesFromCache()).isEqualTo(105);
    }

    @Test
    void emptyResultHasNoRequestsAndNoCost() {
        assertThat(BatchReadResult.EMPTY.requests()).isZero();
        assertThat(BatchReadResult.EMPTY.fetches()).isZero();
        assertThat(BatchReadResult.EMPTY.bytesTransferred()).isZero();
        assertThat(BatchReadResult.EMPTY.bytesRequested()).isZero();
    }

    @Test
    void rejectsMismatchedCountsAndNegativeNumbers() {
        assertThatThrownBy(() -> BatchReadResult.of(THREE, new int[] {1, 2}, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchReadResult.of(THREE, new int[] {1, 2, -3}, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchReadResult.of(THREE, new int[] {1, 2, 3}, -1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchReadResult.of(THREE, new int[] {1, 2, 3}, 0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BatchReadResult.of(THREE, new int[] {1, 2, 3}, 0, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void equalityCoversTheCountsAndTheCost() {
        BatchReadResult one = BatchReadResult.of(THREE, new int[] {100, 0, 50}, 2, 150, 0);
        BatchReadResult same = BatchReadResult.of(THREE, new int[] {100, 0, 50}, 2, 150, 0);
        BatchReadResult other = BatchReadResult.of(THREE, new int[] {100, 0, 50}, 2, 151, 0);

        assertThat(one).isEqualTo(same).hasSameHashCodeAs(same).isNotEqualTo(other);
        assertThat(one.toString()).contains("fetches=2").contains("[100, 0, 50]");
    }
}
