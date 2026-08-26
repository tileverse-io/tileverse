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

import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.batch.PlannedFetch.Slice;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchPlannerTest {

    private static RangeRequest request(long offset, int length) {
        return RangeRequest.of(offset, length, ByteBuffer.allocate(Math.max(1, length)));
    }

    @Test
    void noneYieldsOneDirectFetchPerRequestInRequestOrder() {
        List<RangeRequest> requests = List.of(request(100, 10), request(0, 10), request(100, 10));

        List<PlannedFetch> fetches = BatchPlanner.plan(requests, CoalescingPolicy.NONE);

        assertThat(fetches).hasSize(3);
        assertThat(fetches.get(0).range()).isEqualTo(ByteRange.of(100, 10));
        assertThat(fetches.get(1).range()).isEqualTo(ByteRange.of(0, 10));
        assertThat(fetches.get(2).range()).isEqualTo(ByteRange.of(100, 10));
        assertThat(fetches).allMatch(PlannedFetch::isDirect);
        assertThat(fetches.get(2).slices().get(0).requestIndex()).isEqualTo(2);
    }

    @Test
    void zeroLengthRequestsNeverReachAFetch() {
        List<RangeRequest> requests = List.of(request(0, 0), request(50, 0));
        assertThat(BatchPlanner.plan(requests, CoalescingPolicy.NONE)).isEmpty();
        assertThat(BatchPlanner.plan(requests, new CoalescingPolicy(1024, 4096)))
                .isEmpty();
    }

    @Test
    void mergesRequestsWithinTheGapBudget() {
        List<RangeRequest> requests = List.of(request(0, 10), request(20, 10));

        List<PlannedFetch> fetches = BatchPlanner.plan(requests, new CoalescingPolicy(16, 1000));

        assertThat(fetches).hasSize(1);
        PlannedFetch fetch = fetches.get(0);
        assertThat(fetch.range()).isEqualTo(ByteRange.of(0, 30));
        assertThat(fetch.slices()).containsExactly(new Slice(0, 0, 10), new Slice(1, 20, 10));
    }

    @Test
    void gapEqualToTheBudgetMergesOneByteMoreDoesNot() {
        CoalescingPolicy policy = new CoalescingPolicy(16, 1000);
        assertThat(BatchPlanner.plan(List.of(request(0, 10), request(26, 10)), policy))
                .hasSize(1);
        assertThat(BatchPlanner.plan(List.of(request(0, 10), request(27, 10)), policy))
                .hasSize(2);
    }

    @Test
    void maxFetchBytesStartsANewFetch() {
        CoalescingPolicy policy = new CoalescingPolicy(1000, 25);
        assertThat(BatchPlanner.plan(List.of(request(0, 10), request(15, 10)), policy))
                .hasSize(1);
        assertThat(BatchPlanner.plan(List.of(request(0, 10), request(16, 10)), policy))
                .hasSize(2);
    }

    @Test
    void overlappingAndDuplicateRangesAlwaysQualifyForTheCurrentFetch() {
        CoalescingPolicy zeroGap = new CoalescingPolicy(0, 1000);

        List<PlannedFetch> overlapping = BatchPlanner.plan(List.of(request(0, 100), request(50, 100)), zeroGap);
        assertThat(overlapping).hasSize(1);
        assertThat(overlapping.get(0).range()).isEqualTo(ByteRange.of(0, 150));

        List<PlannedFetch> duplicates = BatchPlanner.plan(List.of(request(10, 20), request(10, 20)), zeroGap);
        assertThat(duplicates).hasSize(1);
        assertThat(duplicates.get(0).slices()).containsExactly(new Slice(0, 0, 20), new Slice(1, 0, 20));
    }

    @Test
    void containedRangeBecomesASliceWithoutGrowingTheFetch() {
        List<PlannedFetch> fetches =
                BatchPlanner.plan(List.of(request(0, 100), request(10, 20)), new CoalescingPolicy(0, 1000));

        assertThat(fetches).hasSize(1);
        assertThat(fetches.get(0).range()).isEqualTo(ByteRange.of(0, 100));
        assertThat(fetches.get(0).slices()).containsExactly(new Slice(0, 0, 100), new Slice(1, 10, 20));
    }

    @Test
    void aFetchAlwaysAcceptsItsFirstRequestEvenPastTheCap() {
        List<PlannedFetch> fetches = BatchPlanner.plan(List.of(request(0, 100)), new CoalescingPolicy(0, 10));

        assertThat(fetches).hasSize(1);
        assertThat(fetches.get(0).range()).isEqualTo(ByteRange.of(0, 100));
    }

    @Test
    void equalOffsetsKeepRequestOrder() {
        List<PlannedFetch> fetches =
                BatchPlanner.plan(List.of(request(5, 10), request(5, 4)), new CoalescingPolicy(0, 1000));

        assertThat(fetches).hasSize(1);
        assertThat(fetches.get(0).slices()).containsExactly(new Slice(0, 0, 10), new Slice(1, 0, 4));
    }

    @Test
    void unorderedRequestsComeBackAsAscendingFetchesWithOriginalIndices() {
        List<RangeRequest> requests = List.of(request(1000, 10), request(0, 10));

        List<PlannedFetch> fetches = BatchPlanner.plan(requests, new CoalescingPolicy(0, 10_000));

        assertThat(fetches).hasSize(2);
        assertThat(fetches.get(0).range()).isEqualTo(ByteRange.of(0, 10));
        assertThat(fetches.get(0).slices().get(0).requestIndex()).isEqualTo(1);
        assertThat(fetches.get(1).range()).isEqualTo(ByteRange.of(1000, 10));
        assertThat(fetches.get(1).slices().get(0).requestIndex()).isZero();
    }

    @Test
    void plannerNeverTouchesTargets() {
        RangeRequest request = request(0, 10);
        BatchPlanner.plan(List.of(request), new CoalescingPolicy(0, 1000));
        assertThat(request.target().position()).isZero();
    }
}
