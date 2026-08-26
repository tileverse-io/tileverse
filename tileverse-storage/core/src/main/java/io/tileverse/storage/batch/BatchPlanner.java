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

import static java.util.Objects.requireNonNull;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.batch.PlannedFetch.Slice;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure planning: turns a validated batch of range requests into the fetches worth issuing downstream, merging requests
 * whose gap costs less than a round trip under the given {@link CoalescingPolicy}.
 *
 * <p>Zero-length requests never reach a fetch; their result entries stay 0. With {@link CoalescingPolicy#NONE} every
 * non-empty request becomes its own direct fetch in request order. Under a merging policy, requests are sorted by
 * offset with an explicit index-tagged comparator ({@link ByteRange#compareTo} orders by offset only and is
 * inconsistent with equals; it plays no part here), and a request joins the current fetch when its gap from the fetch
 * end is at most {@link CoalescingPolicy#maxGapBytes()} (overlapping and duplicate ranges have negative gaps and always
 * qualify) and the merged fetch stays within {@link CoalescingPolicy#maxFetchBytes()}. A fetch always accepts its first
 * request, even one longer than the cap: a single contiguous request cannot be split.
 *
 * <p>Fetches come back in ascending offset order (request order under {@code NONE}). Worst-case amplification is the
 * requested bytes plus the merged gaps, never more than {@code maxFetchBytes} per fetch.
 *
 * <p>The planner performs no I/O and never touches request targets.
 */
public final class BatchPlanner {

    private BatchPlanner() {}

    private record IndexedRange(int index, ByteRange range) {}

    private static final Comparator<IndexedRange> BY_OFFSET_THEN_INDEX = Comparator.comparingLong(
                    (IndexedRange tagged) -> tagged.range().offset())
            .thenComparingInt(IndexedRange::index);

    /**
     * Plans the downstream fetches for a validated batch.
     *
     * @param requests the batch, already validated by {@link RangeRequest#validate(List)}
     * @param policy the merge policy
     * @return the planned fetches; empty when no request needs any byte
     */
    public static List<PlannedFetch> plan(List<RangeRequest> requests, CoalescingPolicy policy) {
        requireNonNull(requests, "requests cannot be null");
        requireNonNull(policy, "policy cannot be null");
        List<IndexedRange> ranges = nonEmptyRanges(requests);
        if (ranges.isEmpty()) {
            return List.of();
        }
        if (policy.maxGapBytes() < 0) {
            return directFetches(ranges);
        }
        return mergedFetches(ranges, policy);
    }

    private static List<IndexedRange> nonEmptyRanges(List<RangeRequest> requests) {
        List<IndexedRange> ranges = new ArrayList<>(requests.size());
        for (int i = 0; i < requests.size(); i++) {
            ByteRange range = requests.get(i).range();
            if (range.length() > 0) {
                ranges.add(new IndexedRange(i, range));
            }
        }
        return ranges;
    }

    private static List<PlannedFetch> directFetches(List<IndexedRange> ranges) {
        List<PlannedFetch> fetches = new ArrayList<>(ranges.size());
        for (IndexedRange tagged : ranges) {
            Slice slice = new Slice(tagged.index(), 0, tagged.range().length());
            fetches.add(new PlannedFetch(tagged.range(), List.of(slice)));
        }
        return fetches;
    }

    private static List<PlannedFetch> mergedFetches(List<IndexedRange> ranges, CoalescingPolicy policy) {
        List<IndexedRange> sorted = new ArrayList<>(ranges);
        sorted.sort(BY_OFFSET_THEN_INDEX);

        List<PlannedFetch> fetches = new ArrayList<>();
        List<IndexedRange> members = new ArrayList<>();
        long start = 0;
        long end = 0;
        for (IndexedRange candidate : sorted) {
            if (members.isEmpty()) {
                start = candidate.range().offset();
                end = candidate.range().end();
                members.add(candidate);
                continue;
            }
            long gap = candidate.range().offset() - end;
            long mergedEnd = Math.max(end, candidate.range().end());
            long mergedLength = mergedEnd - start;
            if (gap <= policy.maxGapBytes() && mergedLength <= policy.maxFetchBytes()) {
                end = mergedEnd;
                members.add(candidate);
            } else {
                fetches.add(toFetch(start, end, members));
                members = new ArrayList<>();
                start = candidate.range().offset();
                end = candidate.range().end();
                members.add(candidate);
            }
        }
        fetches.add(toFetch(start, end, members));
        return fetches;
    }

    private static PlannedFetch toFetch(long start, long end, List<IndexedRange> members) {
        List<Slice> slices = new ArrayList<>(members.size());
        for (IndexedRange member : members) {
            int offsetInFetch = (int) (member.range().offset() - start);
            slices.add(new Slice(member.index(), offsetInFetch, member.range().length()));
        }
        return new PlannedFetch(new ByteRange(start, (int) (end - start)), slices);
    }
}
