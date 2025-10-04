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
package io.tileverse.tiling.pyramid;

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Utility methods for working with {@link TileRange} instances.
 */
final class TileRangeUtil {

    private TileRangeUtil() {
        // Utility class - no instantiation
    }

    public static Optional<TileRange> intersection(TileRange range1, TileRange range2) {
        if (requireNonNull(range2).zoomLevel() != requireNonNull(range1).zoomLevel()) {
            throw new IllegalArgumentException("Cannot intersect ranges at different zoom levels: " + range1.zoomLevel()
                    + " and " + range2.zoomLevel());
        }
        if (range1 instanceof TileRangeImpl contiguous1 && range2 instanceof TileRangeImpl contiguous2) {
            return intersectContiguousRanges(contiguous1, contiguous2);
        }

        SparseTileRange sparse;
        TileRange other;
        if (range1 instanceof SparseTileRange s) {
            sparse = s;
            other = range2;
        } else if (range2 instanceof SparseTileRange s) {
            sparse = s;
            other = range1;
        } else {
            throw new IllegalArgumentException("Unknown TileRange type, expected SparseTileRange or TileRangeImpl");
        }

        // if both are sparse it doesn't matter, will recurse back here for each part
        return intersectSparse(sparse, other);
    }

    private static Optional<TileRange> intersectSparse(SparseTileRange sparse, TileRange other) {
        List<TileRange> intersecting = sparse.parts().stream()
                .map(range -> range.intersection(other))
                .filter(Optional::isPresent)
                .map(Optional::orElseThrow)
                .toList();

        return switch (intersecting.size()) {
            case 0 -> Optional.empty();
            case 1 -> Optional.of(intersecting.get(0));
            default -> Optional.of(TileRangeUtil.union(intersecting));
        };
    }

    private static Optional<TileRange> intersectContiguousRanges(TileRangeImpl range, TileRangeImpl other) {
        // Transform other range to same corner of origin if needed
        TileRange compatible = other.withCornerOfOrigin(range.cornerOfOrigin());

        long minX = Math.max(range.minx(), compatible.minx());
        long minY = Math.max(range.miny(), compatible.miny());
        long maxX = Math.min(range.maxx(), compatible.maxx());
        long maxY = Math.min(range.maxy(), compatible.maxy());

        // Ensure the intersection is valid
        if (minX > maxX || minY > maxY) {
            // No intersection - return empty range using MAX_VALUE coordinates as a marker
            return Optional.empty();
        }

        return Optional.of(TileRange.of(minX, minY, maxX, maxY, range.zoomLevel(), range.cornerOfOrigin()));
    }

    public static TileRange union(TileRange tileRange, TileRange other) {
        if (requireNonNull(tileRange, "tileRange").contains(requireNonNull(other, "other"))) {
            return tileRange;
        }
        if (other.contains(tileRange)) {
            return other;
        }
        return union(List.of(tileRange, other));
    }

    public static TileRange union(List<TileRange> ranges) {
        if (requireNonNull(ranges).isEmpty()) {
            throw new IllegalArgumentException("Union requires at least one part");
        }
        final int zoomLevel = ranges.get(0).zoomLevel();
        final CornerOfOrigin cornerOfOrigin = ranges.get(0).cornerOfOrigin();

        List<TileRange> compatible = ranges.stream()
                .map(range -> {
                    if (range.zoomLevel() != zoomLevel) {
                        throw new IllegalArgumentException("Cannot union ranges at different zoom levels: %d and %d"
                                .formatted(zoomLevel, range.zoomLevel()));
                    }
                    return range.withCornerOfOrigin(cornerOfOrigin);
                })
                .toList();

        List<TileRange> partitioned = partitionIntoNonOverlappingRanges(compatible);
        return switch (partitioned.size()) {
            case 1 -> partitioned.get(0);
            default -> new SparseTileRange(partitioned);
        };
    }

    /**
     * Partitions a list of potentially overlapping tile ranges into non-overlapping parts.
     * For any pair of {@link TileRange#intersects(TileRange) intersecting} ranges in {@code ranges},
     * a set of new non-intersecting ranges will be created and returned in place of the originals.
     * <p>
     * For example:
     * <pre>{@code
     * +-----------+
     * |           |
     * |    +-------------+
     * |    |      |      |
     * +-----------+      |
     *      |             |
     *      +-------------+
     * }</pre>
     *
     * Will produce:
     *
     * <pre>{@code
     * +-----------+
     * |           |
     * |           +------+
     * |           |      |
     * +----+------+      |
     *      |      |      |
     *      +------+------+
     * }</pre>
     *
     * @param ranges the list of tile ranges to partition (may contain overlaps)
     * @return a list of non-overlapping tile ranges covering the same total area
     * @implNote uses a <a href="https://en.wikipedia.org/wiki/Sweep_line_algorithm">Sweep line algorithm</a> to partition overlapping ranges
     */
    static List<TileRange> partitionIntoNonOverlappingRanges(List<TileRange> ranges) {
        if (requireNonNull(ranges).isEmpty()) {
            return List.of();
        }
        if (ranges.size() == 1) {
            return List.copyOf(ranges);
        }
        ranges = flattenSparseRanges(ranges);

        // Use sweep line algorithm to partition overlapping rectangles
        List<TileRange> result = new ArrayList<>();
        List<TileRange> remaining = new ArrayList<>(ranges);
        Collections.sort(remaining);

        while (!remaining.isEmpty()) {
            TileRange current = remaining.remove(0);
            List<TileRange> newParts = new ArrayList<>();
            newParts.add(current);

            // Check for intersections with remaining ranges
            for (int i = 0; i < remaining.size(); i++) {
                TileRange other = remaining.get(i);

                if (current.intersects(other)) {
                    // Remove the intersecting range - we'll split it
                    remaining.remove(i);
                    i--; // Adjust index after removal

                    // Split both ranges into non-overlapping parts
                    List<TileRange> splitResult = splitTwoRanges(current, other);

                    // Replace current with the split results
                    newParts.clear();
                    newParts.addAll(splitResult);

                    // Update current to continue checking against other ranges
                    current = createBoundingRange(splitResult);
                    break;
                }
            }

            // If no intersections found, add current to result
            if (newParts.size() == 1) {
                result.add(current);
            } else {
                // Recursively process the split parts
                List<TileRange> nonOverlapping = partitionIntoNonOverlappingRanges(newParts);
                result.addAll(nonOverlapping);
            }
        }

        return consolidateAdjacentRanges(result);
    }

    private static List<TileRange> flattenSparseRanges(List<TileRange> ranges) {
        List<TileRange> flattened = new ArrayList<>();
        for (TileRange part : ranges) {
            if (part instanceof SparseTileRange sparse) {
                flattened.addAll(sparse.parts());
            } else {
                flattened.add(part);
            }
        }
        return flattened;
    }

    /**
     * Splits two intersecting tile ranges into non-overlapping parts.
     * Returns a list containing the non-overlapping rectangles that cover
     * the union of the two input ranges.
     */
    private static List<TileRange> splitTwoRanges(TileRange a, TileRange b) {
        // Check containment cases first for optimization
        if (a.contains(b)) {
            return List.of(a);
        }
        if (b.contains(a)) {
            return List.of(b);
        }

        // Find the intersection
        Optional<TileRange> intersectionOpt = a.intersection(b);
        if (intersectionOpt.isEmpty()) {
            // No intersection, return both ranges as-is
            return List.of(a, b);
        }

        TileRange intersection = intersectionOpt.get();

        // Check if we can keep one range intact and just subtract the intersection from the other
        // This is more efficient when one range is much larger or when it leads to fewer parts
        List<TileRange> aParts = subtractRange(a, intersection);
        List<TileRange> bParts = subtractRange(b, intersection);

        // Strategy 1: Keep A intact, split B
        if (aParts.isEmpty()) {
            // A is completely covered by intersection, keep B and A's remainder parts
            List<TileRange> result = new ArrayList<>(bParts);
            result.add(a);
            return result;
        }

        // Strategy 2: Keep B intact, split A
        if (bParts.isEmpty()) {
            // B is completely covered by intersection, keep A and B's remainder parts
            List<TileRange> result = new ArrayList<>(aParts);
            result.add(b);
            return result;
        }

        // Strategy 3: Check if keeping one range intact results in fewer total parts
        int splitBothCount = aParts.size() + bParts.size() + 1; // +1 for intersection
        int keepACount = bParts.size() + 1; // Keep A intact, split B
        int keepBCount = aParts.size() + 1; // Keep B intact, split A

        if (keepACount <= keepBCount && keepACount < splitBothCount) {
            // Keep A intact, use B parts
            List<TileRange> result = new ArrayList<>(bParts);
            result.add(a);
            return result;
        }

        if (keepBCount < splitBothCount) {
            // Keep B intact, use A parts
            List<TileRange> result = new ArrayList<>(aParts);
            result.add(b);
            return result;
        }

        // Default: split both ranges
        List<TileRange> parts = new ArrayList<>();
        parts.addAll(aParts);
        parts.addAll(bParts);
        parts.add(intersection);
        return parts;
    }

    /**
     * Subtracts the second range from the first, returning the non-overlapping parts
     * of the first range that don't intersect with the second.
     */
    private static List<TileRange> subtractRange(TileRange from, TileRange subtract) {
        if (!from.intersects(subtract)) {
            return List.of(from);
        }

        List<TileRange> parts = new ArrayList<>();

        // Create up to 4 non-overlapping rectangles around the subtracted area
        long fromMinX = from.minx(), fromMinY = from.miny();
        long fromMaxX = from.maxx(), fromMaxY = from.maxy();
        long subMinX = subtract.minx(), subMinY = subtract.miny();
        long subMaxX = subtract.maxx(), subMaxY = subtract.maxy();

        // Left part (west of subtract)
        if (fromMinX < subMinX) {
            parts.add(TileRange.of(fromMinX, fromMinY, subMinX - 1, fromMaxY, from.zoomLevel(), from.cornerOfOrigin()));
        }

        // Right part (east of subtract)
        if (fromMaxX > subMaxX) {
            parts.add(TileRange.of(subMaxX + 1, fromMinY, fromMaxX, fromMaxY, from.zoomLevel(), from.cornerOfOrigin()));
        }

        // Top part (north of subtract, within X bounds)
        long effectiveMinX = Math.max(fromMinX, subMinX);
        long effectiveMaxX = Math.min(fromMaxX, subMaxX);
        if (fromMaxY > subMaxY && effectiveMinX <= effectiveMaxX) {
            parts.add(TileRange.of(
                    effectiveMinX, subMaxY + 1, effectiveMaxX, fromMaxY, from.zoomLevel(), from.cornerOfOrigin()));
        }

        // Bottom part (south of subtract, within X bounds)
        if (fromMinY < subMinY && effectiveMinX <= effectiveMaxX) {
            parts.add(TileRange.of(
                    effectiveMinX, fromMinY, effectiveMaxX, subMinY - 1, from.zoomLevel(), from.cornerOfOrigin()));
        }

        return parts;
    }

    /**
     * Creates a bounding range that encompasses all the given ranges.
     */
    private static TileRange createBoundingRange(List<TileRange> ranges) {
        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("Cannot create bounding range from empty list");
        }

        TileRange first = ranges.get(0);
        long minX = first.minx(), minY = first.miny();
        long maxX = first.maxx(), maxY = first.maxy();

        for (int i = 1; i < ranges.size(); i++) {
            TileRange range = ranges.get(i);
            minX = Math.min(minX, range.minx());
            minY = Math.min(minY, range.miny());
            maxX = Math.max(maxX, range.maxx());
            maxY = Math.max(maxY, range.maxy());
        }

        return TileRange.of(minX, minY, maxX, maxY, first.zoomLevel(), first.cornerOfOrigin());
    }

    /**
     * Consolidates adjacent or touching tile ranges into larger ranges where possible.
     * Two ranges can be merged if they share an edge and form a rectangular union.
     */
    private static List<TileRange> consolidateAdjacentRanges(List<TileRange> ranges) {
        if (ranges.size() <= 1) {
            return ranges;
        }

        List<TileRange> result = new ArrayList<>(ranges);
        boolean changed;

        do {
            changed = false;
            for (int i = 0; i < result.size() && !changed; i++) {
                for (int j = i + 1; j < result.size() && !changed; j++) {
                    TileRange a = result.get(i);
                    TileRange b = result.get(j);

                    Optional<TileRange> merged = tryMergeRanges(a, b);
                    if (merged.isPresent()) {
                        result.set(i, merged.get());
                        result.remove(j);
                        changed = true;
                    }
                }
            }
        } while (changed);

        return result;
    }

    /**
     * Attempts to merge two ranges if they are adjacent or touching and form a rectangle.
     */
    private static Optional<TileRange> tryMergeRanges(TileRange a, TileRange b) {
        // Try to create the union rectangle
        long minX = Math.min(a.minx(), b.minx());
        long minY = Math.min(a.miny(), b.miny());
        long maxX = Math.max(a.maxx(), b.maxx());
        long maxY = Math.max(a.maxy(), b.maxy());

        TileRange unionRect = TileRange.of(minX, minY, maxX, maxY, a.zoomLevel(), a.cornerOfOrigin());

        // Check if the union rectangle's area equals the sum of the two ranges' areas
        // This means the ranges are adjacent/touching with no gap and no overlap
        if (unionRect.count() == a.count() + b.count()) {
            return Optional.of(unionRect);
        }

        return Optional.empty();
    }
}
