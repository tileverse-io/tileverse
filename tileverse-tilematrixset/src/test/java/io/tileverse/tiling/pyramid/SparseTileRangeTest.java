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

import static io.tileverse.tiling.pyramid.TileIndex.xyz;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tileverse.tiling.common.CornerOfOrigin;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test matrices:
 * <pre>
 * {@code
 *
 *   0,0
 *    o--------o
 *    |        |
 *    |        |
 *    |  4,4   |
 *    |   o----+---o
 *    |   |    |   |
 *    |   |    |   |
 *    |   |    |   |
 *    |   |    |   |
 *    o---+----o   |
 *        |    9,9 |
 *        |        |
 *        |        |
 *        |        |
 *        o--------o 14,14
 *
 *            15,15 o--------o
 *   1,17           |        |
 *     o------------+-----o  |
 *     |            |     |  |
 *     |            |     |  |
 *     |            |     |  |
 *     o------------+-----o  |
 *                  |  21,21 |
 *                  |        |
 *                  o--------o
 *                          24,24
 * }
 * </pre>
 */
class SparseTileRangeTest {

    /**
     * {@code (0,0)-(9,9)}
     */
    private TileRange topLeft;
    /**
     * {@code (4,4)-(14,14)}
     */
    private TileRange topLeftOverlapping;

    /**
     * {@code (15,15)-(24,24)}
     */
    private TileRange bottomRight;

    /**
     * {@code (1,17)-(21,21)}
     */
    private TileRange bottomRightOverlapping;

    /**
     * topLeft.union(topLeftOverlapping)
     */
    private TileRange topUnion;
    /**
     * bottomRight.union(bottomRightOverlapping)
     */
    private TileRange bottomUnion;
    /**
     * topUnion.union(bottomUnion)
     */
    private TileRange union;

    @BeforeEach
    void setUp() throws Exception {
        topLeft = range(0, 0, 9, 9);
        topLeftOverlapping = range(4, 4, 14, 14);

        bottomRight = range(15, 15, 24, 24);
        bottomRightOverlapping = range(1, 17, 21, 21);

        assertThat(topLeft.intersects(topLeftOverlapping)).isTrue();
        assertThat(topLeft.intersects(bottomRight)).isFalse();
        assertThat(topLeft.intersects(bottomRightOverlapping)).isFalse();

        assertThat(bottomRight.intersects(bottomRightOverlapping)).isTrue();
        assertThat(bottomRight.intersects(topLeftOverlapping)).isFalse();
        assertThat(bottomRight.intersects(topLeft)).isFalse();

        topUnion = topLeft.union(topLeftOverlapping);
        bottomUnion = bottomRight.union(bottomRightOverlapping);
        union = topUnion.union(bottomUnion);
    }

    private TileRange range(TileIndex min, TileIndex max) {
        return range(min.x(), min.y(), max.x(), max.y());
    }

    private TileRange range(long minx, long miny, long maxx, long maxy) {
        return TileRange.of(minx, miny, maxx, maxy, 4, CornerOfOrigin.TOP_LEFT);
    }

    @Test
    void testConstructorPreconditions() {
        assertThrows(NullPointerException.class, () -> new SparseTileRange(null));
        assertThrows(IllegalArgumentException.class, () -> new SparseTileRange(List.of()));
        IllegalArgumentException iae =
                assertThrows(IllegalArgumentException.class, () -> new SparseTileRange(List.of(topLeft)));
        assertThat(iae.getMessage()).contains("should have at least 2 parts");
    }

    @Test
    void testConstructorSplittingParts() {
        /*
         * Original:
         *   0,0
         *    o--------o
         *    |        |
         *    |        |
         *    |  4,4   |
         *    |   o----+---o
         *    |   |    |   |
         *    |   |    |   |
         *    |   |    |   |
         *    |   |    |   |
         *    o---+----o   |
         *        |    9,9 |
         *        |        |
         *        |        |
         *        |        |
         *        o--------o 14,14
         *
         * Merged:
         *   0,0
         *    o--------o
         *    |        |
         *    |        |
         *    |        |10,4
         *    |        o---o
         *    |        |   |
         *    |        |   |
         *    |        |   |
         *    |     9,9|   |
         *    o---o----o   |
         *    4,10|    |   |
         *        |    |   |
         *        |    |   |
         *        |9,14|   |
         *        o----o---o 14,14
         *
         */
        List<TileRange> topParts = List.of(topLeft, range(4, 10, 9, 14), range(10, 4, 14, 14));
        assertThat(((SparseTileRange) topUnion).parts()).isEqualTo(topParts);

        /*
         * Original:
         *            15,15 o--------o
         *   1,17           |        |
         *     o------------+-----o  |
         *     |            |     |  |
         *     |            |     |  |
         *     |            |     |  |
         *     o------------+-----o  |
         *                  |  21,21 |
         *                  |        |
         *                  o--------o
         *                          24,24
         * Merged:
         *            15,15 o--------o
         *   1,17           |        |
         *     o------------o        |
         *     |            |        |
         *     |            |        |
         *     |      14,21 |        |
         *     o------------o 15,21  |
         *                  |        |
         *                  |        |
         *                  o--------o
         *                          24,24
         */
        List<TileRange> bottomParts = List.of(range(1, 17, 14, 21), bottomRight);
        assertThat(((SparseTileRange) bottomUnion).parts()).isEqualTo(bottomParts);

        Set<TileRange> expectedUnionParts =
                Stream.concat(topParts.stream(), bottomParts.stream()).collect(Collectors.toSet());
        Set<TileRange> actualUnionParts = new HashSet<>(((SparseTileRange) union).parts());
        assertThat(actualUnionParts).isEqualTo(expectedUnionParts);
    }

    @Test
    void testUnionCoveredBy() {
        TileRange identity = range(topLeft.min(), topLeft.max());
        assertThat(topLeft.union(identity)).isSameAs(topLeft);
        assertThat(identity.union(topLeft)).isSameAs(identity);

        TileRange covered = range(topLeft.minx(), topLeft.miny(), topLeft.maxx() - 1, topLeft.maxy() - 1);
        assertThat(topLeft.union(covered)).isSameAs(topLeft);
        assertThat(covered.union(topLeft)).isSameAs(topLeft);
    }

    @Test
    void testIsSparse() {
        assertThat(topLeft.isSparse()).isFalse();
        assertThat(topUnion.isSparse()).isTrue();
        assertThat(bottomUnion.isSparse()).isTrue();
        assertThat(union.isSparse()).isTrue();
    }

    @Test
    void testZoomLevel() {
        assertThat(topUnion.zoomLevel()).isEqualTo(topLeft.zoomLevel());
        assertThat(union.zoomLevel()).isEqualTo(topLeft.zoomLevel());
    }

    @Test
    void testCornerOfOrigin() {
        assertThat(topUnion.cornerOfOrigin()).isEqualTo(topLeft.cornerOfOrigin());
        assertThat(union.cornerOfOrigin()).isEqualTo(topLeft.cornerOfOrigin());
    }

    @Test
    void testMin() {
        long minx = Math.min(bottomRight.minx(), bottomRightOverlapping.minx());
        long miny = Math.min(bottomRight.miny(), bottomRightOverlapping.miny());
        TileIndex min = TileIndex.xyz(minx, miny, bottomRight.zoomLevel());
        assertThat(bottomUnion.min()).isEqualTo(min);

        assertThat(union.min()).isEqualTo(topLeft.min());
    }

    @Test
    void testMax() {
        long maxx = Math.max(bottomRight.maxx(), bottomRightOverlapping.maxx());
        long maxy = Math.max(bottomRight.maxy(), bottomRightOverlapping.maxy());
        TileIndex max = TileIndex.xyz(maxx, maxy, bottomRight.zoomLevel());
        assertThat(bottomUnion.max()).isEqualTo(max);

        assertThat(union.max()).isEqualTo(bottomRight.max());
    }

    @Test
    void testTopLeft() {
        assertThat(union.topLeft()).isEqualTo(topLeft.topLeft());
    }

    @Test
    void testBottomLeft() {
        assertThat(union.bottomLeft()).isEqualTo(xyz(0, 24, topLeft.zoomLevel()));
    }

    @Test
    void testTopRight() {
        assertThat(union.topRight()).isEqualTo(xyz(24, 0, topLeft.zoomLevel()));
    }

    @Test
    void testBottomRight() {
        assertThat(union.bottomRight()).isEqualTo(bottomRight.bottomRight());
    }

    @Test
    void testMinx() {
        assertThat(topUnion.minx()).isEqualTo(topLeft.minx());
        assertThat(bottomUnion.minx()).isEqualTo(bottomRightOverlapping.minx());
        assertThat(union.minx()).isEqualTo(topLeft.minx());
    }

    @Test
    void testMaxx() {
        assertThat(topUnion.maxx()).isEqualTo(topLeftOverlapping.maxx());
        assertThat(bottomUnion.maxx()).isEqualTo(bottomRight.maxx());
        assertThat(union.maxx()).isEqualTo(bottomRight.maxx());
    }

    @Test
    void testMiny() {
        assertThat(topUnion.miny()).isEqualTo(topLeft.miny());
        assertThat(bottomUnion.miny()).isEqualTo(bottomRight.miny());
        assertThat(union.miny()).isEqualTo(topLeft.miny());
    }

    @Test
    void testMaxy() {
        assertThat(topUnion.maxy()).isEqualTo(topLeftOverlapping.maxy());
        assertThat(bottomUnion.maxy()).isEqualTo(bottomRight.maxy());
        assertThat(union.maxy()).isEqualTo(bottomRight.maxy());
    }

    @Test
    void testCount() {
        assertThat(topUnion.count()).isLessThan(topLeft.count() + topLeftOverlapping.count());

        TileRange topLeftIntersection = topLeft.intersection(topLeftOverlapping).orElseThrow();
        long expected = topLeft.count() + topLeftOverlapping.count() - topLeftIntersection.count();
        assertThat(topUnion.count()).isEqualTo(expected);

        TileRange bottomRightIntersection =
                bottomRight.intersection(bottomRightOverlapping).orElseThrow();
        expected = bottomRight.count() + bottomRightOverlapping.count() - bottomRightIntersection.count();
        assertThat(bottomUnion.count()).isEqualTo(expected);

        expected = bottomUnion.count() + topUnion.count();
        assertThat(union.count()).isEqualTo(expected);
    }

    @Test
    void testFirst() {
        assertThat(topUnion.first()).isEqualTo(topLeft.first());
        assertThat(bottomUnion.first()).isEqualTo(bottomRightOverlapping.first());
        assertThat(union.first()).isEqualTo(topLeft.first());
    }

    @Test
    void testLast() {
        assertThat(topUnion.last()).isEqualTo(topLeftOverlapping.last());
        assertThat(bottomUnion.last()).isEqualTo(bottomRight.last());
        assertThat(union.last()).isEqualTo(bottomRight.last());
    }

    @Test
    void testAll() {
        List<TileIndex> expected = ((SparseTileRange) union)
                .parts().stream().flatMap(TileRange::all).toList();
        List<TileIndex> actual = union.all().toList();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testNext() {
        List<TileRange> parts = ((SparseTileRange) union).parts();
        List<TileIndex> all = parts.stream().flatMap(TileRange::all).toList();

        assertThat(union.next(all.get(all.size() - 1))).isEmpty();

        for (int i = 0; i < all.size() - 1; i++) {
            TileIndex curr = all.get(i);
            TileIndex next = all.get(i + 1);
            assertThat(union.next(curr)).as("at index " + i).hasValue(next);
        }
    }

    @Test
    void testPrev() {
        List<TileRange> parts = ((SparseTileRange) union).parts();
        List<TileIndex> all = parts.stream().flatMap(TileRange::all).toList();

        assertThat(union.prev(all.get(0))).isEmpty();

        for (int i = all.size() - 1; i > 0; i--) {
            TileIndex curr = all.get(i);
            TileIndex prev = all.get(i - 1);
            assertThat(union.prev(curr)).as("at index " + i).hasValue(prev);
        }
    }

    @Test
    void testWithCornerOfOrigin() {
        assertThat(union.cornerOfOrigin()).isEqualTo(CornerOfOrigin.TOP_LEFT);
        assertThat(union.withCornerOfOrigin(CornerOfOrigin.TOP_LEFT)).isSameAs(union);

        TileRange withBottomLeft = union.withCornerOfOrigin(CornerOfOrigin.BOTTOM_LEFT);
        assertThat(withBottomLeft).isInstanceOf(SparseTileRange.class).isNotSameAs(union);
        assertThat(withBottomLeft.cornerOfOrigin()).isEqualTo(CornerOfOrigin.BOTTOM_LEFT);

        ((SparseTileRange) withBottomLeft).parts().forEach(part -> assertThat(part.cornerOfOrigin())
                .isEqualTo(CornerOfOrigin.BOTTOM_LEFT));

        assertThat(withBottomLeft).isNotEqualTo(union);
        assertThat(withBottomLeft.withCornerOfOrigin(CornerOfOrigin.TOP_LEFT)).isEqualTo(union);
    }

    @Test
    void testCompareTo() {
        assertThat(topUnion.compareTo(bottomUnion)).isNegative();
        assertThat(bottomUnion.compareTo(topUnion)).isPositive();
    }

    @Test
    void testContainsSelfParts() {
        assertThat(union.contains(topLeft)).isTrue();
        assertThat(union.contains(topLeftOverlapping)).isTrue();
        assertThat(union.contains(bottomRight)).isTrue();
        assertThat(union.contains(bottomRightOverlapping)).isTrue();

        assertThat(topUnion.contains(topLeft)).isTrue();
        assertThat(topUnion.contains(topUnion)).isTrue();
        assertThat(topUnion.contains(bottomRight)).isFalse();
    }

    @Test
    void testContainsRangeNotOverlappingAnyPart() {
        // a range that's within the bounds of the union but does not actually overlap any of its parts
        TileRange range = range(1, 10, 2, 11);
        ((SparseTileRange) union).parts().forEach(nonSparse -> assertThat(nonSparse.contains(range))
                .isFalse());
        assertThat(union.contains(range)).isFalse();
    }

    @Test
    void testContainsRangeOverlappingPartsNotContained() {
        /*
         * Original:
         *   0,0
         *    o--------o
         *    |        |
         *    |        |
         *    |        |10,4
         *    |        o---o
         *    |        |   |
         *    |        |   |
         *    |        |   |
         *    |     9,9|   |
         *    o---o----o   |
         *    4,10|    |   |
         *        |    |   |
         *        |    |   |
         *        |9,14|   |
         *        o----o---o 14,14
         *            15,15 o--------o
         *   1,17           |        |
         *     o------------o        |
         *     |            |        |
         *     |            |        |
         *     |      14,21 |        |
         *     o------------o 15,21  |
         *                  |        |
         *                  |        |
         *                  o--------o
         *                          24,24
         */

        // a range that's within the union bounds and overlaps some of the union parts but it not fully contained
        TileRange bottomLeftCornerNotContained = range(3, 8, 5, 11);
        TileRange bottomRightCornerNotContained = range(15, 21, 25, 25);
        TileRange topLeftCornerNotContained = range(14, 16, 20, 18);
        TileRange topRightCornerNotContained = range(9, 3, 11, 6);

        assertThat(union.intersects(bottomLeftCornerNotContained)).isTrue();
        assertThat(union.intersects(bottomRightCornerNotContained)).isTrue();
        assertThat(union.intersects(topLeftCornerNotContained)).isTrue();
        assertThat(union.intersects(topRightCornerNotContained)).isTrue();

        assertThat(union.contains(bottomLeftCornerNotContained)).isFalse();
        assertThat(union.contains(bottomRightCornerNotContained)).isFalse();
        assertThat(union.contains(topLeftCornerNotContained)).isFalse();
        assertThat(union.contains(topRightCornerNotContained)).isFalse();
    }

    @Test
    void testContainsRangeOverlappingPartsContained() {
        // a range that's contained across multiple parts but not by any single one
        /*
         *
         *            15,15 o--------o
         *   1,17           |        | bottomRight
         *     o------------+-----o  |
         *     |            |     |  |
         *     |            |     |  |
         *     |            |     |  |
         *     o------------+-----o 21,21
         * bottomRightOverlapping    |
         *                  |        |
         *                  o--------o
         *                          24,24
         * bottomUnion
         *            15,15 o--------o
         *   1,17           |        |
         *     o------------o        |
         *     |            |        |
         *     |            |        |
         *     |            |        |
         *     o------------o 15,21  |
         *                  |        |
         *                  |        |
         *                  o--------o
         *                          24,24
         */
        TileRange containedCrossingParts = range(2, 18, 22, 20);
        assertThat(bottomRight.contains(containedCrossingParts)).isFalse();
        assertThat(bottomRightOverlapping.contains(containedCrossingParts)).isFalse();
        assertThat(bottomUnion.contains(containedCrossingParts)).isTrue();
    }

    @Test
    void testIntersectionSelfParts() {
        assertThat(union.intersection(topLeft)).hasValue(topLeft);
        assertThat(union.intersection(topLeftOverlapping)).hasValue(topLeftOverlapping);
        assertThat(union.intersection(bottomRight)).hasValue(bottomRight);
        assertThat(union.intersection(bottomRightOverlapping)).hasValue(bottomRightOverlapping);

        assertThat(union.intersection(topUnion)).hasValue(topUnion);
        assertThat(union.intersection(bottomUnion)).hasValue(bottomUnion);
    }

    @Test
    void testIntersection() {
        TileRange contiguous = range(union.min(), union.max());
        assertThat(union.intersection(contiguous)).hasValue(union);
        assertThat(contiguous.intersection(union)).hasValue(union);
    }

    @Test
    void testNoIntersection() {
        TileRange rangeWithinBoundsNotIntersecting = range(2, 22, 3, 23);
        assertThat(union.intersection(rangeWithinBoundsNotIntersecting)).isEmpty();
        assertThat(rangeWithinBoundsNotIntersecting.intersection(union)).isEmpty();

        assertThat(topUnion.intersection(bottomUnion)).isEmpty();
    }

    @Test
    void testIntersectionOverlappingSparseRanges() {
        /*
         *   0,0
         *    o--------o
         *    |        |
         *    |        |
         *    |        |10,4
         *    |        o---o
         *    |        |   |
         *    |        |   |
         *    |        |   |
         *    |     9,9|   |
         *    o---o----o   |
         *    4,10|    |   |
         *        |    |   |
         *        |    |   |
         *        |9,14|   |
         *        o----o---o 14,14
         *
         *            15,15 o--------o
         *   1,17           |        |
         *     o------------o        |
         *     |            |        |
         *     |            |        |
         *     |      14,21 |        |
         *     o------------o 15,21  |
         *                  |        |
         *                  |        |
         *                  o--------o
         *                          24,24
         */

        TileRange rangeOverlappingBothUnions = range(9, 9, 16, 23);
        TileRange union1 = topLeftOverlapping.union(rangeOverlappingBothUnions);
        TileRange union2 = bottomRightOverlapping.union(rangeOverlappingBothUnions);
        assertThat(union1).isInstanceOf(SparseTileRange.class);
        assertThat(union2).isInstanceOf(SparseTileRange.class);

        assertThat(union1.intersection(union2)).hasValue(rangeOverlappingBothUnions);

        assertThat(union1.intersection(topUnion)).hasValue(range(4, 4, 14, 14));
        assertThat(topUnion.intersection(union1)).hasValue(range(4, 4, 14, 14));

        TileRange expected =
                TileRangeUtil.union(List.of(range(4, 4, 14, 14), range(9, 17, 14, 21), range(15, 15, 16, 23)));
        assertThat(union1.intersection(union)).hasValue(expected);
    }
}
