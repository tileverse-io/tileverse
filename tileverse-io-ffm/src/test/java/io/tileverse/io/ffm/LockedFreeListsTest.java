/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.io.ffm.DefaultSegmentPool.Backing;
import java.lang.foreign.Arena;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LockedFreeListsTest {

    private final Arena arena = Arena.ofShared();

    private final LockedFreeLists lists = new LockedFreeLists(Long.MAX_VALUE);

    @AfterEach
    void closeArena() {
        arena.close();
    }

    // The large class sees unbounded distinct capacities: an emptied group must not stay behind
    @Test
    void takeRemovesEmptiedGroups() {
        putBackings(1024, 2048, 4096);
        assertThat(lists.take(1024)).isNotNull();
        assertThat(lists.take(1024)).isNotNull();
        assertThat(lists.take(1024)).isNotNull();
        assertThat(lists.groupCount()).isZero();
        assertThat(lists.take(1024)).isNull();
    }

    @Test
    void reapRemovesEmptiedGroups() {
        putBackings(1024, 2048);
        assertThat(lists.reapStale(System.nanoTime() + 1, 1)).hasSize(2);
        assertThat(lists.groupCount()).isZero();
    }

    @Test
    void drainRemovesGroups() {
        putBackings(1024, 2048);
        assertThat(lists.drain()).hasSize(2);
        assertThat(lists.groupCount()).isZero();
    }

    private void putBackings(long... capacities) {
        for (long capacity : capacities) {
            assertThat(lists.put(new Backing(arena, arena.allocate(capacity)))).isTrue();
        }
        assertThat(lists.groupCount()).isEqualTo(capacities.length);
    }
}
