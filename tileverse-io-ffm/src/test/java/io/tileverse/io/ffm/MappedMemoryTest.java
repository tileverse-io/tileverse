/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MappedMemoryTest {

    @Test
    void largeAllocationIsPageAligned() {
        assumeTrue(MappedMemory.mapsLargeBackings(), "backings are mapped on Linux and macOS only");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = MappedMemory.allocate(MappedMemory.MIN_MAPPED_BYTES, arena);
            assertThat(segment.byteSize()).isEqualTo(MappedMemory.MIN_MAPPED_BYTES);
            assertThat(segment.address() % 4096)
                    .as("a mapping starts on a page")
                    .isZero();
        }
    }

    @Test
    void allocationIsZeroedAndWritable() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = MappedMemory.allocate(1 << 20, arena);
            long last = segment.byteSize() - Long.BYTES;
            assertThat(segment.get(ValueLayout.JAVA_LONG, 0)).isZero();
            assertThat(segment.get(ValueLayout.JAVA_LONG, last)).isZero();
            segment.set(ValueLayout.JAVA_LONG, last, 42L);
            assertThat(segment.get(ValueLayout.JAVA_LONG, last)).isEqualTo(42L);
        }
    }

    @Test
    void smallAllocationIsServedToo() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = MappedMemory.allocate(16, arena);
            segment.set(ValueLayout.JAVA_INT, 0, 7);
            assertThat(segment.get(ValueLayout.JAVA_INT, 0)).isEqualTo(7);
        }
    }

    @Test
    void closedArenaInvalidatesMemory() {
        Arena arena = Arena.ofShared();
        MemorySegment segment = MappedMemory.allocate(1 << 20, arena);
        arena.close();
        assertThat(segment.scope().isAlive()).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"Linux, 32802", "Mac OS X, 4098", "Windows 11, 0", "FreeBSD, 0"})
    void mappingFlagsFollowThePlatform(String osName, int expectedFlags) {
        assertThat(MappedMemory.anonymousPrivateMappingFlags(osName)).isEqualTo(expectedFlags);
    }
}
