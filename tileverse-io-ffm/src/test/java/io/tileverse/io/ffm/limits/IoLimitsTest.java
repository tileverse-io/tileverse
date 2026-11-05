/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm.limits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class IoLimitsTest {

    private static final Path DIR = Path.of("/tmp/spill");

    @Test
    void appliesFractionsToRawFacts() {
        ResourceLimits fake = ResourceLimits.fixed(10L << 30, 100L << 30, 8, DIR);
        IoLimits limits = IoLimits.from(fake);
        assertThat(limits.maxOffHeapBytes()).as("0.2 of 10 GiB").isEqualTo((long) (0.2 * (10L << 30)));
        assertThat(limits.maxSpillBytes()).as("0.5 of 100 GiB").isEqualTo((long) (0.5 * (100L << 30)));
        assertThat(limits.spillDir()).isEqualTo(DIR);
    }

    @Test
    void decodeCapIsHalfOfMemory() {
        ResourceLimits limits = ResourceLimits.fixed(2_000_000_000L, 100_000_000_000L, 8, Path.of("/tmp"));
        IoLimits ioLimits = IoLimits.from(limits);
        assertThat(ioLimits.maxDecodeBytes())
                .as("decode ceiling is half of the memory beside the heap")
                .isEqualTo(1_000_000_000L);
    }

    // A memory pool must not fail on a missing spill directory
    @Test
    void memoryCapsSkipTheDisk() {
        ResourceLimits limits = new ResourceLimits() {
            @Override
            public long availableMemoryBytes() {
                return 1000L;
            }

            @Override
            public long usableDiskBytes(Path dir) {
                throw new UncheckedIOException(new IOException("no spill directory"));
            }

            @Override
            public int availableProcessors() {
                return 1;
            }

            @Override
            public Path spillDir() {
                return DIR;
            }
        };
        assertThat(IoLimits.offHeapCap(limits)).isEqualTo(200L);
        assertThat(IoLimits.decodeCap(limits)).isEqualTo(500L);
    }

    @Test
    void capsAreAtLeastOneByte() {
        ResourceLimits tiny = ResourceLimits.fixed(1L, 1L, 1, DIR);
        IoLimits limits = IoLimits.from(tiny);
        assertThat(limits.maxOffHeapBytes()).as("off-heap floor").isEqualTo(1L);
        assertThat(limits.maxSpillBytes()).as("spill floor").isEqualTo(1L);
        assertThat(limits.maxDecodeBytes()).as("decode floor").isEqualTo(1L);
    }

    @Test
    void rejectsNonPositiveCaps() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IoLimits(0, 1, 1, DIR))
                .withMessageContaining("maxOffHeapBytes");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IoLimits(1, 0, 1, DIR))
                .withMessageContaining("maxSpillBytes");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IoLimits(1, 1, 0, DIR))
                .withMessageContaining("maxDecodeBytes");
    }
}
