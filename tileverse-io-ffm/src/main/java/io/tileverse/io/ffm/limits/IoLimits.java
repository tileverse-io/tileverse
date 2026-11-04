/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm.limits;

import java.nio.file.Path;

/**
 * The caps honored by the read path, derived from a {@link ResourceLimits} by applying fractions to the memory left
 * beside the heap: a fifth of it for the fetches in flight, half of it for off-heap decode buffers, and half of the
 * usable disk for spill. With the heap at half the container, the usual setting, the memory caps come to a tenth and a
 * quarter of the container. This is the one place those fractions live; the budgets consuming the caps own no sizing of
 * their own.
 */
public record IoLimits(long maxOffHeapBytes, long maxSpillBytes, long maxDecodeBytes, Path spillDir) {

    private static final double OFF_HEAP_FRACTION = 0.2;
    private static final double SPILL_FRACTION = 0.5;
    private static final double DECODE_FRACTION = 0.5;

    public IoLimits {
        if (maxOffHeapBytes <= 0) {
            throw new IllegalArgumentException("maxOffHeapBytes must be > 0, got " + maxOffHeapBytes);
        }
        if (maxSpillBytes <= 0) {
            throw new IllegalArgumentException("maxSpillBytes must be > 0, got " + maxSpillBytes);
        }
        if (maxDecodeBytes <= 0) {
            throw new IllegalArgumentException("maxDecodeBytes must be > 0, got " + maxDecodeBytes);
        }
    }

    /** Derives the caps from {@code limits}, applying the off-heap, spill, and decode fractions. */
    public static IoLimits from(ResourceLimits limits) {
        Path dir = limits.spillDir();
        long spill = atLeastOneByte(limits.usableDiskBytes(dir) * SPILL_FRACTION);
        return new IoLimits(offHeapCap(limits), spill, decodeCap(limits), dir);
    }

    /**
     * Returns the off-heap cap of {@link #from}, without reading the disk. A memory pool needs the memory caps only,
     * and must not fail on a missing spill directory.
     */
    public static long offHeapCap(ResourceLimits limits) {
        return atLeastOneByte(limits.availableMemoryBytes() * OFF_HEAP_FRACTION);
    }

    /** Returns the decode cap of {@link #from}, without reading the disk. */
    public static long decodeCap(ResourceLimits limits) {
        return atLeastOneByte(limits.availableMemoryBytes() * DECODE_FRACTION);
    }

    private static long atLeastOneByte(double bytes) {
        return Math.max(1L, (long) bytes);
    }
}
