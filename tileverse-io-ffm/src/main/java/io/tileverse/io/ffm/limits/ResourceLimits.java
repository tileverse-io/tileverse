/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm.limits;

import java.nio.file.Path;

/**
 * The raw machine facts the read path sizes its native and disk budgets from: how much memory and disk the container
 * permits, how many processors it has, and where to spill. This is an injectable port - the default reads the running
 * container, a test supplies a {@link #fixed fixed} instance for a deterministic bound independent of the host. Sizing
 * policy (the fractions applied to these facts) lives where the caps are derived, not here.
 */
public interface ResourceLimits {

    /** Native (off-heap) memory the container permits, in bytes. */
    long availableMemoryBytes();

    /** Usable disk on the file store backing {@code dir}, in bytes. */
    long usableDiskBytes(Path dir);

    /** Processor count available to this process. */
    int availableProcessors();

    /** The directory spill files are written under. */
    Path spillDir();

    /**
     * The process-wide default, reading the running container. Each fact can be set instead, by a system property or
     * else an environment variable, with a byte size such as {@code 512m} or {@code 2g}:
     *
     * <ul>
     *   <li>{@code tileverse.io.availableMemory} ({@code TILEVERSE_IO_AVAILABLE_MEMORY}) sets
     *       {@link #availableMemoryBytes()}. It is the memory the caps are computed from, not a cap:
     *       {@link IoLimits} gives a fifth of it to the fetches in flight and half of it to the decode buffers. The
     *       default segment pool keeps idle up to half of the fetch cap in small buffers and the whole fetch cap in
     *       large buffers, each within fixed bounds, and bounds its footprint by the fetch and decode caps together.
     *   <li>{@code tileverse.io.usableDisk} ({@code TILEVERSE_IO_USABLE_DISK}) sets {@link #usableDiskBytes}, of which
     *       {@link IoLimits} gives half to spill files.
     *   <li>{@code tileverse.io.spillDir} ({@code TILEVERSE_IO_SPILL_DIR}) sets {@link #spillDir()}, the temporary
     *       directory by default.
     * </ul>
     */
    static ResourceLimits getDefault() {
        return DefaultResourceLimits.INSTANCE;
    }

    /** A fixed instance for tests; {@code usableDiskBytes} ignores its argument and returns {@code disk}. */
    static ResourceLimits fixed(long memory, long disk, int processors, Path spillDir) {
        return new ResourceLimits() {
            @Override
            public long availableMemoryBytes() {
                return memory;
            }

            @Override
            public long usableDiskBytes(Path dir) {
                return disk;
            }

            @Override
            public int availableProcessors() {
                return processors;
            }

            @Override
            public Path spillDir() {
                return spillDir;
            }
        };
    }
}
