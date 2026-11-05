/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/** Small {@link MemorySegment} helpers shared across the reader. */
public final class Segments {

    private Segments() {}

    /**
     * Copies {@code source}'s bytes into a fresh heap-backed read-only segment, severing any aliasing with an off-heap
     * or transient backing. The returned segment owns its bytes and outlives {@code source}, which makes it safe to
     * retain a read value past the life of the buffers backing it.
     */
    public static MemorySegment toHeapReadOnly(MemorySegment source) {
        return MemorySegment.ofArray(source.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
    }

    /**
     * Returns a read-only view of {@code source} while preserving the {@link MemorySegment#NULL} absent sentinel by
     * identity. A {@code null} reference or {@link MemorySegment#NULL} maps back to {@link MemorySegment#NULL}.
     *
     * <p>Format records use {@code segment == MemorySegment.NULL} to mark an optional binary field as absent, and their
     * serializers omit the field on that exact identity. Calling {@link MemorySegment#asReadOnly()} on the sentinel
     * returns a fresh zero-length segment that no longer compares equal to it, which would turn an absent field into a
     * present zero-byte one that a strict reader of the format may reject. Guarding the sentinel keeps the absence
     * intact.
     */
    public static MemorySegment readOnlyOrAbsent(MemorySegment source) {
        if (source == null || source == MemorySegment.NULL) {
            return MemorySegment.NULL;
        }
        return source.isReadOnly() ? source : source.asReadOnly();
    }

    /**
     * Allocates {@code byteSize} zeroed native bytes owned by {@code arena}. On Linux and macOS a request of 64 KiB or
     * more is an anonymous memory mapping, unmapped when the arena closes. A smaller request, and any request on
     * another platform, comes from malloc. The segment pool allocates its backings this way: memory freed to malloc
     * stays in the allocator's per-thread arenas, a mapping leaves the process at once. A short-lived buffer belongs in
     * the pool rather than in a mapping of its own, because a mapping per use costs two system calls on the calling
     * thread.
     */
    public static MemorySegment allocateNative(long byteSize, Arena arena) {
        return MappedMemory.allocate(byteSize, arena);
    }
}
