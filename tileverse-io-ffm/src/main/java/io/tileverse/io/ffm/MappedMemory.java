/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

/**
 * Native memory for large buffers: the backings of a {@link SegmentPool}, and the buffers allocated by
 * {@link Segments#allocateNative}. On Linux and macOS a buffer is an anonymous memory mapping; elsewhere it comes from
 * malloc. {@link Segments#allocateNative} is the public entry point.
 *
 * <p>Such a buffer is large and short-lived. A backing is freed when its pool evicts or reaps it. Memory freed to
 * malloc does not leave the process: glibc parks the freed chunk in the arena of the thread that allocated it, and
 * under many threads the arenas together keep several times the memory in use at any moment. A mapping is unmapped when
 * its arena closes, and the pages go back to the kernel at once, whatever the allocator does. Mappings start at
 * {@link #MIN_MAPPED_BYTES}. Below that size malloc reuses its small chunks well, and a system call per buffer would
 * cost more than it saves.
 *
 * <p>Mapped memory is not counted by the JVM's native memory tracking; {@link SegmentPool#stats()} reports what a pool
 * retains. The system property {@code tileverse.io.mappedBackings=false} keeps the buffers on malloc.
 */
final class MappedMemory {

    /** A buffer at least this large is mapped; a smaller one comes from malloc through its arena. */
    static final long MIN_MAPPED_BYTES = 64L << 10;

    private static final String PROPERTY = "tileverse.io.mappedBackings";

    private static final int PROT_READ_WRITE = 0x1 | 0x2;
    private static final long MAP_FAILED = -1L;

    private static final MethodHandle MMAP;
    private static final MethodHandle MUNMAP;
    private static final int MAP_FLAGS;

    static {
        MethodHandle mmap = null;
        MethodHandle munmap = null;
        int flags = anonymousPrivateMappingFlags(System.getProperty("os.name", ""));
        if (flags != 0 && Boolean.parseBoolean(System.getProperty(PROPERTY, "true"))) {
            try {
                Linker linker = Linker.nativeLinker();
                SymbolLookup libc = linker.defaultLookup();
                mmap = linker.downcallHandle(
                        libc.find("mmap").orElseThrow(),
                        FunctionDescriptor.of(
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_LONG,
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_LONG));
                munmap = linker.downcallHandle(
                        libc.find("munmap").orElseThrow(),
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
            } catch (RuntimeException | LinkageError _) {
                // Native access is denied to this module, or libc has no such symbols: the buffers stay on malloc.
                mmap = null;
                munmap = null;
            }
        }
        MMAP = mmap;
        MUNMAP = munmap;
        MAP_FLAGS = flags;
    }

    private MappedMemory() {}

    /** Whether this platform maps the buffers of at least {@link #MIN_MAPPED_BYTES}. */
    static boolean mapsLargeBackings() {
        return MMAP != null;
    }

    /**
     * Allocates {@code byteSize} zeroed bytes owned by {@code arena}. The bytes are a mapping when the platform maps
     * and the size reaches {@link #MIN_MAPPED_BYTES}, and malloc memory otherwise. Closing the arena frees either.
     */
    static MemorySegment allocate(long byteSize, Arena arena) {
        if (MMAP != null && byteSize >= MIN_MAPPED_BYTES) {
            MemorySegment mapped = map(byteSize, arena);
            if (mapped != null) {
                return mapped;
            }
        }
        return arena.allocate(byteSize);
    }

    /** Maps {@code byteSize} bytes, or returns {@code null} when the kernel refuses the mapping. */
    private static MemorySegment map(long byteSize, Arena arena) {
        MemorySegment address;
        try {
            address =
                    (MemorySegment) MMAP.invokeExact(MemorySegment.NULL, byteSize, PROT_READ_WRITE, MAP_FLAGS, -1, 0L);
        } catch (Throwable t) {
            throw new IllegalStateException("mmap of " + byteSize + " bytes failed", t);
        }
        if (address.address() == MAP_FAILED) {
            return null;
        }
        return address.reinterpret(byteSize, arena, MappedMemory::unmap);
    }

    private static void unmap(MemorySegment mapping) {
        try {
            int _ = (int) MUNMAP.invokeExact(mapping, mapping.byteSize());
        } catch (Throwable t) {
            throw new IllegalStateException("munmap of " + mapping.byteSize() + " bytes failed", t);
        }
    }

    /**
     * The {@code MAP_PRIVATE | MAP_ANONYMOUS} flags of {@code osName}, or zero on a platform where the buffers stay on
     * malloc. The anonymous flag differs between the Linux and the BSD lineages. On Linux the flags include
     * {@code MAP_POPULATE}, which has the kernel install and zero the pages in the mapping call. A buffer is written in
     * full right after, and one call costs less than a fault per page.
     */
    static int anonymousPrivateMappingFlags(String osName) {
        String lower = osName.toLowerCase(Locale.ROOT);
        if (lower.contains("linux")) {
            return 0x02 | 0x20 | 0x8000;
        }
        if (lower.contains("mac os") || lower.contains("darwin")) {
            return 0x02 | 0x1000;
        }
        return 0;
    }
}
