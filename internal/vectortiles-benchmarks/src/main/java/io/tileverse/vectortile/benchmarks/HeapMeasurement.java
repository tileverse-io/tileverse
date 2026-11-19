/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.benchmarks;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Locale;

/**
 * The heap measurement of the size programs of this module, {@link DecodedTileSize} and
 * {@link TileUnderConstructionSize}, and the cells of their tables.
 *
 * <p>A size program keeps copies of a tile in memory, and divides the growth of the used heap by the number of copies.
 * It reads the used heap after full garbage collections, when the heap holds the live objects alone.
 */
final class HeapMeasurement {

    /** The heap held by the copies of one measurement: enough to make the size per tile exact to a few KiB. */
    private static final long MEASURED_BYTES = 512L * 1024 * 1024;

    /** The heap assumed for a tile in memory, as a multiple of its encoded size, to size the copies. */
    private static final int SIZE_RATIO = 10;

    private static final int MIN_COPIES = 3;

    private static final int MAX_COPIES = 500;

    private HeapMeasurement() {}

    /** The number of copies of a tile kept by one measurement, from the encoded size of the tile. */
    static int copiesFor(int encodedSize) {
        long copies = MEASURED_BYTES / ((long) encodedSize * SIZE_RATIO);
        return (int) Math.max(MIN_COPIES, Math.min(MAX_COPIES, copies));
    }

    /** The used heap after full garbage collections, in bytes. */
    @SuppressWarnings("java:S1215") // the used heap counts the live objects alone after a full collection
    static long usedHeapAfterGc() {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        // The second collection frees the objects held by references cleared in the first
        System.gc();
        System.gc();
        return memory.getHeapMemoryUsage().getUsed();
    }

    /** Prints the version of Java and the maximum heap, then an empty line: the lines before a table. */
    static void printRuntime() {
        long maxHeapMiB = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        print("Java " + Runtime.version() + ", maximum heap " + maxHeapMiB + " MiB");
        print("");
    }

    /** The heap of tileverse against the heap of java-vector-tile, as a percentage: -60% means 60% less. */
    static String heapDelta(long tileverseSize, long javaVectorTileSize) {
        double delta = (double) tileverseSize / javaVectorTileSize - 1.0;
        return String.format(Locale.ROOT, "%+.0f%%", delta * 100);
    }

    static String kib(long bytes) {
        return String.format(Locale.ROOT, "%,d KiB", Math.round(bytes / 1024.0));
    }

    @SuppressWarnings("java:S106") // the tables go to the standard output, saved by run-benchmarks.sh
    static void print(String line) {
        System.out.println(line);
    }
}
