/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm.limits;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import com.sun.management.OperatingSystemMXBean;

final class DefaultResourceLimits implements ResourceLimits {

    static final DefaultResourceLimits INSTANCE = create();

    private static final String MEMORY_PROPERTY = "tileverse.io.availableMemory";
    private static final String MEMORY_ENV = "TILEVERSE_IO_AVAILABLE_MEMORY";
    private static final String DISK_PROPERTY = "tileverse.io.usableDisk";
    private static final String DISK_ENV = "TILEVERSE_IO_USABLE_DISK";
    private static final String SPILL_DIR_PROPERTY = "tileverse.io.spillDir";
    private static final String SPILL_DIR_ENV = "TILEVERSE_IO_SPILL_DIR";

    private final long memoryOverride;
    private final long diskOverride;
    private final Path spillDir;

    private DefaultResourceLimits(long memoryOverride, long diskOverride, Path spillDir) {
        this.memoryOverride = memoryOverride;
        this.diskOverride = diskOverride;
        this.spillDir = spillDir;
    }

    static DefaultResourceLimits create() {
        long memory = resolveSizeBytes(System.getProperty(MEMORY_PROPERTY), System.getenv(MEMORY_ENV), MEMORY_PROPERTY);
        long disk = resolveSizeBytes(System.getProperty(DISK_PROPERTY), System.getenv(DISK_ENV), DISK_PROPERTY);
        Path dir = resolveSpillDir(System.getProperty(SPILL_DIR_PROPERTY), System.getenv(SPILL_DIR_ENV));
        return new DefaultResourceLimits(memory, disk, dir);
    }

    @Override
    public long availableMemoryBytes() {
        if (memoryOverride > 0) {
            return memoryOverride;
        }
        return nativeAllowance(containerMemoryBytes(), Runtime.getRuntime().maxMemory());
    }

    /**
     * The memory left for native buffers once the heap has taken its share: the container limit minus the maximum heap.
     * The heap grows to its maximum under load whatever its live set, hence the budgets derived from this figure must
     * not count that memory a second time. A heap set close to the container limit leaves an eighth of the container:
     * the budgets stay positive, and the rest spills to disk.
     */
    static long nativeAllowance(long containerBytes, long maxHeapBytes) {
        return Math.max(containerBytes - maxHeapBytes, containerBytes / 8);
    }

    @Override
    public long usableDiskBytes(Path dir) {
        if (diskOverride > 0) {
            return diskOverride;
        }
        try {
            return Math.max(1L, Files.getFileStore(dir).getUsableSpace());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read usable disk for " + dir, e);
        }
    }

    @Override
    public int availableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    @Override
    public Path spillDir() {
        return spillDir;
    }

    /**
     * The container memory limit, as the JVM already detects it. With container support on (the default), the platform
     * bean honors the cgroup v1/v2 limit and reports physical RAM when none is set; reading {@code /sys/fs/cgroup}
     * ourselves would only duplicate that, less correctly. Falls back to the heap basis on a JVM without the bean.
     */
    private static long containerMemoryBytes() {
        OperatingSystemMXBean operatingSystem = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        if (operatingSystem != null) {
            long total = operatingSystem.getTotalMemorySize();
            if (total > 0) {
                return total;
            }
        }
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * Resolves an explicit byte-size override from its already-read property and environment values, the property
     * winning. A blank or absent override returns {@code 0}, the sentinel for "probe the machine instead"; a present
     * but unparseable or non-positive value fails naming {@code key}. Pure: the ambient reads happen in the caller.
     */
    static long resolveSizeBytes(String propertyValue, String envValue, String key) {
        String raw = propertyValue != null ? propertyValue : envValue;
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        return parsePositiveSize(key, raw.trim());
    }

    private static long parsePositiveSize(String property, String raw) {
        long size;
        try {
            size = parseSize(raw);
        } catch (NumberFormatException _) {
            throw invalidByteSize(property, raw);
        }
        if (size <= 0) {
            throw invalidByteSize(property, raw);
        }
        return size;
    }

    private static IllegalArgumentException invalidByteSize(String property, String raw) {
        return new IllegalArgumentException("Invalid byte size for " + property + ": '" + raw + "'");
    }

    private static long parseSize(String raw) {
        String lower = raw.toLowerCase(Locale.ROOT);
        long multiplier = 1L;
        if (lower.endsWith("k")) {
            multiplier = 1L << 10;
        } else if (lower.endsWith("m")) {
            multiplier = 1L << 20;
        } else if (lower.endsWith("g")) {
            multiplier = 1L << 30;
        }
        String digits = multiplier == 1L ? lower : lower.substring(0, lower.length() - 1);
        return Long.parseLong(digits.trim()) * multiplier;
    }

    /**
     * Resolves the spill directory from its already-read property and environment values, the property winning,
     * defaulting to the temp directory when neither is set. Pure: the ambient reads happen in the caller.
     */
    static Path resolveSpillDir(String propertyValue, String envValue) {
        String raw = propertyValue != null ? propertyValue : envValue;
        if (raw == null || raw.isBlank()) {
            return Path.of(System.getProperty("java.io.tmpdir"));
        }
        return Path.of(raw.trim());
    }
}
