/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.io.ffm.limits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ResourceLimitsTest {

    @Test
    void defaultReportsPositiveMachineFacts() {
        ResourceLimits limits = ResourceLimits.getDefault();
        assertThat(limits.availableMemoryBytes()).as("memory").isPositive();
        assertThat(limits.usableDiskBytes(Path.of(System.getProperty("java.io.tmpdir"))))
                .as("disk")
                .isPositive();
        assertThat(limits.availableProcessors()).as("cores").isPositive();
        assertThat(limits.spillDir()).as("spill dir").isNotNull();
    }

    @Test
    void aFixedFakeReportsItsValues() {
        Path dir = Path.of("/tmp/x");
        ResourceLimits fake = ResourceLimits.fixed(2L << 30, 9L << 30, 4, dir);
        assertThat(fake.availableMemoryBytes()).isEqualTo(2L << 30);
        assertThat(fake.usableDiskBytes(Path.of("/anything"))).isEqualTo(9L << 30);
        assertThat(fake.availableProcessors()).isEqualTo(4);
        assertThat(fake.spillDir()).isEqualTo(dir);
    }

    @Test
    void allowanceIsContainerMinusHeap() {
        assertThat(DefaultResourceLimits.nativeAllowance(4L << 30, 2L << 30)).isEqualTo(2L << 30);
    }

    // a heap set to the whole container still leaves the budgets an eighth of it
    @Test
    void allowanceKeepsAnEighthFloor() {
        assertThat(DefaultResourceLimits.nativeAllowance(4L << 30, 4L << 30)).isEqualTo(512L << 20);
    }

    private static final String KEY = "tileverse.io.availableMemory";

    @ParameterizedTest
    @CsvSource({
        "512m, 536870912",
        "2g, 2147483648",
        "1024k, 1048576",
        "1048576, 1048576",
        "512M, 536870912",
    })
    void resolveSizeBytesParsesUnitsAndCase(String raw, long expectedBytes) {
        assertThat(DefaultResourceLimits.resolveSizeBytes(raw, null, KEY)).isEqualTo(expectedBytes);
    }

    @Test
    void sizePropertyWinsOverEnv() {
        assertThat(DefaultResourceLimits.resolveSizeBytes("512m", "1g", KEY)).isEqualTo(512L << 20);
    }

    @Test
    void sizeFallsBackToEnv() {
        assertThat(DefaultResourceLimits.resolveSizeBytes(null, "256m", KEY)).isEqualTo(256L << 20);
    }

    @Test
    void unsetSizeReturnsProbeSentinel() {
        assertThat(DefaultResourceLimits.resolveSizeBytes(null, null, KEY)).isZero();
        assertThat(DefaultResourceLimits.resolveSizeBytes("   ", null, KEY)).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "-5", "0", "g"})
    void anInvalidSizeOverrideFailsNamingTheKey(String raw) {
        assertThatThrownBy(() -> DefaultResourceLimits.resolveSizeBytes(raw, null, KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(KEY);
    }

    @Test
    void spillDirDefaultsToTempDir() {
        assertThat(DefaultResourceLimits.resolveSpillDir(null, null))
                .isEqualTo(Path.of(System.getProperty("java.io.tmpdir")));
    }

    @Test
    void resolveSpillDirUsesTheOverride() {
        assertThat(DefaultResourceLimits.resolveSpillDir("/var/spill", null)).isEqualTo(Path.of("/var/spill"));
    }
}
