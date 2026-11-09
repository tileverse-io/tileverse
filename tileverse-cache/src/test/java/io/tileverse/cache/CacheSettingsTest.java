/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import static io.tileverse.cache.CacheSettings.DATA_MAX_READ_SIZE;
import static io.tileverse.cache.CacheSettings.DATA_MAX_SIZE;
import static io.tileverse.cache.CacheSettings.METADATA_MAX_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CacheSettingsTest {

    private static final String DATA_MAX_SIZE_VARIABLE = "IO_TILEVERSE_CACHE_DATA_MAXSIZE";

    private final Map<String, String> properties = new HashMap<>();
    private final Map<String, String> environment = new HashMap<>();
    private final CacheSettings settings = new CacheSettings(properties::get, environment::get, 1_000_000L);

    @Test
    void percentOfMaxHeap() {
        properties.put(DATA_MAX_SIZE, "25%");

        assertThat(settings.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(250_000L);
    }

    // a JVM without a heap limit reports Long.MAX_VALUE
    @Test
    void fullShareOfUnboundedHeap() {
        CacheSettings unbounded = new CacheSettings(properties::get, environment::get, Long.MAX_VALUE);
        properties.put(DATA_MAX_SIZE, "100%");

        assertThat(unbounded.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void overflowingByteCountRejected() {
        properties.put(DATA_MAX_SIZE, "99999999999G");

        assertThatThrownBy(() -> settings.maxSize(DATA_MAX_SIZE, "20%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("99999999999G");
    }

    @ParameterizedTest
    @MethodSource("byteCounts")
    void byteCountsWithUnits(String text, long expected) {
        properties.put(DATA_MAX_SIZE, text);

        assertThat(settings.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(expected);
    }

    static Stream<Arguments> byteCounts() {
        return Stream.of(
                Arguments.of("512M", 536_870_912L),
                Arguments.of("1G", 1_073_741_824L),
                Arguments.of("1GiB", 1_073_741_824L),
                Arguments.of("2KB", 2048L),
                Arguments.of("4096", 4096L),
                Arguments.of("0", 0L),
                Arguments.of("0%", 0L),
                Arguments.of("1g", 1_073_741_824L));
    }

    @ParameterizedTest
    @MethodSource("invalidSizes")
    void invalidSizeNamesSettingAndValue(String text) {
        properties.put(DATA_MAX_SIZE, text);

        assertThatThrownBy(() -> settings.maxSize(DATA_MAX_SIZE, "20%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("system property io.tileverse.cache.data.max-size")
                .hasMessageContaining(text);
    }

    static Stream<String> invalidSizes() {
        return Stream.of("abc", "-1", "101%", "1.5G", "%", "16 M");
    }

    @Test
    void environmentVariableName() {
        String variable = CacheSettings.environmentVariableOf("io.tileverse.cache.data.max-read-size");

        assertThat(variable).isEqualTo("IO_TILEVERSE_CACHE_DATA_MAXREADSIZE");
    }

    @Test
    void environmentVariableSetsValue() {
        environment.put(DATA_MAX_SIZE_VARIABLE, "30%");

        assertThat(settings.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(300_000L);
    }

    @Test
    void propertyWinsOverEnvironment() {
        properties.put(DATA_MAX_SIZE, "10%");
        environment.put(DATA_MAX_SIZE_VARIABLE, "30%");

        assertThat(settings.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(100_000L);
    }

    @Test
    void blankCountsAsUnset() {
        properties.put(DATA_MAX_SIZE, "  ");

        assertThat(settings.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(200_000L);
    }

    @Test
    void defaultsApply() {
        assertThat(settings.maxSize(DATA_MAX_SIZE, "20%")).isEqualTo(200_000L);
        assertThat(settings.maxSize(METADATA_MAX_SIZE, "10%")).isEqualTo(100_000L);
        assertThat(settings.byteCount(DATA_MAX_READ_SIZE, "16M")).isEqualTo(16_777_216L);
    }

    @Test
    void readSizeRejectsPercent() {
        properties.put(DATA_MAX_READ_SIZE, "10%");

        assertThatThrownBy(() -> settings.byteCount(DATA_MAX_READ_SIZE, "16M"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("system property " + DATA_MAX_READ_SIZE)
                .hasMessageContaining("10%");
    }

    @Test
    void rejectedEnvironmentVariableIsNamed() {
        environment.put(DATA_MAX_SIZE_VARIABLE, "x");

        assertThatThrownBy(() -> settings.maxSize(DATA_MAX_SIZE, "20%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("environment variable " + DATA_MAX_SIZE_VARIABLE);
    }
}
