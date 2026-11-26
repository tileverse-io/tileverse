/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Size settings of the shared caches. A setting is read from its system property first and from its environment
 * variable second.
 *
 * <p>The system property is named in lowercase, with dots and dashes. The environment variable has the name given to
 * that property by the relaxed binding of Spring Boot: dots become underscores, dashes are dropped, letters are
 * uppercased. The same variable works for a plain JVM and for a Spring Boot application.
 *
 * <p>A size is a share of the maximum heap, such as {@code 20%}, or a byte count. A byte count is an integer with an
 * optional unit {@code K}, {@code M} or {@code G}, in powers of 1024, optionally followed by {@code B} or {@code iB}:
 * {@code 512M}, {@code 1GiB} and {@code 4096} are byte counts. Units are case-insensitive.
 *
 * <p>A blank value counts as unset. Any other invalid value fails the read, naming its source.
 */
@NullMarked
public final class CacheSettings {

    /** The budget of the {@link CachePool#DATA} pool, as a share of the heap or a byte count. */
    public static final String DATA_MAX_SIZE = "io.tileverse.cache.data.max-size";

    /** The budget of the {@link CachePool#METADATA} pool, as a share of the heap or a byte count. */
    public static final String METADATA_MAX_SIZE = "io.tileverse.cache.metadata.max-size";

    /** The largest read kept by the byte-range cache, as a byte count. */
    public static final String DATA_MAX_READ_SIZE = "io.tileverse.cache.data.max-read-size";

    /** The budget of the {@link CachePool#DATA} pool when {@link #DATA_MAX_SIZE} is unset. */
    public static final String DEFAULT_DATA_MAX_SIZE = "20%";

    /** The budget of the {@link CachePool#METADATA} pool when {@link #METADATA_MAX_SIZE} is unset. */
    public static final String DEFAULT_METADATA_MAX_SIZE = "10%";

    /** The largest read kept by the byte-range cache when {@link #DATA_MAX_READ_SIZE} is unset. */
    public static final String DEFAULT_DATA_MAX_READ_SIZE = "16M";

    private final UnaryOperator<@Nullable String> systemProperties;
    private final UnaryOperator<@Nullable String> environment;
    private final long maxHeapBytes;

    CacheSettings(
            UnaryOperator<@Nullable String> systemProperties,
            UnaryOperator<@Nullable String> environment,
            long maxHeapBytes) {
        this.systemProperties = Objects.requireNonNull(systemProperties, "systemProperties");
        this.environment = Objects.requireNonNull(environment, "environment");
        this.maxHeapBytes = maxHeapBytes;
    }

    /** Returns the settings of this process, with shares taken of its maximum heap. */
    public static CacheSettings ofProcess() {
        long maxHeapBytes = Runtime.getRuntime().maxMemory();
        return new CacheSettings(System::getProperty, System::getenv, maxHeapBytes);
    }

    static String environmentVariableOf(String property) {
        String underscored = property.replace('.', '_');
        String undashed = underscored.replace("-", "");
        return undashed.toUpperCase(Locale.ROOT);
    }

    /**
     * Reads a size written as a share of the maximum heap from {@code 0%} to {@code 100%}, or as a byte count.
     *
     * @return the size in bytes
     * @throws IllegalArgumentException if the value is not a valid size
     */
    public long maxSize(String property, String defaultValue) {
        return read(property, defaultValue).asMaxSize(maxHeapBytes);
    }

    /**
     * Reads a size written as a byte count. A share of the heap is rejected.
     *
     * @return the size in bytes
     * @throws IllegalArgumentException if the value is not a valid byte count
     */
    public long byteCount(String property, String defaultValue) {
        return read(property, defaultValue).asByteCount();
    }

    private SetValue read(String property, String defaultValue) {
        Optional<SetValue> set = find(property);
        if (set.isPresent()) {
            return set.get();
        }
        return new SetValue("default value of " + property, defaultValue);
    }

    private Optional<SetValue> find(String property) {
        String fromProperty = systemProperties.apply(property);
        if (isSet(fromProperty)) {
            return Optional.of(new SetValue("system property " + property, fromProperty.trim()));
        }
        String variable = environmentVariableOf(property);
        String fromVariable = environment.apply(variable);
        if (isSet(fromVariable)) {
            return Optional.of(new SetValue("environment variable " + variable, fromVariable.trim()));
        }
        return Optional.empty();
    }

    private static boolean isSet(@Nullable String value) {
        return value != null && !value.isBlank();
    }

    /** A value found for a setting, and where it was found. */
    private record SetValue(String source, String text) {

        private static final Pattern PERCENT = Pattern.compile("(\\d+)%");

        private static final Pattern BYTE_COUNT =
                Pattern.compile("(\\d+)(?:([kmg])(?:i?b)?)?", Pattern.CASE_INSENSITIVE);

        private static final String EXPECTED_MAX_SIZE =
                "a share of the heap from 0% to 100%, or a byte count such as 512M";

        private static final String EXPECTED_BYTE_COUNT = "a byte count such as 16M";

        long asMaxSize(long maxHeapBytes) {
            Matcher percent = PERCENT.matcher(text);
            if (percent.matches()) {
                return shareOf(maxHeapBytes, percent.group(1));
            }
            return parseByteCount(EXPECTED_MAX_SIZE);
        }

        private long shareOf(long maxHeapBytes, String digits) {
            long percent = parseNumber(digits, EXPECTED_MAX_SIZE);
            if (percent > 100) {
                throw rejected(EXPECTED_MAX_SIZE, null);
            }
            // Dividing first keeps the product within a long: a JVM without a heap limit reports Long.MAX_VALUE.
            return maxHeapBytes / 100 * percent + maxHeapBytes % 100 * percent / 100;
        }

        long asByteCount() {
            return parseByteCount(EXPECTED_BYTE_COUNT);
        }

        private long parseByteCount(String expected) {
            Matcher numberAndUnit = BYTE_COUNT.matcher(text);
            if (!numberAndUnit.matches()) {
                throw rejected(expected, null);
            }
            long number = parseNumber(numberAndUnit.group(1), expected);
            long unitBytes = unitBytesOf(numberAndUnit.group(2));
            try {
                return Math.multiplyExact(number, unitBytes);
            } catch (ArithmeticException tooLarge) {
                throw rejected(expected, tooLarge);
            }
        }

        private long parseNumber(String digits, String expected) {
            try {
                return Long.parseLong(digits);
            } catch (NumberFormatException tooLarge) {
                throw rejected(expected, tooLarge);
            }
        }

        private static long unitBytesOf(@Nullable String unit) {
            if (unit == null) {
                return 1L;
            }
            return switch (unit.toLowerCase(Locale.ROOT)) {
                case "k" -> 1L << 10;
                case "m" -> 1L << 20;
                case "g" -> 1L << 30;
                default -> throw new IllegalStateException("Unit not matched by the byte count pattern: " + unit);
            };
        }

        private IllegalArgumentException rejected(String expected, @Nullable Exception cause) {
            String message = "Invalid " + source + ": '" + text + "', expected " + expected;
            return new IllegalArgumentException(message, cause);
        }
    }
}
