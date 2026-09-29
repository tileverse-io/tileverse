/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.s3;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Settings of the whole process, for resources shared by every Storage and described by no Storage parameter. A setting
 * is read from its system property first and from its environment variable second.
 *
 * <p>The system property is named in lowercase, with dots and dashes. The environment variable has the name given to
 * that property by the relaxed binding of Spring Boot: dots become underscores, dashes are dropped, letters are
 * uppercased. A deployment sets one variable for a plain JVM and for a Spring Boot application alike.
 *
 * <p>A blank value counts as unset. Any other value unfit for its setting fails the read, naming its source.
 */
@NullMarked
final class ProcessSettings {

    private final UnaryOperator<@Nullable String> systemProperties;
    private final UnaryOperator<@Nullable String> environment;

    ProcessSettings(UnaryOperator<@Nullable String> systemProperties, UnaryOperator<@Nullable String> environment) {
        this.systemProperties = Objects.requireNonNull(systemProperties, "systemProperties");
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    static ProcessSettings ofProcess() {
        return new ProcessSettings(System::getProperty, System::getenv);
    }

    static String environmentVariableOf(String property) {
        String underscored = property.replace('.', '_');
        String undashed = underscored.replace("-", "");
        return undashed.toUpperCase(Locale.ROOT);
    }

    /** @throws IllegalArgumentException if the value set is no positive integer */
    int positiveInt(String property, int defaultValue) {
        Optional<SetValue> set = find(property);
        if (set.isEmpty()) {
            return defaultValue;
        }
        return set.get().asPositiveInt();
    }

    /**
     * Reads a duration written as ISO-8601 ({@code PT30S}) or as a number and a unit ({@code 30s}): {@code ns},
     * {@code us}, {@code ms}, {@code s}, {@code m}, {@code h} or {@code d}, milliseconds without a unit.
     *
     * @throws IllegalArgumentException if the value set is no positive duration
     */
    Duration positiveDuration(String property, Duration defaultValue) {
        Optional<SetValue> set = find(property);
        if (set.isEmpty()) {
            return defaultValue;
        }
        return set.get().asPositiveDuration();
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

        private static final Pattern NUMBER_AND_UNIT = Pattern.compile("([+-]?\\d+)([a-z]{0,2})");

        private static final Map<String, ChronoUnit> UNITS = Map.of(
                "ns", ChronoUnit.NANOS,
                "us", ChronoUnit.MICROS,
                "ms", ChronoUnit.MILLIS,
                "", ChronoUnit.MILLIS,
                "s", ChronoUnit.SECONDS,
                "m", ChronoUnit.MINUTES,
                "h", ChronoUnit.HOURS,
                "d", ChronoUnit.DAYS);

        int asPositiveInt() {
            int value = parseInt();
            if (value <= 0) {
                throw rejected("a positive integer", null);
            }
            return value;
        }

        private int parseInt() {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException malformed) {
                throw rejected("a positive integer", malformed);
            }
        }

        Duration asPositiveDuration() {
            Duration value = parseDuration();
            if (value.isZero() || value.isNegative()) {
                throw rejected("a positive duration", null);
            }
            return value;
        }

        private Duration parseDuration() {
            try {
                Matcher numberAndUnit = NUMBER_AND_UNIT.matcher(text.toLowerCase(Locale.ROOT));
                if (numberAndUnit.matches()) {
                    return durationOf(numberAndUnit.group(1), numberAndUnit.group(2));
                }
                return Duration.parse(text);
            } catch (DateTimeException | ArithmeticException | NumberFormatException malformed) {
                throw rejected("a positive duration such as PT30S, 30s or 500ms", malformed);
            }
        }

        private Duration durationOf(String number, String unit) {
            ChronoUnit chronoUnit = UNITS.get(unit);
            if (chronoUnit == null) {
                throw new DateTimeException("unknown unit: " + unit);
            }
            return Duration.of(Long.parseLong(number), chronoUnit);
        }

        private IllegalArgumentException rejected(String expected, @Nullable Exception cause) {
            String message = "Invalid " + source + ": '" + text + "', expected " + expected;
            return new IllegalArgumentException(message, cause);
        }
    }
}
