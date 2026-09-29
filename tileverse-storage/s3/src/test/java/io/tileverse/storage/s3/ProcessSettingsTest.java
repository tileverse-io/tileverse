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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ProcessSettingsTest {

    private static final String PROPERTY = "io.tileverse.storage.s3-http-client.max-concurrency";
    private static final String VARIABLE = "IO_TILEVERSE_STORAGE_S3HTTPCLIENT_MAXCONCURRENCY";
    private static final Duration DEFAULT = Duration.ofSeconds(2);

    private final Map<String, String> systemProperties = new HashMap<>();
    private final Map<String, String> environment = new HashMap<>();
    private final ProcessSettings settings = new ProcessSettings(systemProperties::get, environment::get);

    @Test
    void theEnvironmentVariableIsNamedAsSpringBootNamesIt() {
        assertThat(ProcessSettings.environmentVariableOf(PROPERTY)).isEqualTo(VARIABLE);
    }

    @Test
    void anUnsetSettingTakesTheDefault() {
        assertThat(settings.positiveInt(PROPERTY, 50)).isEqualTo(50);
        assertThat(settings.positiveDuration(PROPERTY, DEFAULT)).isEqualTo(DEFAULT);
    }

    @Test
    void aBlankSettingTakesTheDefault() {
        systemProperties.put(PROPERTY, " ");
        environment.put(VARIABLE, "");

        assertThat(settings.positiveInt(PROPERTY, 50)).isEqualTo(50);
    }

    @Test
    void theEnvironmentVariableSetsTheValue() {
        environment.put(VARIABLE, "200");

        assertThat(settings.positiveInt(PROPERTY, 50)).isEqualTo(200);
    }

    @Test
    void theSystemPropertyWinsOverTheEnvironmentVariable() {
        systemProperties.put(PROPERTY, "100");
        environment.put(VARIABLE, "200");

        assertThat(settings.positiveInt(PROPERTY, 50)).isEqualTo(100);
    }

    @ParameterizedTest
    @MethodSource("durations")
    void aDurationReadsInIsoAndInShortForm(String text, Duration expected) {
        systemProperties.put(PROPERTY, text);

        assertThat(settings.positiveDuration(PROPERTY, DEFAULT)).isEqualTo(expected);
    }

    static Stream<Arguments> durations() {
        return Stream.of(
                Arguments.of("PT2M", Duration.ofMinutes(2)),
                Arguments.of("pt0.5s", Duration.ofMillis(500)),
                Arguments.of("P1D", Duration.ofDays(1)),
                Arguments.of("1500", Duration.ofMillis(1500)),
                Arguments.of("250ns", Duration.ofNanos(250)),
                Arguments.of("250us", Duration.ofNanos(250_000)),
                Arguments.of("500ms", Duration.ofMillis(500)),
                Arguments.of("30s", Duration.ofSeconds(30)),
                Arguments.of("5m", Duration.ofMinutes(5)),
                Arguments.of("2h", Duration.ofHours(2)),
                Arguments.of("1d", Duration.ofDays(1)),
                Arguments.of(" 30S ", Duration.ofSeconds(30)));
    }

    @ParameterizedTest
    @MethodSource("rejectedIntegers")
    void anIntegerMustBePositiveAndWellFormed(String text) {
        systemProperties.put(PROPERTY, text);

        assertThatThrownBy(() -> settings.positiveInt(PROPERTY, 50))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PROPERTY)
                .hasMessageContaining(text);
    }

    static Stream<String> rejectedIntegers() {
        return Stream.of("0", "-1", "fifty", "50.5", "99999999999");
    }

    @ParameterizedTest
    @MethodSource("rejectedDurations")
    void aDurationMustBePositiveAndWellFormed(String text) {
        systemProperties.put(PROPERTY, text);

        assertThatThrownBy(() -> settings.positiveDuration(PROPERTY, DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PROPERTY)
                .hasMessageContaining(text);
    }

    static Stream<String> rejectedDurations() {
        return Stream.of("0", "0s", "PT0S", "-5s", "PT-5S", "5 minutes", "5w", "soon", "P");
    }

    @Test
    void aRejectedEnvironmentVariableIsNamedInTheFailure() {
        environment.put(VARIABLE, "fifty");

        assertThatThrownBy(() -> settings.positiveInt(PROPERTY, 50))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(VARIABLE)
                .hasMessageContaining("fifty");
    }
}
