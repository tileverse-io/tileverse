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

class S3HttpClientSettingsTest {

    private final Map<String, String> systemProperties = new HashMap<>();
    private final Map<String, String> environment = new HashMap<>();
    private final ProcessSettings process = new ProcessSettings(systemProperties::get, environment::get);

    @Test
    void aProcessSettingNothingWaitsThirtySecondsForAConnection() {
        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process);

        assertThat(settings.maxConcurrency()).isEqualTo(50);
        assertThat(settings.connectionTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(settings.connectionAcquisitionTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void everySettingReadsFromItsSystemProperty() {
        systemProperties.put("io.tileverse.storage.s3-http-client.max-concurrency", "200");
        systemProperties.put("io.tileverse.storage.s3-http-client.connection-timeout", "10s");
        systemProperties.put("io.tileverse.storage.s3-http-client.connection-acquisition-timeout", "PT1M");

        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process);

        assertThat(settings).isEqualTo(new S3HttpClientSettings(200, Duration.ofSeconds(10), Duration.ofMinutes(1)));
    }

    @Test
    void everySettingReadsFromItsEnvironmentVariable() {
        environment.put("IO_TILEVERSE_STORAGE_S3HTTPCLIENT_MAXCONCURRENCY", "200");
        environment.put("IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONTIMEOUT", "10s");
        environment.put("IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONACQUISITIONTIMEOUT", "PT1M");

        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process);

        assertThat(settings).isEqualTo(new S3HttpClientSettings(200, Duration.ofSeconds(10), Duration.ofMinutes(1)));
    }

    @ParameterizedTest
    @MethodSource("rejectedSettings")
    void everySettingMustBePositive(int maxConcurrency, Duration connectionTimeout, Duration acquisitionTimeout) {
        assertThatThrownBy(() -> new S3HttpClientSettings(maxConcurrency, connectionTimeout, acquisitionTimeout))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> rejectedSettings() {
        Duration valid = Duration.ofSeconds(1);
        return Stream.of(
                Arguments.of(0, valid, valid),
                Arguments.of(-1, valid, valid),
                Arguments.of(1, Duration.ZERO, valid),
                Arguments.of(1, Duration.ofSeconds(-1), valid),
                Arguments.of(1, valid, Duration.ZERO),
                Arguments.of(1, valid, Duration.ofSeconds(-1)));
    }
}
