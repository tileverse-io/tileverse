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
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

class S3HttpClientSettingsTest {

    private final Map<String, String> systemProperties = new HashMap<>();
    private final Map<String, String> environment = new HashMap<>();
    private final ProcessSettings process = new ProcessSettings(systemProperties::get, environment::get);

    @Test
    void aProcessSettingNothingWaitsThirtySecondsForAConnection() {
        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process, OptionalLong.of(gib(8)));

        assertThat(settings.maxConcurrency()).isEqualTo(117);
        assertThat(settings.connectionTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(settings.connectionAcquisitionTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void everySettingReadsFromItsSystemProperty() {
        systemProperties.put("io.tileverse.storage.s3-http-client.max-concurrency", "200");
        systemProperties.put("io.tileverse.storage.s3-http-client.connection-timeout", "10s");
        systemProperties.put("io.tileverse.storage.s3-http-client.connection-acquisition-timeout", "PT1M");

        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process, OptionalLong.empty());

        assertThat(settings).isEqualTo(new S3HttpClientSettings(200, Duration.ofSeconds(10), Duration.ofMinutes(1)));
    }

    @Test
    void everySettingReadsFromItsEnvironmentVariable() {
        environment.put("IO_TILEVERSE_STORAGE_S3HTTPCLIENT_MAXCONCURRENCY", "200");
        environment.put("IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONTIMEOUT", "10s");
        environment.put("IO_TILEVERSE_STORAGE_S3HTTPCLIENT_CONNECTIONACQUISITIONTIMEOUT", "PT1M");

        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process, OptionalLong.empty());

        assertThat(settings).isEqualTo(new S3HttpClientSettings(200, Duration.ofSeconds(10), Duration.ofMinutes(1)));
    }

    @Test
    void theHttpClientIsBuiltWithTheSettingsAndAOneMebibyteReadWindow() {
        S3HttpClientSettings settings = new S3HttpClientSettings(20, Duration.ofSeconds(10), Duration.ofMinutes(1));
        AwsCrtAsyncHttpClient.Builder builder = mock(AwsCrtAsyncHttpClient.Builder.class, RETURNS_SELF);
        SdkAsyncHttpClient configured = mock(SdkAsyncHttpClient.class);
        when(builder.build()).thenReturn(configured);

        SdkAsyncHttpClient built;
        try (MockedStatic<AwsCrtAsyncHttpClient> crt = mockStatic(AwsCrtAsyncHttpClient.class)) {
            crt.when(AwsCrtAsyncHttpClient::builder).thenReturn(builder);
            built = settings.newAsyncHttpClient();
        }

        assertThat(built).isSameAs(configured);
        verify(builder).maxConcurrency(20);
        verify(builder).connectionTimeout(Duration.ofSeconds(10));
        verify(builder).connectionAcquisitionTimeout(Duration.ofMinutes(1));
        verify(builder).readBufferSizeInBytes(1024L * 1024L);
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

    @ParameterizedTest
    @MethodSource("memoryBudgetsAndPoolSizes")
    void thePoolSizeFollowsTheMemoryBudgetOfTheJvm(OptionalLong memoryBudget, int expected) {
        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process, memoryBudget);

        assertThat(settings.maxConcurrency()).isEqualTo(expected);
    }

    static Stream<Arguments> memoryBudgetsAndPoolSizes() {
        return Stream.of(
                Arguments.of(OptionalLong.empty(), 50),
                Arguments.of(OptionalLong.of(gib(1)), 50),
                Arguments.of(OptionalLong.of(gib(2)), 50),
                Arguments.of(OptionalLong.of(gib(4)), 58),
                Arguments.of(OptionalLong.of(gib(8)), 117),
                Arguments.of(OptionalLong.of(gib(16)), 234),
                Arguments.of(OptionalLong.of(gib(32)), 468),
                Arguments.of(OptionalLong.of(gib(64)), 500));
    }

    @ParameterizedTest
    @MethodSource("memoryLimitsHeapsAndBudgets")
    void theMemoryBudgetIsTheSmallerOfTheLimitAndFourHeaps(
            OptionalLong memoryLimit, long maxHeap, OptionalLong expected) {
        OptionalLong budget = S3HttpClientSettings.memoryBudget(memoryLimit, maxHeap);

        assertThat(budget).isEqualTo(expected);
    }

    static Stream<Arguments> memoryLimitsHeapsAndBudgets() {
        return Stream.of(
                Arguments.of(OptionalLong.of(gib(32)), gib(2), OptionalLong.of(gib(8))),
                Arguments.of(OptionalLong.of(gib(2)), mib(512), OptionalLong.of(gib(2))),
                Arguments.of(OptionalLong.of(gib(48)), gib(12), OptionalLong.of(gib(48))),
                Arguments.of(OptionalLong.empty(), gib(2), OptionalLong.of(gib(8))),
                Arguments.of(OptionalLong.of(gib(32)), Long.MAX_VALUE, OptionalLong.of(gib(32))),
                Arguments.of(OptionalLong.empty(), Long.MAX_VALUE, OptionalLong.empty()),
                Arguments.of(OptionalLong.of(gib(32)), Long.MAX_VALUE / 4 + 1, OptionalLong.of(gib(32))));
    }

    @ParameterizedTest
    @MethodSource("memoryLimitsHeapsAndPoolSizes")
    void thePoolSizeFollowsTheSmallerOfTheMemoryLimitAndFourHeaps(
            OptionalLong memoryLimit, long maxHeap, int expected) {
        OptionalLong budget = S3HttpClientSettings.memoryBudget(memoryLimit, maxHeap);

        int poolSize = S3HttpClientSettings.defaultMaxConcurrency(budget);

        assertThat(poolSize).isEqualTo(expected);
    }

    static Stream<Arguments> memoryLimitsHeapsAndPoolSizes() {
        return Stream.of(
                Arguments.of(OptionalLong.of(gib(32)), gib(2), 117),
                Arguments.of(OptionalLong.of(gib(48)), gib(1), 58),
                Arguments.of(OptionalLong.of(gib(48)), gib(12), 500),
                Arguments.of(OptionalLong.of(gib(2)), mib(512), 50));
    }

    @Test
    void aConfiguredPoolSizeWinsOverTheMemoryBudget() {
        systemProperties.put("io.tileverse.storage.s3-http-client.max-concurrency", "20");

        S3HttpClientSettings settings = S3HttpClientSettings.resolve(process, OptionalLong.of(gib(64)));

        assertThat(settings.maxConcurrency()).isEqualTo(20);
    }

    @ParameterizedTest
    @MethodSource("poolSizesAndUploadPartsInFlight")
    void anUploadKeepsAnEighthOfThePoolInFlight(int maxConcurrency, int expected) {
        S3HttpClientSettings settings =
                new S3HttpClientSettings(maxConcurrency, Duration.ofSeconds(2), Duration.ofSeconds(30));

        assertThat(settings.uploadPartsInFlight()).isEqualTo(expected);
    }

    static Stream<Arguments> poolSizesAndUploadPartsInFlight() {
        return Stream.of(
                Arguments.of(50, 6),
                Arguments.of(58, 7),
                Arguments.of(117, 14),
                Arguments.of(500, 62),
                Arguments.of(7, 1),
                Arguments.of(1, 1));
    }

    @Test
    void theJvmReportsAMemoryLimit() {
        assertThat(S3HttpClientSettings.memoryLimitOfTheJvm()).isPresent();
    }

    private static long gib(long count) {
        return count * 1024 * 1024 * 1024;
    }

    private static long mib(long count) {
        return count * 1024 * 1024;
    }
}
