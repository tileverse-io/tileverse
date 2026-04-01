/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.rangereader.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GoogleCloudStorageRangeReaderProviderTest {

    private final GoogleCloudStorageRangeReaderProvider provider = new GoogleCloudStorageRangeReaderProvider();

    @Test
    void testFactoryLookup() {
        assertThat(RangeReaderProvider.findProvider(GoogleCloudStorageRangeReaderProvider.ID))
                .isPresent();
        assertThat(RangeReaderProvider.getProvider(GoogleCloudStorageRangeReaderProvider.ID, true))
                .isNotNull();
    }

    @Test
    void testBuildParametersAndDefaults() {
        List<RangeReaderParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .containsExactly(
                        GoogleCloudStorageRangeReaderProvider.GCS_PROJECT_ID,
                        GoogleCloudStorageRangeReaderProvider.GCS_QUOTA_PROJECT_ID,
                        GoogleCloudStorageRangeReaderProvider.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS);

        RangeReaderConfig defaults = provider.getDefaultConfig();
        assertThat(defaults.getParameter(GoogleCloudStorageRangeReaderProvider.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS))
                .hasValue(false);
    }

    @Test
    void testCanProcessRecognizedUrisOnly() {
        RangeReaderConfig config = provider.getDefaultConfig();

        assertThrows(NullPointerException.class, () -> provider.canProcess(null));
        assertThrows(NullPointerException.class, () -> provider.canProcess(config));

        assertThat(provider.canProcess(config.uri("gs://bucket/file.pmtiles"))).isTrue();
        assertThat(provider.canProcess(config.uri("https://storage.googleapis.com/bucket/file.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://storage.cloud.google.com/bucket/file.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("http://localhost:4443/storage/v1/b/bucket/o/file.pmtiles")))
                .isTrue();

        assertThat(provider.canProcess(config.uri("https://example.com/bucket/file.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.uri("https://demo-bucket.protomaps.com/v4.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.providerId("http").uri("gs://bucket/file.pmtiles")))
                .isFalse();
    }

    @Test
    void testCanProcessHeaders() {
        assertThat(provider.canProcessHeaders(
                        URI.create("https://storage.googleapis.com/bucket/file.pmtiles"), Map.of()))
                .isTrue();

        assertThat(provider.canProcessHeaders(
                        URI.create("http://localhost:4443/storage/v1/b/bucket/o/file.pmtiles"),
                        Map.of("x-goog-generation", List.of("1"))))
                .isTrue();

        assertThat(provider.canProcessHeaders(
                        URI.create("https://example.com/bucket/file.pmtiles"),
                        Map.of("x-goog-generation", List.of("1"))))
                .isFalse();
    }
}
