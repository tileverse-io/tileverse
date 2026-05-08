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
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GoogleCloudStorageProviderTest {

    private final GoogleCloudStorageProvider provider = new GoogleCloudStorageProvider();

    @Test
    void testFactoryLookup() {
        assertThat(StorageProvider.findProvider(GoogleCloudStorageProvider.ID)).isPresent();
        assertThat(StorageProvider.getProvider(GoogleCloudStorageProvider.ID, true))
                .isNotNull();
    }

    @Test
    void testBuildParametersAndDefaults() {
        List<StorageParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .containsExactly(
                        GoogleCloudStorageProvider.GCS_PROJECT_ID,
                        GoogleCloudStorageProvider.GCS_QUOTA_PROJECT_ID,
                        GoogleCloudStorageProvider.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS,
                        GoogleCloudStorageProvider.GCS_HOST);

        StorageConfig defaults = provider.getDefaultConfig();
        assertThat(defaults.getParameter(GoogleCloudStorageProvider.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS))
                .hasValue(false);
    }

    @Test
    void testCanProcessRecognizedUrisOnly() {
        StorageConfig config = provider.getDefaultConfig();

        assertThatThrownBy(() -> provider.canProcess(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> provider.canProcess(config)).isInstanceOf(NullPointerException.class);

        assertThat(provider.canProcess(config.baseUri("gs://bucket/file.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://storage.googleapis.com/bucket/file.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://storage.cloud.google.com/bucket/file.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("http://localhost:4443/storage/v1/b/bucket/o/file.pmtiles")))
                .isTrue();

        assertThat(provider.canProcess(config.baseUri("https://example.com/bucket/file.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.baseUri("https://demo-bucket.protomaps.com/v4.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.providerId("http").baseUri("gs://bucket/file.pmtiles")))
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
