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
package io.tileverse.storage.s3;

import static io.tileverse.storage.s3.S3StorageProvider.S3_ANONYMOUS;
import static io.tileverse.storage.s3.S3StorageProvider.S3_AWS_ACCESS_KEY_ID;
import static io.tileverse.storage.s3.S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY;
import static io.tileverse.storage.s3.S3StorageProvider.S3_DEFAULT_CREDENTIALS_PROFILE;
import static io.tileverse.storage.s3.S3StorageProvider.S3_FORCE_PATH_STYLE;
import static io.tileverse.storage.s3.S3StorageProvider.S3_REGION;
import static io.tileverse.storage.s3.S3StorageProvider.S3_USE_DEFAULT_CREDENTIALS_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

class S3StorageProviderTest {

    private S3StorageProvider provider = new S3StorageProvider();

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void testFactoryLookup() {
        assertThat(StorageProvider.findProviders().anyMatch(S3StorageProvider.class::isInstance))
                .isTrue();
        assertThat(StorageProvider.getAvailableProviders().stream().anyMatch(S3StorageProvider.class::isInstance))
                .isTrue();
        assertThat(StorageProvider.findProvider(S3StorageProvider.ID)).isPresent();
        assertThat(StorageProvider.getProvider(S3StorageProvider.ID, true)).isNotNull();

        System.setProperty(S3StorageProvider.ENABLED_KEY, "false");
        try {
            assertThat(StorageProvider.findProviders().anyMatch(S3StorageProvider.class::isInstance))
                    .isTrue();
            assertThat(StorageProvider.getAvailableProviders().stream().anyMatch(S3StorageProvider.class::isInstance))
                    .isFalse();
            assertThat(StorageProvider.findProvider(S3StorageProvider.ID)).isPresent();
            assertThatThrownBy(() -> StorageProvider.getProvider(S3StorageProvider.ID, true))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("The specified StorageProvider is not available: s3");
        } finally {
            System.clearProperty(S3StorageProvider.ENABLED_KEY);
        }
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void isAvailable() {
        assertThat(provider.isAvailable()).isTrue();
        System.setProperty(S3StorageProvider.ENABLED_KEY, "false");
        try {
            assertThat(provider.isAvailable()).isFalse();
        } finally {
            System.clearProperty(S3StorageProvider.ENABLED_KEY);
        }
    }

    @Test
    void buildParameters() {
        List<StorageParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .isEqualTo(List.of(
                        S3_FORCE_PATH_STYLE,
                        S3_REGION,
                        S3_ANONYMOUS,
                        S3_AWS_ACCESS_KEY_ID,
                        S3_AWS_SECRET_ACCESS_KEY,
                        S3_USE_DEFAULT_CREDENTIALS_PROVIDER,
                        S3_DEFAULT_CREDENTIALS_PROFILE));
    }

    @Test
    void defaultConfig() {
        StorageConfig config = provider.getDefaultConfig();
        for (StorageParameter<?> param : S3StorageProvider.PARAMS) {
            Object value = config.getParameter(param).orElse(null);
            assertThat(value).isEqualTo(param.defaultValue().orElse(null));
        }
    }

    @Test
    @SuppressWarnings("java:S5778")
    void canProcess() {
        assertThatThrownBy(() -> provider.canProcess(null)).isInstanceOf(NullPointerException.class);
        StorageConfig config = provider.getDefaultConfig();
        assertThatThrownBy(() -> provider.canProcess(config))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("config baseUri is null");

        // Invalid S3 URIs: StorageConfig.baseUri rejects malformed URIs eagerly, so the IAE is thrown from
        // baseUri() itself rather than canProcess(). Keeping both calls in the lambda is intentional.
        assertThatThrownBy(() -> provider.canProcess(config.baseUri("s3:")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> provider.canProcess(config.baseUri("s3://")))
                .isInstanceOf(IllegalArgumentException.class);

        // Unsupported schemes
        assertThat(provider.canProcess(config.baseUri("ftp://my-bucket/my-blob")))
                .isFalse();

        // Empty paths for HTTP URLs should fail
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/")))
                .isFalse();
        assertThat(provider.canProcess(config.baseUri("https://s3.amazonaws.com/")))
                .isFalse();

        // S3 URIs: bucket-only is accepted (Storage use case); bucket+key is the range-reader case.
        // The strict bucket+key requirement is preserved only for ambiguous http(s):// URIs above.
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket"))).isTrue();
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket/"))).isTrue();
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket/my-blob")))
                .isTrue();

        // Valid AWS S3 URLs
        assertThat(provider.canProcess(config.baseUri("https://my-bucket.s3.amazonaws.com/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://my-bucket.s3.us-west-2.amazonaws.com/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://s3.amazonaws.com/my-bucket/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://s3.us-west-2.amazonaws.com/my-bucket/my-blob")))
                .isTrue();

        // Valid MinIO and S3-compatible URLs
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/my-bucket/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://minio.example.com/my-bucket/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://storage.googleapis.com/my-bucket/my-blob")))
                .isTrue();

        // Bucket root URLs: s3:// scheme is now accepted (Storage use case).
        // http(s):// without a key remains rejected to fall back to HTTP for ambiguous custom domains.
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket/"))).isTrue();
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/my-bucket/")))
                .isFalse();
        assertThat(provider.canProcess(config.baseUri("https://s3.amazonaws.com/my-bucket/")))
                .isFalse();
    }

    @Test
    void canProcessWithSpecialCharacters() {
        StorageConfig config = provider.getDefaultConfig();

        // URL encoded characters should be handled
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket/path/file%20with%20spaces.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://s3.amazonaws.com/my-bucket/file%2Bwith%26symbols.txt")))
                .isTrue();

        // Complex nested paths
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket/level1/level2/level3/file.json")))
                .isTrue();
        assertThat(provider.canProcess(
                        config.baseUri("http://localhost:9000/my-bucket/path/to/file-name_with.special+chars.txt")))
                .isTrue();
    }

    @Test
    void canProcessDifferentS3Services() {
        StorageConfig config = provider.getDefaultConfig();

        // AWS S3 formats
        assertThat(provider.canProcess(config.baseUri("s3://my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://my-bucket.s3.amazonaws.com/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt")))
                .isTrue();

        // MinIO
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("http://192.168.1.100:9000/my-bucket/file.txt")))
                .isTrue();

        // Other S3-compatible services
        assertThat(provider.canProcess(config.baseUri("https://storage.googleapis.com/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://digitaloceanspaces.com/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://wasabisys.com/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://s3.company.internal/my-bucket/file.txt")))
                .isTrue();
    }

    @Test
    void canProcessForcePathStyleParameter() {
        StorageConfig config = provider.getDefaultConfig();

        // The FORCE_PATH_STYLE parameter doesn't affect canProcess() anymore
        // because the URL parsing now automatically detects the required style

        config.setParameter(S3_FORCE_PATH_STYLE.key(), false);
        // These should still work because the parser detects the URL format
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/my-bucket/file.txt")))
                .as("MinIO URLs work regardless of force-path-style setting")
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://my-bucket.s3.amazonaws.com/file.txt")))
                .as("AWS virtual hosted-style URLs work regardless of force-path-style setting")
                .isTrue();

        config.setParameter(S3_FORCE_PATH_STYLE.key(), true);
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/my-bucket/file.txt")))
                .as("MinIO URLs work regardless of force-path-style setting")
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("https://my-bucket.s3.amazonaws.com/file.txt")))
                .as("AWS virtual hosted-style URLs work regardless of force-path-style setting")
                .isTrue();
    }

    @Test
    void canProcessEdgeCases() {
        StorageConfig config = provider.getDefaultConfig();

        // Endpoint-only URLs (like what MinIO container provides) should fail
        // because they don't contain bucket/key information
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000")))
                .as("Endpoint-only URLs cannot be processed")
                .isFalse();

        // Invalid URLs that look like they might be S3
        assertThat(provider.canProcess(config.baseUri("http://not-s3-service.com")))
                .as("URLs without bucket/key path cannot be processed")
                .isFalse();

        // URLs with only bucket but no key point to bucket root, not files (should use HTTP)
        assertThat(provider.canProcess(config.baseUri("http://localhost:9000/my-bucket")))
                .as("Bucket root URLs should use HTTP, not S3 client")
                .isFalse();
    }

    @Test
    void canProcessHeaders() {
        URI uri = URI.create("http://localhost:9000/my-bucket/my-object");
        Map<String, List<String>> headers = Map.of("x-custom-header", List.of(), "x-amz-request-id", List.of());
        assertThat(provider.canProcessHeaders(uri, headers)).isTrue();

        headers = Map.of("x-custom-header", List.of(), "X-Amz-Request-Id", List.of());
        assertThat(provider.canProcessHeaders(uri, headers)).isTrue();

        headers = Map.of("x-custom-header", List.of());
        assertThat(provider.canProcessHeaders(uri, headers)).isFalse();
    }

    // The legacy create(URI) / create(StorageConfig) / prepareRangeReaderBuilder(...) factory paths
    // were removed from the SPI; per-key reads now flow through StorageFactory.open(...).openRangeReader(key).
    // End-to-end coverage of the S3 reader path lives in the S3 module's TCK and ITs.
}
