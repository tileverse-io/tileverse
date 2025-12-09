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
package io.tileverse.rangereader.s3;

import static io.tileverse.rangereader.s3.S3RangeReaderProvider.*;
import static io.tileverse.rangereader.s3.S3RangeReaderProvider.S3_AWS_SECRET_ACCESS_KEY;
import static io.tileverse.rangereader.s3.S3RangeReaderProvider.S3_FORCE_PATH_STYLE;
import static io.tileverse.rangereader.s3.S3RangeReaderProvider.S3_REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.s3.S3RangeReader.Builder;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;

/**
 * Tests for {@link S3RangeReaderProvider}
 */
class S3RangeReaderProviderTest {

    private S3RangeReaderProvider provider = new S3RangeReaderProvider();

    @Test
    void testFactoryLookup() {
        assertThat(RangeReaderProvider.getProviders().stream().anyMatch(S3RangeReaderProvider.class::isInstance))
                .isTrue();
        assertThat(RangeReaderProvider.getAvailableProviders().stream()
                        .anyMatch(S3RangeReaderProvider.class::isInstance))
                .isTrue();
        assertThat(RangeReaderProvider.findProvider(S3RangeReaderProvider.ID)).isPresent();
        assertThat(RangeReaderProvider.getProvider(S3RangeReaderProvider.ID, true))
                .isNotNull();

        System.setProperty(S3RangeReaderProvider.ENABLED_KEY, "false");
        try {
            assertThat(RangeReaderProvider.getProviders().stream().anyMatch(S3RangeReaderProvider.class::isInstance))
                    .isTrue();
            assertThat(RangeReaderProvider.getAvailableProviders().stream()
                            .anyMatch(S3RangeReaderProvider.class::isInstance))
                    .isFalse();
            assertThat(RangeReaderProvider.findProvider(S3RangeReaderProvider.ID))
                    .isPresent();
            IllegalStateException ex = assertThrows(
                    IllegalStateException.class, () -> RangeReaderProvider.getProvider(S3RangeReaderProvider.ID, true));
            assertThat(ex.getMessage().contains("The specified RangeReaderProvider is not available: s3"));
        } finally {
            System.clearProperty(S3RangeReaderProvider.ENABLED_KEY);
        }
    }

    @Test
    void isAvailable() {
        assertThat(provider.isAvailable()).isTrue();
        System.setProperty(S3RangeReaderProvider.ENABLED_KEY, "false");
        try {
            assertThat(provider.isAvailable()).isFalse();
        } finally {
            System.clearProperty(S3RangeReaderProvider.ENABLED_KEY);
        }
    }

    @Test
    void buildParameters() {
        List<RangeReaderParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .isEqualTo(List.of(
                        S3_FORCE_PATH_STYLE,
                        S3_REGION,
                        S3_AWS_ACCESS_KEY_ID,
                        S3_AWS_SECRET_ACCESS_KEY,
                        S3_USE_DEFAULT_CREDENTIALS_PROVIDER,
                        S3_DEFAULT_CREDENTIALS_PROFILE));
    }

    @Test
    void defaultConfig() {
        RangeReaderConfig config = provider.getDefaultConfig();
        for (RangeReaderParameter<?> param : S3RangeReaderProvider.PARAMS) {
            Object value = config.getParameter(param).orElse(null);
            assertThat(value).isEqualTo(param.defaultValue().orElse(null));
        }
    }

    @Test
    void canProcess() {
        assertThrows(NullPointerException.class, () -> provider.canProcess(null));
        RangeReaderConfig config = provider.getDefaultConfig();
        NullPointerException npe = assertThrows(NullPointerException.class, () -> provider.canProcess(config));
        assertThat(npe.getMessage()).contains("config uri is null");

        // Invalid S3 URIs
        assertThrows(IllegalArgumentException.class, () -> provider.canProcess(config.uri("s3:")));
        assertThrows(IllegalArgumentException.class, () -> provider.canProcess(config.uri("s3://")));

        // Unsupported schemes
        assertThat(provider.canProcess(config.uri("ftp://my-bucket/my-blob"))).isFalse();

        // Empty paths for HTTP URLs should fail
        assertThat(provider.canProcess(config.uri("http://localhost:9000/"))).isFalse();
        assertThat(provider.canProcess(config.uri("https://s3.amazonaws.com/"))).isFalse();

        // S3 URIs - bucket only points to bucket root, not a file (should use HTTP)
        assertThat(provider.canProcess(config.uri("s3://my-bucket"))).isFalse();
        assertThat(provider.canProcess(config.uri("s3://my-bucket/my-blob"))).isTrue();

        // Valid AWS S3 URLs
        assertThat(provider.canProcess(config.uri("https://my-bucket.s3.amazonaws.com/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://my-bucket.s3.us-west-2.amazonaws.com/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://s3.amazonaws.com/my-bucket/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://s3.us-west-2.amazonaws.com/my-bucket/my-blob")))
                .isTrue();

        // Valid MinIO and S3-compatible URLs
        assertThat(provider.canProcess(config.uri("http://localhost:9000/my-bucket/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://minio.example.com/my-bucket/my-blob")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://storage.googleapis.com/my-bucket/my-blob")))
                .isTrue();

        // Bucket root URLs (with trailing slash) point to bucket root, not files (should use HTTP)
        assertThat(provider.canProcess(config.uri("s3://my-bucket/"))).isFalse();
        assertThat(provider.canProcess(config.uri("http://localhost:9000/my-bucket/")))
                .isFalse();
        assertThat(provider.canProcess(config.uri("https://s3.amazonaws.com/my-bucket/")))
                .isFalse();
    }

    @Test
    void canProcessWithSpecialCharacters() {
        RangeReaderConfig config = provider.getDefaultConfig();

        // URL encoded characters should be handled
        assertThat(provider.canProcess(config.uri("s3://my-bucket/path/file%20with%20spaces.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://s3.amazonaws.com/my-bucket/file%2Bwith%26symbols.txt")))
                .isTrue();

        // Complex nested paths
        assertThat(provider.canProcess(config.uri("s3://my-bucket/level1/level2/level3/file.json")))
                .isTrue();
        assertThat(provider.canProcess(
                        config.uri("http://localhost:9000/my-bucket/path/to/file-name_with.special+chars.txt")))
                .isTrue();
    }

    @Test
    void canProcessDifferentS3Services() {
        RangeReaderConfig config = provider.getDefaultConfig();

        // AWS S3 formats
        assertThat(provider.canProcess(config.uri("s3://my-bucket/file.txt"))).isTrue();
        assertThat(provider.canProcess(config.uri("https://my-bucket.s3.amazonaws.com/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt")))
                .isTrue();

        // MinIO
        assertThat(provider.canProcess(config.uri("http://localhost:9000/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("http://192.168.1.100:9000/my-bucket/file.txt")))
                .isTrue();

        // Other S3-compatible services
        assertThat(provider.canProcess(config.uri("https://storage.googleapis.com/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://digitaloceanspaces.com/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://wasabisys.com/my-bucket/file.txt")))
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://s3.company.internal/my-bucket/file.txt")))
                .isTrue();
    }

    @Test
    void canProcessForcePathStyleParameter() {
        RangeReaderConfig config = provider.getDefaultConfig();

        // The FORCE_PATH_STYLE parameter doesn't affect canProcess() anymore
        // because the URL parsing now automatically detects the required style

        config.setParameter(S3_FORCE_PATH_STYLE.key(), false);
        // These should still work because the parser detects the URL format
        assertThat(provider.canProcess(config.uri("http://localhost:9000/my-bucket/file.txt")))
                .as("MinIO URLs work regardless of force-path-style setting")
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://my-bucket.s3.amazonaws.com/file.txt")))
                .as("AWS virtual hosted-style URLs work regardless of force-path-style setting")
                .isTrue();

        config.setParameter(S3_FORCE_PATH_STYLE.key(), true);
        assertThat(provider.canProcess(config.uri("http://localhost:9000/my-bucket/file.txt")))
                .as("MinIO URLs work regardless of force-path-style setting")
                .isTrue();
        assertThat(provider.canProcess(config.uri("https://my-bucket.s3.amazonaws.com/file.txt")))
                .as("AWS virtual hosted-style URLs work regardless of force-path-style setting")
                .isTrue();
    }

    @Test
    void canProcessEdgeCases() {
        RangeReaderConfig config = provider.getDefaultConfig();

        // Endpoint-only URLs (like what MinIO container provides) should fail
        // because they don't contain bucket/key information
        assertThat(provider.canProcess(config.uri("http://localhost:9000")))
                .as("Endpoint-only URLs cannot be processed")
                .isFalse();

        // Invalid URLs that look like they might be S3
        assertThat(provider.canProcess(config.uri("http://not-s3-service.com")))
                .as("URLs without bucket/key path cannot be processed")
                .isFalse();

        // URLs with only bucket but no key point to bucket root, not files (should use HTTP)
        assertThat(provider.canProcess(config.uri("http://localhost:9000/my-bucket")))
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

    @Test
    void createWithURI() throws IOException {
        assertThrows(NullPointerException.class, () -> provider.create((URI) null));

        provider = Mockito.spy(provider);
        ArgumentCaptor<RangeReaderConfig> configCaptor = ArgumentCaptor.forClass(RangeReaderConfig.class);

        // Create a mock RangeReader to return instead of calling real method
        RangeReader mockReader = mock(RangeReader.class);
        doReturn(mockReader).when(provider).create(configCaptor.capture());

        URI testUri = URI.create("s3://my-bucket/my-key");
        RangeReader result = provider.create(testUri);

        // Verify the result is our mock
        assertThat(result).isSameAs(mockReader);

        // Validate the captured config
        RangeReaderConfig capturedConfig = configCaptor.getValue();
        assertThat(capturedConfig.uri()).isEqualTo(testUri);
        //        assertThat(capturedConfig.providerId()).isEqualTo("s3");
        assertThat(capturedConfig.getParameter(S3_FORCE_PATH_STYLE)).hasValue(true);
    }

    @Test
    void createWithConfig() throws IOException {
        assertThrows(NullPointerException.class, () -> provider.create((RangeReaderConfig) null));
        RangeReaderConfig config = new RangeReaderConfig();
        NullPointerException npe = assertThrows(NullPointerException.class, () -> provider.create(config));
        assertThat(npe).hasMessageContaining("URI cannot be null");

        config.uri("s3://my-bucket/file.txt");
        S3RangeReader.Builder rangeReaderBuilder = provider.prepareRangeReaderBuilder(config);
        assertThat(rangeReaderBuilder)
                .hasFieldOrPropertyWithValue("s3Location.endpoint", null)
                .hasFieldOrPropertyWithValue("s3Location.bucket", "my-bucket")
                .hasFieldOrPropertyWithValue("s3Location.key", "file.txt");

        config.setParameter(S3_FORCE_PATH_STYLE, true);
        config.setParameter(S3_REGION, "us-west-2");
        config.setParameter(S3_AWS_ACCESS_KEY_ID, "access-key");
        config.setParameter(S3_AWS_SECRET_ACCESS_KEY, "secret-key");

        rangeReaderBuilder = provider.prepareRangeReaderBuilder(config);
        assertThat(rangeReaderBuilder)
                .hasFieldOrPropertyWithValue("forcePathStyle", true)
                .hasFieldOrPropertyWithValue("region", Region.US_WEST_2)
                .hasFieldOrPropertyWithValue("awsAccessKeyId", "access-key")
                .hasFieldOrPropertyWithValue("awsSecretAccessKey", "secret-key");
    }

    @Test
    void createWithCustomRegion() throws IOException {
        final String customRegion = "eu-central-12";
        assertThat(Region.regions().stream().anyMatch(region -> region.id().equals(customRegion)))
                .isFalse();

        RangeReaderConfig config = new RangeReaderConfig().uri("s3://my-bucket/file.txt");

        config.setParameter(S3_REGION, customRegion);

        Builder rangeReaderBuilder = provider.prepareRangeReaderBuilder(config);
        assertThat(rangeReaderBuilder).hasFieldOrPropertyWithValue("region", Region.of(customRegion));
    }
}
