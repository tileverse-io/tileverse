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
package io.tileverse.storage;

import static io.tileverse.storage.StorageFactoryIT.testCreate;
import static io.tileverse.storage.StorageFactoryIT.testFindBestProvider;
import static io.tileverse.storage.StorageFactoryIT.testS3;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.gcs.GoogleCloudStorageProvider;
import io.tileverse.storage.http.HttpStorageProvider;
import io.tileverse.storage.s3.S3StorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for {@link StorageFactory} resolving real URLs to their {@link StorageProvider}: an https URL of a
 * cloud object opens the HTTP backend, and the cloud backend when the config names its provider.
 */
@Testcontainers(disabledWithoutDocker = true)
class StorageFactoryOnlineIT {

    /**
     * Anonymous-read object in the AWS Open Data sentinel-cogs bucket, unchanged since 2020. Overture Maps buckets are
     * unsuitable here: their release objects expire 60 days after publication (rule-id "release data 60 day
     * retention"), which is how the previous URLs went dark.
     */
    private static final String S3_BUCKET = "sentinel-cogs";

    private static final String S3_REGION = "us-west-2";
    private static final String S3_KEY = "sentinel-s2-l2a-cogs/33/T/UL/2020/6/S2A_33TUL_20200604_0_L2A/B01.tif";

    /** The legacy global endpoint: no region in the URL. */
    private static final String S3_LEGACY_VIRTUAL_HOSTED_URL = "https://" + S3_BUCKET + ".s3.amazonaws.com/" + S3_KEY;

    private static final String S3_VIRTUAL_HOSTED_URL =
            "https://" + S3_BUCKET + ".s3." + S3_REGION + ".amazonaws.com/" + S3_KEY;
    private static final String S3_PATH_STYLE_URL =
            "https://s3." + S3_REGION + ".amazonaws.com/" + S3_BUCKET + "/" + S3_KEY;

    private static final String GCS_OBJECT =
            "gcp-public-data-landsat/LC08/01/001/003/LC08_L1GT_001003_20140812_20170420_01_T2/LC08_L1GT_001003_20140812_20170420_01_T2_B3.TIF";
    private static final String GCS_HTTPS_URL = "https://storage.googleapis.com/" + GCS_OBJECT;

    @Test
    @DisplayName("HTTPS URL of a plain web server reads through HTTP")
    void plainHttpsUrl() throws IOException {
        // httpbin.io provides a /range/1024 endpoint that streams n bytes and allows specifying a Range header to
        // select a subset of the data.
        String url = "https://httpbin.io/range/1024";
        testFindBestProvider(URI.create(url), HttpStorageProvider.class);

        StorageConfig config = new StorageConfig(url);
        try (RangeReader reader = testCreate(config)) {
            assertThat(reader.size()).hasValue(1024);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("publicCloudObjectUrls")
    @DisplayName("https:// URL of a public cloud object with no provider named reads through HTTP")
    void httpsCloudUrlWithNoProviderReadsThroughHttp(String url) throws IOException {
        testFindBestProvider(URI.create(url), HttpStorageProvider.class);

        try (RangeReader reader = testCreate(new StorageConfig(url))) {
            assertThat(reader.size()).isPresent();
        }
    }

    static Stream<String> publicCloudObjectUrls() {
        return Stream.of(S3_LEGACY_VIRTUAL_HOSTED_URL, S3_VIRTUAL_HOSTED_URL, S3_PATH_STYLE_URL, GCS_HTTPS_URL);
    }

    @Test
    @DisplayName("GCS https:// URL with the GCS provider named reads through GCS")
    void gcsHttpsUrlWithTheProviderNamed() throws IOException {
        StorageConfig config = new StorageConfig(GCS_HTTPS_URL)
                .providerId(GoogleCloudStorageProvider.ID)
                .setParameter(GoogleCloudStorageProvider.GCS_ANONYMOUS, true);
        testFindBestProvider(config, GoogleCloudStorageProvider.class);

        try (RangeReader reader = testCreate(config)) {
            assertThat(reader.size()).isPresent();
        }
    }

    @Test
    @DisplayName("GCS gs:// URL reads through GCS")
    void gcsGsUrl() throws IOException {
        String gcsUrl = "gs://" + GCS_OBJECT;
        testFindBestProvider(URI.create(gcsUrl), GoogleCloudStorageProvider.class);

        StorageConfig config = new StorageConfig(gcsUrl).setParameter(GoogleCloudStorageProvider.GCS_ANONYMOUS, true);
        try (RangeReader reader = testCreate(config)) {
            assertThat(reader.size()).isPresent();
        }
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style (legacy format) URL with the S3 provider named reads through S3")
    void s3HttpsUrlVirtualHostedStyle() throws IOException {
        // the legacy global endpoint has no region in the URL; the provider requires it from configuration
        try (RangeReader reader = testS3(S3_LEGACY_VIRTUAL_HOSTED_URL, S3_REGION)) {
            assertThat(reader.size()).isPresent();
        }
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style URL with region and the S3 provider named reads through S3")
    void s3HttpsUrlVirtualHostedWithRegion() throws IOException {
        try (RangeReader reader = testS3(S3_VIRTUAL_HOSTED_URL)) {
            assertThat(reader.size()).isPresent();
        }
    }

    @Test
    @DisplayName("S3 https:// path-style URL with region and the S3 provider named reads through S3")
    void s3PathStyleUrlWithRegion() throws IOException {
        try (RangeReader reader = testS3(S3_PATH_STYLE_URL)) {
            assertThat(reader.size()).isPresent();
        }
    }

    @Test
    @DisplayName("S3 s3:// URL reads through S3")
    void s3Url() throws IOException {
        // s3:// URIs have no region; the provider requires it from configuration or the AWS environment
        StorageConfig config = new StorageConfig("s3://" + S3_BUCKET + "/" + S3_KEY)
                .setParameter(S3StorageProvider.S3_ANONYMOUS, true)
                .setParameter(S3StorageProvider.S3_REGION, S3_REGION);
        testFindBestProvider(config, S3StorageProvider.class);

        try (RangeReader reader = testCreate(config)) {
            assertThat(reader.size()).isPresent();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("s3HttpsUrls")
    @DisplayName("S3 https:// URL with the HTTP provider named reads through HTTP")
    void s3HttpsUrlWithTheHttpProviderNamed(String url) throws IOException {
        StorageConfig config = new StorageConfig(url).providerId(HttpStorageProvider.ID);
        testFindBestProvider(config, HttpStorageProvider.class);

        try (RangeReader reader = testCreate(config)) {
            assertThat(reader.size()).isPresent();
        }
    }

    static Stream<String> s3HttpsUrls() {
        return Stream.of(S3_LEGACY_VIRTUAL_HOSTED_URL, S3_VIRTUAL_HOSTED_URL, S3_PATH_STYLE_URL);
    }

    @Test
    @DisplayName("Custom-domain HTTPS URL reads through HTTP")
    void customDomainUrl() throws IOException {
        URI uri = URI.create("https://pmtiles.io/protomaps(vector)ODbL_firenze.pmtiles");
        testFindBestProvider(uri, HttpStorageProvider.class);

        try (RangeReader reader = testCreate(new StorageConfig(uri))) {
            assertThat(reader.size()).isPresent();
        }
    }
}
