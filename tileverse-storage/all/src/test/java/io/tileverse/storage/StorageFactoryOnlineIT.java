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

import static io.tileverse.storage.StorageFactoryIT.testAzureBlob;
import static io.tileverse.storage.StorageFactoryIT.testCreate;
import static io.tileverse.storage.StorageFactoryIT.testFindBestProvider;
import static io.tileverse.storage.StorageFactoryIT.testS3;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.gcs.GoogleCloudStorageProvider;
import io.tileverse.storage.http.HttpStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for {@link StorageFactory} resolving URLs to the correct {@link StorageProvider} implementation
 * (e.g. several providers accept http(s) URLs)
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

    @Test
    @DisplayName("HTTPS URL that doesn't match a cloud provider uses HttpRangeReader")
    void plainHttpsUrl() throws IOException {
        // httpbin.io provides a /range/1024 endpoint that streams n bytes and allows specifying a Range header to
        // select a subset of the data.
        String url = "https://httpbin.io/range/1024";
        testFindBestProvider(URI.create(url), HttpStorageProvider.class);

        StorageConfig config = new StorageConfig(url);
        RangeReader reader = testCreate(config);
        assertThat(reader.size()).hasValue(1024);
    }

    @Test
    @DisplayName("GCS https:// URL uses GoogleCloudStorageRangeReader")
    void gcsHttpsURL() throws IOException {
        String gcsURL =
                "https://storage.googleapis.com/gcp-public-data-landsat/LC08/01/001/003/LC08_L1GT_001003_20140812_20170420_01_T2/LC08_L1GT_001003_20140812_20170420_01_T2_B3.TIF";

        testFindBestProvider(URI.create(gcsURL), GoogleCloudStorageProvider.class);

        StorageConfig config = new StorageConfig(gcsURL);
        testCreate(config);
    }

    @Test
    @DisplayName("GCS gs:// URL uses GoogleCloudStorageRangeReader")
    void gcsGsUrl() throws IOException {
        String gcsURL =
                "gs://gcp-public-data-landsat/LC08/01/001/003/LC08_L1GT_001003_20140812_20170420_01_T2/LC08_L1GT_001003_20140812_20170420_01_T2_B3.TIF";

        testFindBestProvider(URI.create(gcsURL), GoogleCloudStorageProvider.class);

        StorageConfig config = new StorageConfig(gcsURL);
        testCreate(config);
    }

    @Test
    @DisplayName("Azure https:// URL uses AzureBlobRangeReader")
    @org.junit.jupiter.api.Disabled("AzureBlobStorage routes through BlobServiceClient which requires a credential; "
            + "anonymous public-blob reads need a separate code path. Tracked separately.")
    void azureHttps() throws IOException {
        String onlineURI =
                "https://overturemapswestus2.blob.core.windows.net/release/2026-04-15.0/theme=base/type=land/part-00000-76938bf2-d731-5eff-a9c6-644d601ef99e-c000.zstd.parquet";
        testAzureBlob(onlineURI, null);
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style (legacy format) URL uses S3RangeReaderProvider")
    void s3HttpsUrlVirtualHostedStyle() throws IOException {
        // the legacy global endpoint has no region in the URL; the provider requires it from configuration
        testS3("https://" + S3_BUCKET + ".s3.amazonaws.com/" + S3_KEY, S3_REGION);
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style with region URL uses S3RangeReaderProvider")
    void s3HttpsUrlVirtualHostedWithRegion() throws IOException {
        testS3("https://" + S3_BUCKET + ".s3." + S3_REGION + ".amazonaws.com/" + S3_KEY);
    }

    @Test
    @DisplayName("S3 https:// path-style URL with region uses S3RangeReaderProvider")
    void s3PathStyleUrlWithRegion() throws IOException {
        testS3("https://s3." + S3_REGION + ".amazonaws.com/" + S3_BUCKET + "/" + S3_KEY);
    }

    @Test
    @DisplayName("S3 s3:// URL uses S3RangeReaderProvider")
    void s3Url() throws IOException {
        // s3:// URIs have no region; the provider requires it from configuration or the AWS environment
        testS3("s3://" + S3_BUCKET + "/" + S3_KEY, S3_REGION);
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style (legacy format) URL with forced HTTP provider uses HttpRangeReader")
    void s3LegacyVirtualHostedStyleUrlWithForcedHttpProvider() throws IOException {
        testForceHttp("https://" + S3_BUCKET + ".s3.amazonaws.com/" + S3_KEY);
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style URL with forced HTTP provider uses HttpRangeReader")
    void s3VirtualHostedStyleUrlWithForcedHttpProvider() throws IOException {
        testForceHttp("https://" + S3_BUCKET + ".s3." + S3_REGION + ".amazonaws.com/" + S3_KEY);
    }

    @Test
    @DisplayName("S3 https:// path-style URL with forced HTTP provider uses HttpRangeReader")
    void s3PathStyleUrlWithForcedHttpProvider() throws IOException {
        testForceHttp("https://s3." + S3_REGION + ".amazonaws.com/" + S3_BUCKET + "/" + S3_KEY);
    }

    /**
     * A custom-domain HTTPS URL with a single-segment path: the S3 provider cannot parse a bucket and object key out of
     * it, hence {@link StorageFactory} should use {@link HttpStorageProvider}.
     */
    @Test
    @DisplayName("Custom-domain HTTPS URL that isn't a parseable S3 URL falls back to HttpRangeReader")
    void customDomainUrlNotParseableAsS3FallsBackToHttp() throws IOException {
        URI uri = URI.create("https://pmtiles.io/protomaps(vector)ODbL_firenze.pmtiles");
        testFindBestProvider(uri, HttpStorageProvider.class);
        testCreate(new StorageConfig(uri));
    }

    private void testForceHttp(String url) throws IOException {
        StorageConfig config = new StorageConfig(url).providerId(HttpStorageProvider.ID);
        testFindBestProvider(config, HttpStorageProvider.class);
        testCreate(config);
    }
}
