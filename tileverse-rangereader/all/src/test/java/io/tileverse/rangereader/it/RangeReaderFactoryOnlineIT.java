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
package io.tileverse.rangereader.it;

import static io.tileverse.rangereader.it.RangeReaderFactoryIT.testAzureBlob;
import static io.tileverse.rangereader.it.RangeReaderFactoryIT.testCreate;
import static io.tileverse.rangereader.it.RangeReaderFactoryIT.testFindBestProvider;
import static io.tileverse.rangereader.it.RangeReaderFactoryIT.testS3;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import io.tileverse.rangereader.gcs.GoogleCloudStorageRangeReader;
import io.tileverse.rangereader.gcs.GoogleCloudStorageRangeReaderProvider;
import io.tileverse.rangereader.http.HttpRangeReader;
import io.tileverse.rangereader.http.HttpRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for {@link RangeReaderFactory} resolving URLs to the correct {@link RangeReaderProvider}
 * implementation (e.g. several providers accept http(s) URLs)
 */
@Testcontainers(disabledWithoutDocker = true)
class RangeReaderFactoryOnlineIT {

    @Test
    @DisplayName("HTTPS URL that doesn't match a cloud provider uses HttpRangeReader")
    void plainHttpsUrl() throws IOException {
        // httpbin.io provides a /range/1024 endpoint that streams n bytes and allows specifying a Range header to
        // select a subset of the data.
        String url = "https://httpbin.io/range/1024";
        testFindBestProvider(URI.create(url), HttpRangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(URI.create(url));
        RangeReader reader = testCreate(config, HttpRangeReader.class);
        assertThat(reader.size()).hasValue(1024);
    }

    @Test
    @DisplayName("GCS https:// URL uses GoogleCloudStorageRangeReader")
    void gcsHttpsURL() throws IOException {
        String gcsURL =
                "https://storage.googleapis.com/gcp-public-data-landsat/LC08/01/001/003/LC08_L1GT_001003_20140812_20170420_01_T2/LC08_L1GT_001003_20140812_20170420_01_T2_B3.TIF";

        testFindBestProvider(URI.create(gcsURL), GoogleCloudStorageRangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(URI.create(gcsURL));
        testCreate(config, GoogleCloudStorageRangeReader.class);
    }

    @Test
    @DisplayName("GCS gs:// URL uses GoogleCloudStorageRangeReader")
    void gcsGsUrl() throws IOException {
        String gcsURL =
                "gs://gcp-public-data-landsat/LC08/01/001/003/LC08_L1GT_001003_20140812_20170420_01_T2/LC08_L1GT_001003_20140812_20170420_01_T2_B3.TIF";

        testFindBestProvider(URI.create(gcsURL), GoogleCloudStorageRangeReaderProvider.class);

        RangeReaderConfig config = new RangeReaderConfig().uri(URI.create(gcsURL));
        testCreate(config, GoogleCloudStorageRangeReader.class);
    }

    @Test
    @DisplayName("Azure https:// URL uses AzureBlobRangeReader")
    void azureHttps() throws IOException {
        String onlineURI =
                "https://overturemapswestus2.blob.core.windows.net/release/2026-04-15.0/theme=base/type=land/part-00000-76938bf2-d731-5eff-a9c6-644d601ef99e-c000.zstd.parquet";
        testAzureBlob(onlineURI, null);
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style (legacy format) URL uses S3RangeReaderProvider")
    void s3HttpsUrlVirtualHostedStyle() throws IOException {
        testS3("https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2025-08-20/base.pmtiles");
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style with region URL uses S3RangeReaderProvider")
    void s3HttpsUrlVirtualHostedWithRegion() throws IOException {
        testS3("https://overturemaps-tiles-us-west-2-beta.s3.us-west-2.amazonaws.com/2025-08-20/base.pmtiles");
    }

    @Test
    @DisplayName("S3 https:// path-style URL with region uses S3RangeReaderProvider")
    void s3PathStyleUrlWithRegion() throws IOException {
        testS3("https://s3.us-west-2.amazonaws.com/overturemaps-tiles-us-west-2-beta/2025-08-20/base.pmtiles");
    }

    @Test
    @DisplayName("S3 s3:// URL uses S3RangeReaderProvider")
    void s3Url() throws IOException {
        testS3("s3://overturemaps-tiles-us-west-2-beta/2025-08-20/base.pmtiles");
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style (legacy format) URL with forced HTTP provider uses HttpRangeReader")
    void s3LegacyVirtualHostedStyleUrlWithForcedHttpProvider() throws IOException {
        testForceHttp("https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2025-08-20/base.pmtiles");
    }

    @Test
    @DisplayName("S3 https:// virtual hosted-style URL with forced HTTP provider uses HttpRangeReader")
    void s3VirtualHostedStyleUrlWithForcedHttpProvider() throws IOException {
        testForceHttp("https://overturemaps-tiles-us-west-2-beta.s3.us-west-2.amazonaws.com/2025-08-20/base.pmtiles");
    }

    @Test
    @DisplayName("S3 https:// path-style URL with forced HTTP provider uses HttpRangeReader")
    void s3PathStyleUrlWithForcedHttpProvider() throws IOException {
        testForceHttp("https://s3.us-west-2.amazonaws.com/overturemaps-tiles-us-west-2-beta/2025-08-20/base.pmtiles");
    }

    /**
     * This is an S3-hosted URL with a custom virtual hosted style, not a valid S3 URL, hence {@link RangeReaderFactory}
     * should use {@link HttpRangeReaderProvider}
     */
    @Test
    @DisplayName("HTTPS URL on S3 with invalid path-style falls back to HttpRangeReader")
    void s3OnlineCustomVirtualHostedStyleFallsBackToHttp() throws IOException {
        URI uri = URI.create("https://demo-bucket.protomaps.com/v4.pmtiles");
        testFindBestProvider(uri, HttpRangeReaderProvider.class);
        testCreate(new RangeReaderConfig().uri(uri), HttpRangeReader.class);
    }

    private void testForceHttp(String url) throws IOException {
        RangeReaderConfig config = new RangeReaderConfig().uri(url).providerId(HttpRangeReaderProvider.ID);
        testFindBestProvider(config, HttpRangeReaderProvider.class);
        testCreate(config, HttpRangeReader.class);
    }
}
