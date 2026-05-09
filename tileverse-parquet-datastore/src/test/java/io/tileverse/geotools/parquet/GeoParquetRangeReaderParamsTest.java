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
package io.tileverse.geotools.parquet;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.Parameter;
import org.junit.jupiter.api.Test;

class GeoParquetRangeReaderParamsTest {

    @Test
    void getParameters_returnsAllProviderBackedParamsInOrder() {
        Param[] params = GeoParquetRangeReaderParams.getParameters();

        assertThat(params).hasSize(GeoParquetRangeReaderParams.PARAMETERS.size());
        assertThat(List.of(params))
                .extracting(p -> p.key)
                .containsExactlyElementsOf(GeoParquetRangeReaderParams.PARAMETERS.stream()
                        .map(p -> p.key)
                        .toList());
    }

    @Test
    void factory_keepsUrlFirstAndAppendsGeoParquetRangeReaderParams() {
        Param[] params = new GeoParquetFileDataStoreFactory().getParametersInfo();
        List<String> expectedKeys = new java.util.ArrayList<>();
        expectedKeys.add("url");
        expectedKeys.addAll(List.of(GeoParquetRangeReaderParams.getParameters()).stream()
                .map(p -> p.key)
                .toList());

        assertThat(params).extracting(p -> p.key).containsExactlyElementsOf(expectedKeys);
    }

    @Test
    void providerParams_preserveRepresentativeMetadata() {
        Param httpPassword = GeoParquetRangeReaderParams.HTTP_AUTH_PASSWORD;
        Param providerId = GeoParquetRangeReaderParams.RANGEREADER_PROVIDER_ID;
        Param gcsDefaultCredentials = GeoParquetRangeReaderParams.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS;

        assertThat(httpPassword.type).isEqualTo(String.class);
        assertThat(httpPassword.metadata).containsEntry(Parameter.IS_PASSWORD, Boolean.TRUE);
        assertThat(httpPassword.metadata).containsEntry(Parameter.LEVEL, "http");

        assertThat(providerId.type).isEqualTo(String.class);
        assertThat(providerId.metadata).containsKey(Parameter.OPTIONS);
        assertThat((Object[]) providerId.metadata.get(Parameter.OPTIONS))
                .contains("file", "http", "s3", "azure", "gcs");

        assertThat(gcsDefaultCredentials.type).isEqualTo(Boolean.class);
        assertThat(gcsDefaultCredentials.sample).isEqualTo(false);
        assertThat(gcsDefaultCredentials.metadata).containsEntry(Parameter.LEVEL, "gcs");
    }

    @Test
    void toProperties_convertsSupportedParametersToStringValues() throws Exception {
        URL url = new URL("file:/tmp/sample.parquet");
        Map<String, Object> params = Map.ofEntries(
                entry("url", url),
                entry("unrelated", "ignored"),
                entry("storage.provider", "s3"),
                entry("storage.caching.enabled", true),
                entry("storage.caching.blockaligned", false),
                entry("storage.caching.blocksize", 8192),
                entry("storage.http.timeout-millis", 1500),
                entry("storage.http.username", "alice"),
                entry("storage.http.password", "secret"),
                entry("storage.s3.region", "us-east-1"),
                entry("storage.s3.force-path-style", true),
                entry("storage.gcs.project-id", "test-project"),
                entry("storage.gcs.default-credentials-chain", false));

        Properties properties = GeoParquetRangeReaderParams.toProperties(params);

        assertThat(properties)
                .containsEntry("storage.provider", "s3")
                .containsEntry("storage.caching.enabled", "true")
                .containsEntry("storage.caching.blockaligned", "false")
                .containsEntry("storage.caching.blocksize", "8192")
                .containsEntry("storage.http.timeout-millis", "1500")
                .containsEntry("storage.http.username", "alice")
                .containsEntry("storage.http.password", "secret")
                .containsEntry("storage.s3.region", "us-east-1")
                .containsEntry("storage.s3.force-path-style", "true")
                .containsEntry("storage.gcs.project-id", "test-project")
                .containsEntry("storage.gcs.default-credentials-chain", "false");
        assertThat(properties).doesNotContainKeys("url", "unrelated");
    }
}
