/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 */
package io.tileverse.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.geotools.api.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;

final class GeoParquetDatastoreCloudITSupport {

    static final String FIXTURE_RESOURCE = "/geoparquet/sample-geoparquet.parquet";
    static final String OBJECT_NAME = "sample-geoparquet.parquet";
    static final String TYPE_NAME = "sample-geoparquet";
    static final GeoParquetFileDataStoreFactory FACTORY = new GeoParquetFileDataStoreFactory();

    private GeoParquetDatastoreCloudITSupport() {}

    static Path extractFixture(Class<?> testClass, Path tempDir) {
        try (InputStream in = testClass.getResourceAsStream(FIXTURE_RESOURCE)) {
            assertThat(in).as("Missing fixture resource %s", FIXTURE_RESOURCE).isNotNull();
            Path fixture = tempDir.resolve(OBJECT_NAME);
            Files.copy(in, fixture, StandardCopyOption.REPLACE_EXISTING);
            return fixture;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void assertReadsSampleGeoParquet(Map<String, Object> params) throws IOException {
        DataStore dataStore = FACTORY.createDataStore(params);
        try {
            assertThat(dataStore.getTypeNames()).containsExactly(TYPE_NAME);
            SimpleFeatureCollection features =
                    dataStore.getFeatureSource(TYPE_NAME).getFeatures();
            assertThat(features.size()).isEqualTo(3);
        } finally {
            dataStore.dispose();
        }
    }

    static void assertReadsSampleGeoParquet(URL url, Map<String, Object> extraParams) throws IOException {
        java.util.LinkedHashMap<String, Object> params = new java.util.LinkedHashMap<>();
        params.put("url", url);
        params.putAll(extraParams);
        assertReadsSampleGeoParquet(params);
    }
}
