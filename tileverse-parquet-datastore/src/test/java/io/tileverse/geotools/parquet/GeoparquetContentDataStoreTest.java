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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GeoparquetContentDataStoreTest {

    private static final String FIXTURE_RESOURCE = "/geoparquet/sample-geoparquet.parquet";

    @TempDir
    Path tmpdir;

    @Test
    void parseGeoParquetMetadata_handlesMissingBlankInvalidAndValidValues() {
        assertThat(GeoparquetContentDataStore.parseGeoParquetMetadata(Map.of())).isNull();
        assertThat(GeoparquetContentDataStore.parseGeoParquetMetadata(Map.of("geo", "   ")))
                .isNull();
        assertThat(GeoparquetContentDataStore.parseGeoParquetMetadata(Map.of("geo", "{not-json")))
                .isNull();

        GeoParquetMetadata metadata = GeoparquetContentDataStore.parseGeoParquetMetadata(
                Map.of(
                        "geo",
                        """
                {
                  "version": "1.1.0",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Point"]
                    }
                  }
                }
                """));

        assertThat(metadata).isNotNull();
        assertThat(metadata.getPrimaryColumn()).isEqualTo("geometry");
    }

    @Test
    void extractGeometryColumnNames_handlesNullEmptyAndPresentMetadata() {
        assertThat(GeoparquetContentDataStore.extractGeometryColumnNames(null)).isEmpty();

        GeoParquetMetadata.V1_1_0 empty = new GeoParquetMetadata.V1_1_0();
        empty.setColumns(Map.of());
        assertThat(GeoparquetContentDataStore.extractGeometryColumnNames(empty)).isEmpty();

        GeoParquetMetadata.V1_1_0 metadata = new GeoParquetMetadata.V1_1_0();
        metadata.setColumns(
                Map.of("geometry", new GeometryColumnMetadata(), "geometry2", new GeometryColumnMetadata()));
        assertThat(GeoparquetContentDataStore.extractGeometryColumnNames(metadata))
                .containsExactlyInAnyOrder("geometry", "geometry2");
    }

    @Test
    void open_acceptsNullRangeReaderConfigAndBuildsStore() throws Exception {
        GeoparquetContentDataStore store =
                GeoparquetContentDataStore.open(fixtureUrl(), (Properties) null, TileverseParquetRecordSource::new);
        try {
            assertThat(store.getSchema().getTypeName()).isEqualTo("sample-geoparquet");
            assertThat(store.getGeoParquetMetadata()).isNotNull();
            assertThat(store.getWkbGeometryColumns()).containsExactly("geometry");
        } finally {
            store.dispose();
        }
    }

    @Test
    void open_closesReaderAndPropagatesFactoryFailures() throws Exception {
        assertThatThrownBy(() -> GeoparquetContentDataStore.open(fixtureUrl(), new Properties(), rangeReader -> {
                    throw new IOException("boom");
                }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("boom");

        assertThatThrownBy(() -> GeoparquetContentDataStore.open(fixtureUrl(), new Properties(), rangeReader -> {
                    throw new IllegalStateException("boom");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("boom");
    }

    private URL fixtureUrl() {
        Path path = extractToTempFile(FIXTURE_RESOURCE);
        try {
            return path.toUri().toURL();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path extractToTempFile(String resource) {
        Path target = tmpdir.resolve(Path.of(resource).getFileName().toString());
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("Missing fixture resource " + resource);
            }
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
