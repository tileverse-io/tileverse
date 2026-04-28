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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

class GeoParquetMetadataTest {

    @Test
    void readValue_supportsKnownAndUnknownVersions() throws Exception {
        GeoParquetMetadata known = GeoParquetMetadata.readValue("""
                {
                  "version": "1.2.0-dev",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Point"]
                    }
                  }
                }
                """);
        assertThat(known).isInstanceOf(GeoParquetMetadata.V1_2_0Dev.class);
        assertThat(known.getVersion()).isEqualTo("1.2.0-dev");

        GeoParquetMetadata unknown = GeoParquetMetadata.readValue("""
                {
                  "version": "9.9.9",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Point"]
                    }
                  }
                }
                """);
        assertThat(unknown).isInstanceOf(GeoParquetMetadata.V1_1_0.class);
        assertThat(unknown.getVersion()).isEqualTo("1.1.0");
    }

    @Test
    void readValue_rejectsInvalidJson() {
        assertThatThrownBy(() -> GeoParquetMetadata.readValue("{not-json")).isInstanceOf(Exception.class);
    }

    @Test
    void bounds_andGetColumnHandleMissingPrimaryColumnAndMissingColumns() {
        GeoParquetMetadata.V1_1_0 metadata = new GeoParquetMetadata.V1_1_0();

        assertThat(metadata.bounds()).isEqualTo(new Envelope());
        assertThat(metadata.getColumn("geometry")).isEmpty();

        metadata.setPrimaryColumn("geometry");
        metadata.setColumns(Map.of());
        assertThat(metadata.bounds()).isEqualTo(new Envelope());
        assertThat(metadata.getColumn("geometry")).isEmpty();
    }

    @Test
    void bounds_usesPrimaryColumnMetadataEnvelope() {
        GeometryColumnMetadata geometry = new GeometryColumnMetadata();
        geometry.setEncoding("WKB");
        geometry.setGeometryTypes(List.of("Point"));
        geometry.setBbox(List.of(-58.0, -34.0, -57.5, -33.5));

        GeoParquetMetadata.V1_1_0 metadata = new GeoParquetMetadata.V1_1_0();
        metadata.setPrimaryColumn("geometry");
        metadata.setColumns(Map.of("geometry", geometry, "secondary", new GeometryColumnMetadata()));

        assertThat(metadata.bounds()).isEqualTo(new Envelope(-58.0, -57.5, -34.0, -33.5));
        assertThat(metadata.getColumn("geometry")).containsSame(geometry);
        assertThat(metadata.getColumn("missing")).isEmpty();
    }
}
