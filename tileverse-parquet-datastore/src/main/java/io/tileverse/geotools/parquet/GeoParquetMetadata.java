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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.locationtech.jts.geom.Envelope;

/**
 * GeoParquet metadata model parsed from the {@code "geo"} key-value metadata entry in Parquet
 * files.
 *
 * <p>Supports polymorphic deserialization based on the {@code version} field, falling back to
 * version 1.1.0 for unknown or missing versions.
 *
 * <p>Adapted from the GeoTools GeoParquet module (LGPL 2.1, authored by the same contributor).
 *
 * @see <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/metadata.md">
 *     GeoParquet Metadata Specification</a>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "version",
        defaultImpl = GeoParquetMetadata.V1_1_0.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = GeoParquetMetadata.V1_1_0.class, name = "1.1.0"),
    @JsonSubTypes.Type(value = GeoParquetMetadata.V1_2_0Dev.class, name = "1.2.0-dev")
})
public abstract class GeoParquetMetadata {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @JsonProperty(value = "version", required = true)
    private String version;

    @JsonProperty(value = "primary_column", required = true)
    private String primaryColumn;

    @JsonProperty(value = "columns", required = true)
    private Map<String, GeometryColumnMetadata> columns;

    public static GeoParquetMetadata readValue(String geo) throws IOException {
        return MAPPER.readValue(geo, GeoParquetMetadata.class);
    }

    public Envelope bounds() {
        return Optional.ofNullable(columns)
                .map(cols -> cols.get(primaryColumn))
                .map(GeometryColumnMetadata::bounds)
                .orElseGet(Envelope::new);
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getPrimaryColumn() {
        return primaryColumn;
    }

    public void setPrimaryColumn(String primaryColumn) {
        this.primaryColumn = primaryColumn;
    }

    public Map<String, GeometryColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, GeometryColumnMetadata> columns) {
        this.columns = columns;
    }

    public Optional<GeometryColumnMetadata> getColumn(String column) {
        return Optional.ofNullable(columns).map(cols -> cols.get(column));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class V1_1_0 extends GeoParquetMetadata {
        public V1_1_0() {
            setVersion("1.1.0");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class V1_2_0Dev extends GeoParquetMetadata {
        public V1_2_0Dev() {
            setVersion("1.2.0-dev");
        }
    }
}
