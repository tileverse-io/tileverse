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
package io.tileverse.jackson.databind.tilejson.v3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Represents a vector layer definition within TileJSON v3.0.0 metadata.
 * Vector layers describe the data fields and zoom range for each layer in a vector tileset.
 *
 * <p>This follows the TileJSON v3.0.0 vector layer specification where each vector layer
 * must have an "id" and "fields" property, with optional "description", "minzoom", and "maxzoom".
 *
 * @since 1.0
 * @see <a href="https://github.com/mapbox/tilejson-spec/tree/master/3.0.0#33-vector_layers">TileJSON Vector Layers</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record VectorLayer(

        /**
         * The layer identifier used in the vector tiles.
         * This corresponds to the layer name in the Mapbox Vector Tile format.
         * Required by TileJSON v3.0.0 specification.
         */
        @JsonProperty("id") String id,

        /**
         * A mapping of field names to their data types.
         * Common types include "String", "Number", "Boolean".
         * Field names correspond to properties in the vector tile features.
         * Required by TileJSON v3.0.0 specification.
         */
        @JsonProperty("fields") Map<String, String> fields,

        /**
         * A human-readable description of the layer.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("description") String description,

        /**
         * The minimum zoom level at which this layer appears.
         * Must be between 0 and 30 if specified.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("minzoom") Integer minZoom,

        /**
         * The maximum zoom level at which this layer appears.
         * Must be between 0 and 30 if specified.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("maxzoom") Integer maxZoom) {

    /**
     * Creates a new VectorLayer with all fields.
     */
    public VectorLayer {}

    /**
     * Creates a minimal VectorLayer with required fields only.
     *
     * @param id the layer identifier (required)
     * @param fields the field name to type mapping (required)
     * @return a new VectorLayer instance
     * @throws IllegalArgumentException if id is null or blank, or fields is null
     */
    public static VectorLayer of(String id, Map<String, String> fields) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Layer id is required and cannot be null or blank");
        }
        if (fields == null) {
            throw new IllegalArgumentException("Fields map is required and cannot be null");
        }
        return new VectorLayer(id, fields, null, null, null);
    }

    /**
     * Creates a VectorLayer with basic information.
     *
     * @param id the layer identifier (required)
     * @param fields the field name to type mapping (required)
     * @param description the layer description (optional)
     * @return a new VectorLayer instance
     * @throws IllegalArgumentException if id is null or blank, or fields is null
     */
    public static VectorLayer of(String id, Map<String, String> fields, String description) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Layer id is required and cannot be null or blank");
        }
        if (fields == null) {
            throw new IllegalArgumentException("Fields map is required and cannot be null");
        }
        return new VectorLayer(id, fields, description, null, null);
    }

    /**
     * Creates a VectorLayer with zoom range information.
     *
     * @param id the layer identifier (required)
     * @param fields the field name to type mapping (required)
     * @param description the layer description (optional)
     * @param minZoom the minimum zoom level (optional, must be 0-30)
     * @param maxZoom the maximum zoom level (optional, must be 0-30)
     * @return a new VectorLayer instance
     * @throws IllegalArgumentException if id is null or blank, fields is null, or zoom levels are invalid
     */
    public static VectorLayer of(
            String id, Map<String, String> fields, String description, Integer minZoom, Integer maxZoom) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Layer id is required and cannot be null or blank");
        }
        if (fields == null) {
            throw new IllegalArgumentException("Fields map is required and cannot be null");
        }
        validateZoomLevel(minZoom, "minZoom");
        validateZoomLevel(maxZoom, "maxZoom");
        if (minZoom != null && maxZoom != null && minZoom > maxZoom) {
            throw new IllegalArgumentException("minZoom cannot be greater than maxZoom");
        }
        return new VectorLayer(id, fields, description, minZoom, maxZoom);
    }

    /**
     * Returns a new VectorLayer with the specified description.
     *
     * @param description the new description
     * @return a new VectorLayer instance
     */
    public VectorLayer withDescription(String description) {
        return new VectorLayer(id, fields, description, minZoom, maxZoom);
    }

    /**
     * Returns a new VectorLayer with the specified zoom range.
     *
     * @param minZoom the minimum zoom level (0-30)
     * @param maxZoom the maximum zoom level (0-30)
     * @return a new VectorLayer instance
     * @throws IllegalArgumentException if zoom levels are invalid
     */
    public VectorLayer withZoomRange(Integer minZoom, Integer maxZoom) {
        validateZoomLevel(minZoom, "minZoom");
        validateZoomLevel(maxZoom, "maxZoom");
        if (minZoom != null && maxZoom != null && minZoom > maxZoom) {
            throw new IllegalArgumentException("minZoom cannot be greater than maxZoom");
        }
        return new VectorLayer(id, fields, description, minZoom, maxZoom);
    }

    /**
     * Returns a new VectorLayer with the specified fields.
     *
     * @param fields the field name to type mapping
     * @return a new VectorLayer instance
     * @throws IllegalArgumentException if fields is null
     */
    public VectorLayer withFields(Map<String, String> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("Fields map is required and cannot be null");
        }
        return new VectorLayer(id, fields, description, minZoom, maxZoom);
    }

    private static void validateZoomLevel(Integer zoom, String fieldName) {
        if (zoom != null && (zoom < 0 || zoom > 30)) {
            throw new IllegalArgumentException(fieldName + " must be between 0 and 30, got: " + zoom);
        }
    }
}
