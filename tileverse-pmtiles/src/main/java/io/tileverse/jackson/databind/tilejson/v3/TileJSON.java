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
import java.util.List;

/**
 * Represents a complete TileJSON v3.0.0 specification-compliant metadata object.
 *
 * <p>TileJSON is a format for describing map tilesets. It describes tilesets' metadata,
 * including their bounds, available zoom levels, and vector layer information.
 *
 * <p>This implementation strictly follows the TileJSON v3.0.0 schema specification,
 * including all required and optional fields with proper validation.
 *
 * @since 1.0
 * @see <a href="https://github.com/mapbox/tilejson-spec/tree/master/3.0.0">TileJSON v3.0.0 Specification</a>
 * @see <a href="https://github.com/mapbox/tilejson-spec/blob/22f5f91e643e8980ef2656674bef84c2869fbe76/3.0.0/schema.json">TileJSON v3.0.0 Schema</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TileJSON(

        // Required fields per TileJSON v3.0.0 specification

        /**
         * The TileJSON specification version number.
         * Must be a semantic version string (e.g., "3.0.0").
         * Required by TileJSON v3.0.0 specification.
         */
        @JsonProperty("tilejson") String tilejson,

        /**
         * An array of tile endpoints. Each endpoint is a string URL template.
         * Must contain at least one tile URL.
         * Required by TileJSON v3.0.0 specification.
         */
        @JsonProperty("tiles") List<String> tiles,

        /**
         * Vector layer definitions for vector tilesets.
         * Each layer describes the data fields and zoom range available.
         * Required by TileJSON v3.0.0 specification.
         */
        @JsonProperty("vector_layers") List<VectorLayer> vectorLayers,

        // Optional fields per TileJSON v3.0.0 specification

        /**
         * Attribution text to be displayed when the tileset is shown.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("attribution") String attribution,

        /**
         * The extent of the tileset in WGS84 coordinates: [west, south, east, north].
         * Must be an array of exactly 4 numbers if specified.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("bounds") List<Double> bounds,

        /**
         * The default latitude and longitude of the tileset center.
         * Must be an array of 2 or 3 numbers [longitude, latitude] or [longitude, latitude, zoom].
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("center") List<Double> center,

        /**
         * A text description of the tileset.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("description") String description,

        /**
         * The zoom level at which to fill in missing tiles.
         * Must be between 0 and 30 if specified.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("fillzoom") Integer fillzoom,

        /**
         * An array of interactivity grid endpoints.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("grids") List<String> grids,

        /**
         * Legend text to be displayed with the tileset.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("legend") String legend,

        /**
         * The maximum zoom level supported by the tileset.
         * Must be between 0 and 30 if specified.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("maxzoom") Integer maxzoom,

        /**
         * The minimum zoom level supported by the tileset.
         * Must be between 0 and 30 if specified.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("minzoom") Integer minzoom,

        /**
         * A name describing the tileset.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("name") String name,

        /**
         * The tiling scheme. Either "xyz" (default) or "tms".
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("scheme") String scheme,

        /**
         * A mustache template to be used to format interaction data.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("template") String template,

        /**
         * The version of the tileset.
         * Should be a semantic version string.
         * Optional in TileJSON v3.0.0 specification.
         */
        @JsonProperty("version") String version) {

    /**
     * Creates a new TileJSON with all fields.
     */
    public TileJSON {}

    /**
     * Creates a minimal TileJSON with required fields only.
     *
     * @param tilejson the TileJSON specification version (required)
     * @param tiles the tile URL templates (required, must not be empty)
     * @param vectorLayers the vector layer definitions (required)
     * @return a new TileJSON instance
     * @throws IllegalArgumentException if any required field is invalid
     */
    public static TileJSON of(String tilejson, List<String> tiles, List<VectorLayer> vectorLayers) {
        validateRequired(tilejson, tiles, vectorLayers);
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates a TileJSON with common metadata fields.
     *
     * @param tilejson the TileJSON specification version (required)
     * @param tiles the tile URL templates (required, must not be empty)
     * @param vectorLayers the vector layer definitions (required)
     * @param name the name of the tileset (optional)
     * @param description the description of the tileset (optional)
     * @param attribution the attribution text (optional)
     * @return a new TileJSON instance
     * @throws IllegalArgumentException if any required field is invalid
     */
    public static TileJSON of(
            String tilejson,
            List<String> tiles,
            List<VectorLayer> vectorLayers,
            String name,
            String description,
            String attribution) {
        validateRequired(tilejson, tiles, vectorLayers);
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                null,
                null,
                description,
                null,
                null,
                null,
                null,
                null,
                name,
                null,
                null,
                null);
    }

    /**
     * Returns a new TileJSON with the specified attribution.
     *
     * @param attribution the new attribution
     * @return a new TileJSON instance
     */
    public TileJSON withAttribution(String attribution) {
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified bounds.
     *
     * @param west the western longitude
     * @param south the southern latitude
     * @param east the eastern longitude
     * @param north the northern latitude
     * @return a new TileJSON instance
     * @throws IllegalArgumentException if bounds are invalid
     */
    public TileJSON withBounds(double west, double south, double east, double north) {
        if (west >= east) {
            throw new IllegalArgumentException("West bound must be less than east bound");
        }
        if (south >= north) {
            throw new IllegalArgumentException("South bound must be less than north bound");
        }
        if (west < -180 || east > 180 || south < -90 || north > 90) {
            throw new IllegalArgumentException("Bounds must be within valid longitude/latitude ranges");
        }
        List<Double> boundsCoords = List.of(west, south, east, north);
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                boundsCoords,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified center coordinates.
     *
     * @param longitude the longitude of the center
     * @param latitude the latitude of the center
     * @param zoom the zoom level for the center (optional)
     * @return a new TileJSON instance
     * @throws IllegalArgumentException if coordinates are invalid
     */
    public TileJSON withCenter(double longitude, double latitude, Double zoom) {
        if (longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("Longitude must be between -180 and 180");
        }
        if (latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Latitude must be between -90 and 90");
        }
        if (zoom != null && (zoom < 0 || zoom > 30)) {
            throw new IllegalArgumentException("Zoom level must be between 0 and 30");
        }
        List<Double> centerCoords = zoom != null ? List.of(longitude, latitude, zoom) : List.of(longitude, latitude);
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                centerCoords,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified description.
     *
     * @param description the new description
     * @return a new TileJSON instance
     */
    public TileJSON withDescription(String description) {
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified zoom range.
     *
     * @param minzoom the minimum zoom level (0-30)
     * @param maxzoom the maximum zoom level (0-30)
     * @return a new TileJSON instance
     * @throws IllegalArgumentException if zoom levels are invalid
     */
    public TileJSON withZoomRange(Integer minzoom, Integer maxzoom) {
        validateZoomLevel(minzoom, "minzoom");
        validateZoomLevel(maxzoom, "maxzoom");
        if (minzoom != null && maxzoom != null && minzoom > maxzoom) {
            throw new IllegalArgumentException("minzoom cannot be greater than maxzoom");
        }
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified name.
     *
     * @param name the new name
     * @return a new TileJSON instance
     */
    public TileJSON withName(String name) {
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified version.
     *
     * @param version the new version
     * @return a new TileJSON instance
     */
    public TileJSON withVersion(String version) {
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    /**
     * Returns a new TileJSON with the specified vector layers.
     *
     * @param vectorLayers the vector layer definitions
     * @return a new TileJSON instance
     * @throws IllegalArgumentException if vectorLayers is null
     */
    public TileJSON withVectorLayers(List<VectorLayer> vectorLayers) {
        if (vectorLayers == null) {
            throw new IllegalArgumentException("Vector layers list is required and cannot be null");
        }
        return new TileJSON(
                tilejson,
                tiles,
                vectorLayers,
                attribution,
                bounds,
                center,
                description,
                fillzoom,
                grids,
                legend,
                maxzoom,
                minzoom,
                name,
                scheme,
                template,
                version);
    }

    private static void validateRequired(String tilejson, List<String> tiles, List<VectorLayer> vectorLayers) {
        if (tilejson == null || tilejson.isBlank()) {
            throw new IllegalArgumentException("TileJSON version is required and cannot be null or blank");
        }
        if (tiles == null || tiles.isEmpty()) {
            throw new IllegalArgumentException("Tiles array is required and cannot be null or empty");
        }
        if (vectorLayers == null) {
            throw new IllegalArgumentException("Vector layers list is required and cannot be null");
        }
        // Validate each tile URL is not null or blank
        for (int i = 0; i < tiles.size(); i++) {
            String tile = tiles.get(i);
            if (tile == null || tile.isBlank()) {
                throw new IllegalArgumentException("Tile URL at index " + i + " cannot be null or blank");
            }
        }
    }

    private static void validateZoomLevel(Integer zoom, String fieldName) {
        if (zoom != null && (zoom < 0 || zoom > 30)) {
            throw new IllegalArgumentException(fieldName + " must be between 0 and 30, got: " + zoom);
        }
    }
}
