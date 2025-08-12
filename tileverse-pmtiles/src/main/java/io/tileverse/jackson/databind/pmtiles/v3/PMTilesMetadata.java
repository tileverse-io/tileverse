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
package io.tileverse.jackson.databind.pmtiles.v3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.tileverse.jackson.databind.tilejson.v3.TilesetType;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import java.util.List;
import java.util.Map;

/**
 * Represents PMTiles v3 metadata as defined in the PMTiles specification.
 * PMTiles metadata is stored as JSON and contains information about the tileset.
 *
 * <p>According to the PMTiles v3 specification, metadata MUST contain a valid JSON object
 * which MAY include additional metadata related to the tileset. For MVT Vector Tiles,
 * it MUST contain a "vector_layers" key as described in the TileJSON 3.0 specification.
 *
 * <p>The PMTiles specification defines specific optional keys: name, description, attribution,
 * type, and version. Additional properties are allowed and ignored during deserialization
 * to ensure forward compatibility.
 *
 * @since 1.0
 * @see <a href="https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md#5-json-metadata">PMTiles v3 Metadata Specification</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PMTilesMetadata(

        /**
         * A name describing the tileset.
         * Optional field as defined in PMTiles v3 specification.
         */
        @JsonProperty("name") String name,

        /**
         * A text description of the tileset.
         * Optional field as defined in PMTiles v3 specification.
         */
        @JsonProperty("description") String description,

        /**
         * An attribution to be displayed when the map is shown to a user.
         * Implementations MAY decide to treat this as HTML or literal text.
         * Optional field as defined in PMTiles v3 specification.
         */
        @JsonProperty("attribution") String attribution,

        /**
         * The type of the tileset, indicating whether it's a base layer or overlay.
         * Must be either "baselayer" or "overlay" if present.
         * Optional field as defined in PMTiles v3 specification.
         */
        @JsonProperty("type") TilesetType type,

        /**
         * The version number of the tileset.
         * Should be a valid version according to Semantic Versioning 2.0.0.
         * Optional field as defined in PMTiles v3 specification.
         */
        @JsonProperty("version") String version,

        /**
         * Vector layer definitions for vector tilesets.
         * Required for MVT Vector Tiles as per TileJSON 3.0 specification.
         * Each layer describes the data fields and zoom range available.
         */
        @JsonProperty("vector_layers") List<VectorLayer> vectorLayers,

        /**
         * Additional arbitrary metadata as key-value pairs.
         * This can contain any additional properties not covered by the PMTiles specification.
         * Allows for future extensions and custom metadata.
         */
        @JsonProperty("extras") Map<String, Object> extras) {

    /**
     * Creates a new PMTilesMetadata with all fields.
     */
    public PMTilesMetadata {}

    /**
     * Creates a minimal PMTilesMetadata with just a name.
     *
     * @param name the name of the tileset
     * @return a new PMTilesMetadata instance
     */
    public static PMTilesMetadata of(String name) {
        return new PMTilesMetadata(name, null, null, null, null, null, null);
    }

    /**
     * Creates a PMTilesMetadata with basic tileset information.
     *
     * @param name the name of the tileset
     * @param description the description of the tileset
     * @param attribution the attribution text
     * @return a new PMTilesMetadata instance
     */
    public static PMTilesMetadata of(String name, String description, String attribution) {
        return new PMTilesMetadata(name, description, attribution, null, null, null, null);
    }

    /**
     * Returns a new PMTilesMetadata with the specified name.
     *
     * @param name the new name
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withName(String name) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }

    /**
     * Returns a new PMTilesMetadata with the specified format.
     *
     * @param format the new format
     * @return a new PMTilesMetadata instance
     */

    /**
     * Returns a new PMTilesMetadata with the specified description.
     *
     * @param description the new description
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withDescription(String description) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }

    /**
     * Returns a new PMTilesMetadata with the specified version.
     *
     * @param version the new version
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withVersion(String version) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }

    /**
     * Returns a new PMTilesMetadata with the specified attribution.
     *
     * @param attribution the new attribution
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withAttribution(String attribution) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }

    /**
     * Returns a new PMTilesMetadata with the specified center coordinates.
     *
     * @param longitude the longitude of the center
     * @param latitude the latitude of the center
     * @param zoom the zoom level for the center (optional)
     * @return a new PMTilesMetadata instance
     */

    /**
     * Returns a new PMTilesMetadata with the specified bounds.
     *
     * @param west the western longitude
     * @param south the southern latitude
     * @param east the eastern longitude
     * @param north the northern latitude
     * @return a new PMTilesMetadata instance
     */

    /**
     * Returns a new PMTilesMetadata with the specified zoom range.
     *
     * @param minZoom the minimum zoom level
     * @param maxZoom the maximum zoom level
     * @return a new PMTilesMetadata instance
     */

    /**
     * Returns a new PMTilesMetadata with the specified vector layers.
     *
     * @param vectorLayers the vector layer definitions
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withVectorLayers(List<VectorLayer> vectorLayers) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }

    /**
     * Returns a new PMTilesMetadata with the specified type.
     *
     * @param type the tileset type (baselayer or overlay)
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withType(TilesetType type) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }

    /**
     * Returns a new PMTilesMetadata with the specified extras.
     *
     * @param extras additional metadata as key-value pairs
     * @return a new PMTilesMetadata instance
     */
    public PMTilesMetadata withExtras(Map<String, Object> extras) {
        return new PMTilesMetadata(name, description, attribution, type, version, vectorLayers, extras);
    }
}
