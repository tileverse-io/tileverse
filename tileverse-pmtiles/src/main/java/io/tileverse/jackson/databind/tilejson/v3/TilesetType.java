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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the type of a tileset according to the TileJSON v3.0.0 specification.
 * The type indicates whether the tileset is meant to be used as a base layer
 * or an overlay layer.
 *
 * @since 1.0
 * @see <a href="https://github.com/mapbox/tilejson-spec/tree/master/3.0.0#32-type">TileJSON Type</a>
 */
public enum TilesetType {

    /**
     * A baselayer tileset provides the foundational map data.
     * Typically used as the bottom layer in a map stack.
     */
    BASELAYER("baselayer"),

    /**
     * An overlay tileset provides additional data that is rendered
     * on top of a base layer. Used for thematic data, annotations, etc.
     */
    OVERLAY("overlay");

    private final String value;

    TilesetType(String value) {
        this.value = value;
    }

    /**
     * Returns the JSON string value for this tileset type.
     *
     * @return the JSON string representation
     */
    @JsonValue
    public String getValue() {
        return value;
    }

    /**
     * Creates a TilesetType from its JSON string representation.
     * This method is case-insensitive and handles unknown values gracefully.
     *
     * @param value the JSON string value
     * @return the corresponding TilesetType, or null for unknown values
     */
    @JsonCreator
    public static TilesetType fromValue(String value) {
        if (value == null) {
            return null;
        }

        String normalized = value.toLowerCase().trim();
        for (TilesetType type : values()) {
            if (type.value.equals(normalized)) {
                return type;
            }
        }

        // Return null for unknown values to maintain forward compatibility
        // Jackson will handle this gracefully due to @JsonIgnoreProperties(ignoreUnknown = true)
        return null;
    }

    @Override
    public String toString() {
        return value;
    }
}
