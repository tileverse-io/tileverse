/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.jackson.databind.versatiles.v2;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.tileverse.jackson.databind.tilejson.v3.TilesetType;
import io.tileverse.jackson.databind.tilejson.v3.VectorLayer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The metadata of a Versatiles 2 container: a TileJSON document stored after the header.
 *
 * <p>The container specification requires the metadata to be JSON. Vector tilesets list their layers in a
 * {@code vector_layers} array, as defined by TileJSON 3.0. The {@code versatiles} tool adds {@code tile_format},
 * {@code tile_schema}, and {@code tile_type} keys, and copies the keys of the converted archive, such as the
 * {@code planetiler:*} keys of a Planetiler build. Fields without a dedicated getter are available from
 * {@link #extras()}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class VersatilesMetadata {

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("attribution")
    private String attribution;

    @JsonProperty("type")
    private TilesetType type;

    @JsonProperty("version")
    private String version;

    @JsonProperty("vector_layers")
    private List<VectorLayer> vectorLayers = List.of();

    private final Map<String, Object> extras = new LinkedHashMap<>();

    /** Returns the name of the tileset, or null when the metadata has none. */
    public String name() {
        return name;
    }

    /** Returns the description of the tileset, or null when the metadata has none. */
    public String description() {
        return description;
    }

    /** Returns the attribution text of the tileset, or null when the metadata has none. */
    public String attribution() {
        return attribution;
    }

    /** Returns whether the tileset is an overlay or a base layer, or null when the metadata does not say. */
    public TilesetType type() {
        return type;
    }

    /** Returns the version of the tileset, or null when the metadata has none. */
    public String version() {
        return version;
    }

    /** Returns the vector layers of the tileset; empty for raster tilesets. */
    public List<VectorLayer> vectorLayers() {
        return vectorLayers == null ? List.of() : Collections.unmodifiableList(vectorLayers);
    }

    /** Returns the metadata fields without a dedicated getter, by JSON key. */
    @JsonAnyGetter
    public Map<String, Object> extras() {
        return Collections.unmodifiableMap(extras);
    }

    @JsonAnySetter
    void putExtra(String key, Object value) {
        extras.put(key, value);
    }

    /** Returns the metadata of an archive without metadata: no name, no layers, no extras. */
    public static VersatilesMetadata empty() {
        return new VersatilesMetadata();
    }
}
