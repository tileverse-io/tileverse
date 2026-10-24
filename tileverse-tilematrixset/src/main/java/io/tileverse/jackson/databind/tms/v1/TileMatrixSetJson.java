/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.jackson.databind.tms.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * JSON encoding of an OGC TMS {@code TileMatrixSet} (clause 7.1 of OGC 17-083r2, Annex C).
 *
 * <p>Field names match the JSON Schema; optional fields are emitted only when non-null/non-empty.
 *
 * @since 1.4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record TileMatrixSetJson(
        @JsonProperty("id") String id,
        @JsonProperty("title") String title,
        @JsonProperty("description") String description,
        @JsonProperty("keywords") List<String> keywords,
        @JsonProperty("uri") String uri,
        @JsonProperty("crs") String crs,
        @JsonProperty("orderedAxes") List<String> orderedAxes,
        @JsonProperty("wellKnownScaleSet") String wellKnownScaleSet,
        @JsonProperty("boundingBox") BoundingBoxJson boundingBox,
        @JsonProperty("tileMatrices") List<TileMatrixJson> tileMatrices) {

    public TileMatrixSetJson {
        if (keywords != null) {
            keywords = List.copyOf(keywords);
        }
        if (orderedAxes != null) {
            orderedAxes = List.copyOf(orderedAxes);
        }
        if (tileMatrices != null) {
            tileMatrices = List.copyOf(tileMatrices);
        }
    }
}
