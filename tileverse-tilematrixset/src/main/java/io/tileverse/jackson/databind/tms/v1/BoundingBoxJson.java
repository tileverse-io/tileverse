/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.jackson.databind.tms.v1;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * JSON encoding of an OGC two-dimensional bounding box (clause 7 of OGC 17-083r2).
 *
 * @param lowerLeft lower-left coordinate as {@code [x, y]}
 * @param upperRight upper-right coordinate as {@code [x, y]}
 * @param crs optional CRS reference; when absent the bounding box uses the parent's CRS
 * @since 1.4
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BoundingBoxJson(
        @JsonProperty("lowerLeft") List<Double> lowerLeft,
        @JsonProperty("upperRight") List<Double> upperRight,
        @JsonProperty("crs") String crs) {

    public BoundingBoxJson {
        if (lowerLeft != null) {
            lowerLeft = List.copyOf(lowerLeft);
        }
        if (upperRight != null) {
            upperRight = List.copyOf(upperRight);
        }
    }

    public BoundingBoxJson(List<Double> lowerLeft, List<Double> upperRight) {
        this(lowerLeft, upperRight, null);
    }
}
