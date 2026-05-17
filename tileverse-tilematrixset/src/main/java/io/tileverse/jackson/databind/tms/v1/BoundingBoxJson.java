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
