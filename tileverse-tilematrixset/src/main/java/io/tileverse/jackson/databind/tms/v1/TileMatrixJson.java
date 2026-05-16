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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * JSON encoding of an OGC TMS {@code TileMatrix} (clause 7.2 of OGC 17-083r2).
 *
 * <p>Field names match the JSON Schema; optional fields are emitted only when non-null/non-empty.
 *
 * @since 1.4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record TileMatrixJson(
        @JsonProperty("id") String id,
        @JsonProperty("title") String title,
        @JsonProperty("description") String description,
        @JsonProperty("keywords") List<String> keywords,
        @JsonProperty("scaleDenominator") Double scaleDenominator,
        @JsonProperty("cellSize") Double cellSize,
        @JsonProperty("cornerOfOrigin") String cornerOfOrigin,
        @JsonProperty("pointOfOrigin") List<Double> pointOfOrigin,
        @JsonProperty("tileWidth") int tileWidth,
        @JsonProperty("tileHeight") int tileHeight,
        @JsonProperty("matrixWidth") long matrixWidth,
        @JsonProperty("matrixHeight") long matrixHeight,
        @JsonProperty("variableMatrixWidths") List<VariableMatrixWidthJson> variableMatrixWidths) {

    public TileMatrixJson {
        if (keywords != null) {
            keywords = List.copyOf(keywords);
        }
        if (pointOfOrigin != null) {
            pointOfOrigin = List.copyOf(pointOfOrigin);
        }
        if (variableMatrixWidths != null) {
            variableMatrixWidths = List.copyOf(variableMatrixWidths);
        }
    }
}
