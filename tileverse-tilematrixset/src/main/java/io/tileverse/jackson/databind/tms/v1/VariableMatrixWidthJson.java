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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON encoding of a {@code VariableMatrixWidth} entry inside a {@code TileMatrix} (clause 7.2 of OGC 17-083r2).
 *
 * @param coalesce coalescence factor; must be greater than 1
 * @param minTileRow inclusive minimum row affected by the coalescence
 * @param maxTileRow inclusive maximum row affected by the coalescence
 * @since 1.4
 */
public record VariableMatrixWidthJson(
        @JsonProperty("coalesce") int coalesce,
        @JsonProperty("minTileRow") long minTileRow,
        @JsonProperty("maxTileRow") long maxTileRow) {}
