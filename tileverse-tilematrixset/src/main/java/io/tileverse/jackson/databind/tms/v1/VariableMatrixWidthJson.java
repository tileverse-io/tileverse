/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
