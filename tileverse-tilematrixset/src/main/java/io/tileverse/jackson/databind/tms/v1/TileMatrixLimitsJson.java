/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.jackson.databind.tms.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON encoding of a {@code TileMatrixLimits} entry (clause 7.3 of OGC 17-083r2).
 *
 * @since 1.4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TileMatrixLimitsJson(
        @JsonProperty("tileMatrix") String tileMatrix,
        @JsonProperty("minTileRow") long minTileRow,
        @JsonProperty("maxTileRow") long maxTileRow,
        @JsonProperty("minTileCol") long minTileCol,
        @JsonProperty("maxTileCol") long maxTileCol) {}
