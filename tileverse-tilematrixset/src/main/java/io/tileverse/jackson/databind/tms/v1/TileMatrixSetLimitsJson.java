/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.jackson.databind.tms.v1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * JSON encoding of an OGC TMS {@code TileMatrixSetLimits} (clause 7.3 of OGC 17-083r2).
 *
 * @since 1.4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TileMatrixSetLimitsJson(
        @JsonProperty("tileMatrixLimits") List<TileMatrixLimitsJson> tileMatrixLimits) {

    public TileMatrixSetLimitsJson {
        if (tileMatrixLimits != null) {
            tileMatrixLimits = List.copyOf(tileMatrixLimits);
        }
    }
}
