/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.matrix;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Optional;

/**
 * Reference from a dataset to a {@link TileMatrixSet} defined elsewhere, optionally narrowed by
 * {@link TileMatrixSetLimits}, as defined by OGC 17-083r2 clause 7.4.
 *
 * <p>When {@code tileMatrixSetLimits} is absent, the dataset implicitly covers the full matrix extent of every tile
 * matrix in the referenced set.
 *
 * @param tileMatrixSet URI identifying the referenced tile matrix set
 * @param tileMatrixSetLimits optional limits narrowing the referenced set
 * @since 1.4
 * @see <a href="https://docs.ogc.org/is/17-083r2/17-083r2.html#28">OGC 17-083r2 clause 7.4</a>
 */
public record TileMatrixSetLink(URI tileMatrixSet, Optional<TileMatrixSetLimits> tileMatrixSetLimits) {

    public TileMatrixSetLink {
        requireNonNull(tileMatrixSet, "tileMatrixSet URI cannot be null");
        requireNonNull(tileMatrixSetLimits, "tileMatrixSetLimits Optional cannot be null");
    }

    /** Creates a link without limits (the dataset covers the full referenced matrix set). */
    public static TileMatrixSetLink of(URI tileMatrixSet) {
        return new TileMatrixSetLink(tileMatrixSet, Optional.empty());
    }

    /** Creates a link with the given limits. */
    public static TileMatrixSetLink of(URI tileMatrixSet, TileMatrixSetLimits limits) {
        return new TileMatrixSetLink(tileMatrixSet, Optional.of(limits));
    }
}
