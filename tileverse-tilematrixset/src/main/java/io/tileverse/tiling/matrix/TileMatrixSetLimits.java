/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.matrix;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A collection of {@link TileMatrixLimits} describing the subset of a {@link TileMatrixSet} that is available or
 * relevant for a particular dataset, as defined by OGC 17-083r2 clause 7.3.
 *
 * <p>Each {@link TileMatrix#identifier() tile matrix identifier} may appear at most once across the contained limits.
 *
 * @param tileMatrixLimits the per-matrix limit declarations
 * @since 1.4
 * @see <a href="https://docs.ogc.org/is/17-083r2/17-083r2.html#27">OGC 17-083r2 clause 7.3</a>
 */
public record TileMatrixSetLimits(List<TileMatrixLimits> tileMatrixLimits) {

    private static final TileMatrixSetLimits EMPTY = new TileMatrixSetLimits(List.of());

    /** Compact constructor enforcing the uniqueness constraint of clause 7.3. */
    public TileMatrixSetLimits {
        requireNonNull(tileMatrixLimits, "tileMatrixLimits cannot be null");
        tileMatrixLimits = List.copyOf(tileMatrixLimits);
        Set<String> seenIds = new HashSet<>(tileMatrixLimits.size());
        for (TileMatrixLimits limits : tileMatrixLimits) {
            if (!seenIds.add(limits.tileMatrix())) {
                throw new IllegalArgumentException("Duplicate tileMatrix identifier in limits: " + limits.tileMatrix());
            }
        }
    }

    /** Returns the singleton empty {@code TileMatrixSetLimits}. */
    public static TileMatrixSetLimits empty() {
        return EMPTY;
    }

    /**
     * Returns the limits declared for a specific tile matrix.
     *
     * @param tileMatrixId the {@link TileMatrix#identifier() tile matrix identifier}
     * @return the limits if declared, or empty
     */
    public Optional<TileMatrixLimits> limitsFor(String tileMatrixId) {
        requireNonNull(tileMatrixId, "tileMatrixId");
        return tileMatrixLimits.stream()
                .filter(l -> l.tileMatrix().equals(tileMatrixId))
                .findFirst();
    }
}
