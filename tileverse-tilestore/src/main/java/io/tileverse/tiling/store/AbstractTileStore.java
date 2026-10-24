/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.store;

import static java.util.Objects.requireNonNull;

import io.tileverse.tiling.matrix.TileMatrixSet;

/**
 * Base implementation of {@link TileStore} that manages the common {@link TileMatrixSet}.
 *
 * @param <T> the type of data associated with tiles
 */
public abstract class AbstractTileStore<T> implements TileStore<T> {
    /** The tile matrix set defining the tiling scheme used by this store. */
    protected final TileMatrixSet matrixSet;

    /**
     * Initializes the store with the specified tile matrix set.
     *
     * @param matrixSet the tile matrix set to use
     */
    protected AbstractTileStore(TileMatrixSet matrixSet) {
        this.matrixSet = requireNonNull(matrixSet);
    }

    @Override
    public TileMatrixSet matrixSet() {
        return matrixSet;
    }
}
