/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.tiling.store;

import io.tileverse.tiling.matrix.Tile;

/**
 * Container for a tile's metadata and its associated content.
 *
 * <p>This record couples a {@link Tile} (which contains spatial metadata like extent and index) with its actual data
 * {@code T}.
 *
 * @param <T> the type of data associated with the tile (e.g., {@code VectorTile} or {@code byte[]})
 * @param tile the spatial metadata of the tile
 * @param data the actual content of the tile
 */
public record TileData<T>(Tile tile, T data) {}
