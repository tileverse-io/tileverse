/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * PMTiles-backed tile store implementations.
 *
 * <p>This package provides concrete implementations of the {@link io.tileverse.tiling.store.TileStore} and
 * {@link io.tileverse.vectortile.store.VectorTileStore} interfaces for the PMTiles archive format.
 *
 * <p>Key components include:
 *
 * <ul>
 *   <li>{@link io.tileverse.pmtiles.store.PMTilesVectorTileStore}: A vector tile store that reads from PMTiles files.
 *   <li>{@link io.tileverse.pmtiles.store.PMTilesTileMatrixSet}: Factory for creating tile matrix sets matching the
 *       specific configuration of a PMTiles archive.
 * </ul>
 */
package io.tileverse.pmtiles.store;
