/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Tile stores over Versatiles archives.
 *
 * <ul>
 *   <li>{@link io.tileverse.versatiles.store.VersatilesVectorTileStore} decodes Mapbox Vector Tiles to
 *       {@link io.tileverse.vectortile.model.VectorTile} objects.
 *   <li>{@link io.tileverse.versatiles.store.VersatilesRasterTileStore} decodes PNG, JPEG, WebP, and AVIF tiles to
 *       {@link java.awt.image.RenderedImage} objects.
 *   <li>{@link io.tileverse.versatiles.store.VersatilesTileMatrixSet} builds the tile matrix set of an archive.
 * </ul>
 */
package io.tileverse.versatiles.store;
