/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Raster-tile specialization of the core {@link io.tileverse.tiling.store.TileStore} abstraction.
 *
 * <p>{@link io.tileverse.rastertile.store.RasterTileStore} returns decoded {@link java.awt.image.RenderedImage}s with a
 * per-store {@code mimeType()}. It is the parallel of {@link io.tileverse.vectortile.store.VectorTileStore}, kept in
 * its own package because the two share no domain surface above {@code AbstractTileStore}.
 */
package io.tileverse.rastertile.store;
