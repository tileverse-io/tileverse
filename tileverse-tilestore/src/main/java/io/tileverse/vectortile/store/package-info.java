/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Implementations and utilities for vector tile storage and retrieval.
 *
 * <p>This package specializes the core {@link io.tileverse.tiling.store.TileStore} abstractions for
 * {@link io.tileverse.vectortile.model.VectorTile} data. It provides:
 *
 * <ul>
 *   <li>{@link io.tileverse.vectortile.store.VectorTileStore}: an abstract base class for vector tile stores.
 *   <li>{@link io.tileverse.vectortile.store.VectorTilesQuery}: a fluent API for querying vector features from a store.
 *   <li>{@link io.tileverse.vectortile.store.VectorTileCache}: a thread-safe, memory-bounded cache for decoded vector
 *       tiles, shared across stores.
 *   <li>{@link io.tileverse.vectortile.store.WebMercatorTransform}: a coordinate transformation utility between
 *       geographic (lat/lon) and Web Mercator (EPSG:3857) coordinates.
 * </ul>
 */
package io.tileverse.vectortile.store;
