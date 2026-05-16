/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
