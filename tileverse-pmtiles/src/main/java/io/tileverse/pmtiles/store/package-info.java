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
