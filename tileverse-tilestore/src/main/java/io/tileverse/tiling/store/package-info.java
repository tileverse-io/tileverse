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
 * Core abstractions for tile storage and retrieval.
 *
 * <p>This package provides the foundational interfaces and classes for accessing tiled data. It is designed to be
 * format-agnostic, supporting both vector and raster tile implementations.
 *
 * <p>The primary entry point is {@link io.tileverse.tiling.store.TileStore}, which defines the contract for querying
 * and loading tiles based on spatial extents, zoom levels, or specific tile indices.
 */
package io.tileverse.tiling.store;
