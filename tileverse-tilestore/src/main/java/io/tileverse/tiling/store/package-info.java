/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
