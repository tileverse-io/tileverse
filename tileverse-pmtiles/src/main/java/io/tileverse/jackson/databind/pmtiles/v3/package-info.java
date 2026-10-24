/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * PMTiles metadata object model with Jackson databind support.
 *
 * <p>This package provides a structured representation of PMTiles metadata, which follows the TileJSON specification
 * format. The metadata contains information about the tileset including attribution, bounds, vector layers, and other
 * descriptive properties.
 *
 * <p>Key classes:
 *
 * <ul>
 *   <li>{@link io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata} - Main metadata container
 * </ul>
 *
 * <p>This package uses types from {@link io.tileverse.jackson.databind.tilejson.v3} for TileJSON v3.0.0 specification
 * compliance, including {@link io.tileverse.jackson.databind.tilejson.v3.VectorLayer} and
 * {@link io.tileverse.jackson.databind.tilejson.v3.TilesetType}.
 *
 * <p>All classes are designed to work seamlessly with Jackson for JSON serialization and deserialization, with proper
 * handling of unknown properties for forward compatibility.
 *
 * @since 1.0
 */
package io.tileverse.jackson.databind.pmtiles.v3;
