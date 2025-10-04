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
 * PMTiles metadata object model with Jackson databind support.
 *
 * <p>This package provides a structured representation of PMTiles metadata,
 * which follows the TileJSON specification format. The metadata contains
 * information about the tileset including attribution, bounds, vector layers,
 * and other descriptive properties.
 *
 * <p>Key classes:
 * <ul>
 * <li>{@link io.tileverse.jackson.databind.pmtiles.v3.PMTilesMetadata} - Main metadata container</li>
 * </ul>
 *
 * <p>This package uses types from {@link io.tileverse.jackson.databind.tilejson.v3} for TileJSON v3.0.0
 * specification compliance, including {@link io.tileverse.jackson.databind.tilejson.v3.VectorLayer} and
 * {@link io.tileverse.jackson.databind.tilejson.v3.TilesetType}.
 *
 * <p>All classes are designed to work seamlessly with Jackson for JSON
 * serialization and deserialization, with proper handling of unknown properties
 * for forward compatibility.
 *
 * @since 1.0
 */
package io.tileverse.jackson.databind.pmtiles.v3;
