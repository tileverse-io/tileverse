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
 * TileJSON v3.0.0 specification object model with Jackson databind support.
 *
 * <p>This package provides a complete implementation of the TileJSON v3.0.0 specification
 * as defined at <a href="https://github.com/mapbox/tilejson-spec/tree/master/3.0.0">TileJSON 3.0.0 Specification</a>.
 *
 * <p>TileJSON is a format for describing map tilesets. It describes tilesets' metadata,
 * including their bounds, available zoom levels, and vector layer information.
 *
 * <p>Key classes:
 * <ul>
 * <li>{@link io.tileverse.jackson.databind.tilejson.v3.TileJSON} - Main TileJSON container following v3.0.0 spec</li>
 * <li>{@link io.tileverse.jackson.databind.tilejson.v3.VectorLayer} - Vector layer definitions with field metadata</li>
 * <li>{@link io.tileverse.jackson.databind.tilejson.v3.TilesetType} - Enum for tileset type (baselayer/overlay)</li>
 * </ul>
 *
 * <p>All classes are designed to work seamlessly with Jackson for JSON
 * serialization and deserialization, with strict adherence to the TileJSON v3.0.0 schema
 * while supporting unknown properties for forward compatibility.
 *
 * @since 1.0
 * @see <a href="https://github.com/mapbox/tilejson-spec/tree/master/3.0.0">TileJSON v3.0.0 Specification</a>
 */
package io.tileverse.jackson.databind.tilejson.v3;
