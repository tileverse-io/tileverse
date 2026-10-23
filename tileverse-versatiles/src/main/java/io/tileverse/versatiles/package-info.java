/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
 * Reads Versatiles 2.0 containers, a single-file archive format for map tiles.
 *
 * <p>{@link io.tileverse.versatiles.VersatilesReader} opens an archive through a
 * {@link io.tileverse.storage.RangeReader} and returns the decompressed bytes of its tiles. The tile stores of
 * {@link io.tileverse.versatiles.store} decode the tiles to vector tiles or images.
 *
 * @see <a href="https://docs.versatiles.org/compendium/specification_container.html">Versatiles container
 *     specification</a>
 */
package io.tileverse.versatiles;
