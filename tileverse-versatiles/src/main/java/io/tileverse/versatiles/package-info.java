/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
