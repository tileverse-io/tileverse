/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

import java.io.IOException;

/**
 * Thrown when a caller opens a PMTiles archive with a store that does not handle the archive's tile type. For example,
 * constructing a {@code PMTilesVectorTileStore} on a raster (PNG/JPEG/WebP/AVIF) archive, or a
 * {@code PMTilesRasterTileStore} on a vector (MVT) archive.
 */
public class UnsupportedTileTypeException extends IOException {

    private static final long serialVersionUID = 1L;

    public UnsupportedTileTypeException(String message) {
        super(message);
    }
}
