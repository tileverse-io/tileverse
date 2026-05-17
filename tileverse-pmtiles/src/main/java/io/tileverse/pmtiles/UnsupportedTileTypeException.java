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
