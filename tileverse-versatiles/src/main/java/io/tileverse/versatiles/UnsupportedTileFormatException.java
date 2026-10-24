/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import java.io.IOException;

/** Thrown when a tile store cannot decode the tile format of an archive. */
public class UnsupportedTileFormatException extends IOException {

    private static final long serialVersionUID = 1L;

    public UnsupportedTileFormatException(String message) {
        super(message);
    }
}
