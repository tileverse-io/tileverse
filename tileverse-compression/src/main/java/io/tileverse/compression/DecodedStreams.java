/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.compression;

import java.io.BufferedInputStream;
import java.io.InputStream;

/** Buffering shared by the decoders. */
final class DecodedStreams {

    private static final int BUFFER_BYTES = 4096;

    private DecodedStreams() {}

    /** Without a buffer, a reader taking one byte at a time would run the decoder once per byte. */
    static InputStream buffered(InputStream decoded) {
        return new BufferedInputStream(decoded, BUFFER_BYTES);
    }
}
