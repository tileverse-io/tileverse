/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Compression codecs for the tile formats: none, gzip, Brotli and Zstandard.
 *
 * <p>{@link io.tileverse.compression.Codec} decompresses the bytes of a {@link java.nio.ByteBuffer} into a stream. gzip
 * and Brotli decode with native libraries and fall back to pure-Java decoders on platforms without them.
 */
@NullMarked
package io.tileverse.compression;

import org.jspecify.annotations.NullMarked;
