/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

/**
 * Thrown when the bytes of a vector tile break the protobuf wire format, or when a layer lacks its required name or
 * version. A decoder failing on malformed geometry also throws it at the first malformed geometry. The message names
 * the problem and its byte offset.
 *
 * <p>Decoding validates the whole tile and reports this exception as an IOException. The exception is unchecked because
 * the later reads of a validated tile cannot raise it.
 */
@SuppressWarnings("serial")
final class MalformedTileException extends RuntimeException {

    MalformedTileException(String message) {
        super(message);
    }
}
