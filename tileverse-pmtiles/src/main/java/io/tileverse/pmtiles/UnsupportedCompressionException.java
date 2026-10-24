/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

/** Exception thrown when an unsupported compression type is used. */
@SuppressWarnings("serial")
public class UnsupportedCompressionException extends RuntimeException {
    public UnsupportedCompressionException(String message) {
        super(message);
    }

    public UnsupportedCompressionException(String message, Throwable cause) {
        super(message, cause);
    }
}
