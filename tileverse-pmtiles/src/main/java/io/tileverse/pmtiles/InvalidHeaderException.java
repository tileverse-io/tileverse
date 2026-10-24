/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.pmtiles;

/** Exception thrown when a PMTiles header is invalid. */
@SuppressWarnings("serial")
public class InvalidHeaderException extends RuntimeException {

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public InvalidHeaderException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public InvalidHeaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
