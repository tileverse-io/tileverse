/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

/** Thrown when a requested byte range is not satisfiable (offset past EOF, etc.). Maps to HTTP 416. */
@SuppressWarnings("serial")
public class RangeNotSatisfiableException extends StorageException {

    public RangeNotSatisfiableException(String message) {
        super(message);
    }

    public RangeNotSatisfiableException(String message, Throwable cause) {
        super(message, cause);
    }
}
