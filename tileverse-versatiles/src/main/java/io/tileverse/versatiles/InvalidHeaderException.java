/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.versatiles;

import java.io.IOException;

/**
 * Thrown when a file does not start with a valid Versatiles 2 header: the magic is not {@code versatiles_v02}, the file
 * is shorter than the header, or a field holds a value outside the specification.
 */
public class InvalidHeaderException extends IOException {

    private static final long serialVersionUID = 1L;

    public InvalidHeaderException(String message) {
        super(message);
    }

    public InvalidHeaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
