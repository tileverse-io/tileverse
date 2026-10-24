/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

/** Thrown when a storage key violates backend naming rules (illegal chars, length, etc.). */
@SuppressWarnings("serial")
public class InvalidKeyException extends StorageException {

    public InvalidKeyException(String message) {
        super(message);
    }

    public InvalidKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
