/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

/** Thrown on resource conflicts (already exists, lock held, etc.). Maps to HTTP 409. */
@SuppressWarnings("serial")
public class ConflictException extends StorageException {

    public ConflictException(String message) {
        super(message);
    }

    public ConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
