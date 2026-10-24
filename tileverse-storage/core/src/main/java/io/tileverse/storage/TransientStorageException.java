/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

/** Thrown for transient/retryable failures (5xx, throttling, network blips). */
@SuppressWarnings("serial")
public class TransientStorageException extends StorageException {

    public TransientStorageException(String message) {
        super(message);
    }

    public TransientStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
