/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

/**
 * Thrown when a caller invokes an operation that the backend does not support (e.g. {@code list} on an HTTP-only
 * Storage, or {@code presignGet} on local FS). The capability name (matching {@code StorageCapabilities} accessors)
 * appears in the message.
 */
@SuppressWarnings("serial")
public class UnsupportedCapabilityException extends StorageException {

    public UnsupportedCapabilityException(String capabilityName) {
        super("Storage capability not supported: " + capabilityName);
    }

    public UnsupportedCapabilityException(String capabilityName, Throwable cause) {
        super("Storage capability not supported: " + capabilityName, cause);
    }
}
