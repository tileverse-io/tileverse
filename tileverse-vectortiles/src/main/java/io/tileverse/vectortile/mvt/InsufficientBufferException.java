/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.io.IOException;

/**
 * Exception thrown when a ByteBuffer has insufficient capacity for encoding a vector tile.
 *
 * <p>The caller can use the {@link #getSerializedSize()} to determine the required buffer size and retry the operation
 * with an appropriately sized buffer.
 */
@SuppressWarnings("serial")
public class InsufficientBufferException extends IOException {

    /** The required buffer size in bytes */
    private final int serializedSize;
    /** The available buffer space in bytes */
    private final int available;

    /**
     * Constructs an InsufficientBufferException with the required and available buffer sizes.
     *
     * @param serializedSize the required buffer size in bytes
     * @param available the available buffer space in bytes
     */
    public InsufficientBufferException(int serializedSize, int available) {
        super("ByteBuffer has insufficient space: required " + serializedSize + ", available " + available);
        this.serializedSize = serializedSize;
        this.available = available;
    }

    /**
     * Returns the required buffer size in bytes for the vector tile. The caller can use this value to allocate an
     * appropriately sized buffer and retry the encoding operation.
     *
     * @return the serialized size in bytes
     */
    public int getSerializedSize() {
        return serializedSize;
    }

    /**
     * Returns the available buffer space that was insufficient.
     *
     * @return the available space in bytes
     */
    public int getAvailable() {
        return available;
    }
}
