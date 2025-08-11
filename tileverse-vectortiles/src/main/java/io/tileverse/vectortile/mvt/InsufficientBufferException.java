/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.vectortile.mvt;

import java.io.IOException;

/**
 * Exception thrown when a ByteBuffer has insufficient capacity for encoding a vector tile.
 * <p>
 * The caller can use the {@link #getSerializedSize()} to determine the required buffer size
 * and retry the operation with an appropriately sized buffer.
 */
@SuppressWarnings("serial")
public class InsufficientBufferException extends IOException {

    private final int serializedSize;
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
     * Returns the required buffer size in bytes for the vector tile.
     * The caller can use this value to allocate an appropriately sized buffer
     * and retry the encoding operation.
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
