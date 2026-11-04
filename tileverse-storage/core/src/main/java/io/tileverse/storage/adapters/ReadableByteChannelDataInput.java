/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

import java.nio.channels.ReadableByteChannel;

/**
 * A {@link java.io.DataInput} adapter over a {@link ReadableByteChannel}.
 *
 * @deprecated moved to {@link io.tileverse.io.ReadableByteChannelDataInput} in the tileverse-io module, and removed in
 *     2.3.0
 */
@Deprecated(since = "2.2", forRemoval = true)
// The deprecated 2.1 name, kept until 2.3.0, shadows the class it extends
@SuppressWarnings({"java:S1133", "java:S2176"})
public class ReadableByteChannelDataInput extends io.tileverse.io.ReadableByteChannelDataInput {

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel.
     *
     * @param channel the ReadableByteChannel to wrap
     * @throws IllegalArgumentException if channel is null
     */
    protected ReadableByteChannelDataInput(ReadableByteChannel channel) {
        super(channel);
    }

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel with specified buffer size.
     *
     * @param channel the ReadableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    protected ReadableByteChannelDataInput(ReadableByteChannel channel, int bufferSize) {
        super(channel, bufferSize);
    }

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel.
     *
     * @param channel the ReadableByteChannel to wrap
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null
     */
    public static ReadableByteChannelDataInput of(ReadableByteChannel channel) {
        return new ReadableByteChannelDataInput(channel);
    }

    /**
     * Creates a new DataInput adapter for the given ReadableByteChannel with specified buffer size.
     *
     * @param channel the ReadableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    public static ReadableByteChannelDataInput of(ReadableByteChannel channel, int bufferSize) {
        return new ReadableByteChannelDataInput(channel, bufferSize);
    }
}
