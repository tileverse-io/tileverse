/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

import java.nio.channels.SeekableByteChannel;

/**
 * A {@link java.io.DataInput} adapter over a {@link SeekableByteChannel}, skipping bytes by moving the channel
 * position.
 *
 * <p>It extends the class of the tileverse-io module, not the deprecated {@link ReadableByteChannelDataInput} of this
 * package.
 *
 * @deprecated moved to {@link io.tileverse.io.SeekableByteChannelDataInput} in the tileverse-io module, and removed in
 *     2.3.0
 */
@Deprecated(since = "2.2", forRemoval = true)
// The deprecated 2.1 name, kept until 2.3.0, shadows the class it extends
@SuppressWarnings({"java:S1133", "java:S2176"})
public final class SeekableByteChannelDataInput extends io.tileverse.io.SeekableByteChannelDataInput {

    private SeekableByteChannelDataInput(SeekableByteChannel channel) {
        super(channel);
    }

    private SeekableByteChannelDataInput(SeekableByteChannel channel, int bufferSize) {
        super(channel, bufferSize);
    }

    /**
     * Creates a new DataInput adapter for the given SeekableByteChannel.
     *
     * @param channel the SeekableByteChannel to wrap
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null
     */
    public static SeekableByteChannelDataInput of(SeekableByteChannel channel) {
        return new SeekableByteChannelDataInput(channel);
    }

    /**
     * Creates a new DataInput adapter for the given SeekableByteChannel with specified buffer size.
     *
     * @param channel the SeekableByteChannel to wrap
     * @param bufferSize the size of the internal buffer
     * @return a new DataInput adapter
     * @throws IllegalArgumentException if channel is null or bufferSize is non-positive
     */
    public static SeekableByteChannelDataInput of(SeekableByteChannel channel, int bufferSize) {
        return new SeekableByteChannelDataInput(channel, bufferSize);
    }
}
