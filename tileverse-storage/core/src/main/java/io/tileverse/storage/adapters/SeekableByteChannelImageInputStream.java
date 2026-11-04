/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.adapters;

import java.nio.channels.SeekableByteChannel;

/**
 * An {@link javax.imageio.stream.ImageInputStream} over a {@link SeekableByteChannel}.
 *
 * @deprecated moved to {@link io.tileverse.io.SeekableByteChannelImageInputStream} in the tileverse-io module, and
 *     removed in 2.3.0
 */
@Deprecated(since = "2.2", forRemoval = true)
// The deprecated 2.1 name, kept until 2.3.0, shadows the class it extends
@SuppressWarnings({"java:S1133", "java:S2176"})
public final class SeekableByteChannelImageInputStream extends io.tileverse.io.SeekableByteChannelImageInputStream {

    private SeekableByteChannelImageInputStream(SeekableByteChannel channel) {
        super(channel);
    }

    /**
     * Creates a new ImageInputStream adapter for the given SeekableByteChannel.
     *
     * @param channel the SeekableByteChannel to wrap
     * @return a new ImageInputStream adapter
     * @throws IllegalArgumentException if channel is null
     */
    public static SeekableByteChannelImageInputStream of(SeekableByteChannel channel) {
        return new SeekableByteChannelImageInputStream(channel);
    }
}
