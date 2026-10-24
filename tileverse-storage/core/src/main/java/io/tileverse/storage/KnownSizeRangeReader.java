/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalLong;

/**
 * Forwarding {@link RangeReader} that answers {@link #size()} from a value the caller already knows, typically from a
 * storage listing, saving the delegate's lazy metadata request. Everything else, the source identity included, forwards
 * to the delegate unchanged.
 *
 * <p>The seeded size reflects the listing observation; if the object is replaced afterwards, reads return whatever the
 * backend serves for the requested offsets.
 */
final class KnownSizeRangeReader implements RangeReader {

    private final RangeReader delegate;
    private final long size;

    private KnownSizeRangeReader(RangeReader delegate, long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size cannot be negative: " + size);
        }
        this.delegate = delegate;
        this.size = size;
    }

    static RangeReader of(RangeReader delegate, long size) {
        return new KnownSizeRangeReader(delegate, size);
    }

    @Override
    public int readRange(long offset, int length, ByteBuffer target) {
        return delegate.readRange(offset, length, target);
    }

    @Override
    public BatchReadResult readRanges(List<RangeRequest> requests) {
        return delegate.readRanges(requests);
    }

    @Override
    public OptionalLong size() {
        return OptionalLong.of(size);
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
