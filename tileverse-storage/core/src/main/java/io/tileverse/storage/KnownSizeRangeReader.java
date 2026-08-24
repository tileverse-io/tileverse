/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
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
