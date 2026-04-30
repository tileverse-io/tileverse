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
package io.tileverse.storage.spi;

import io.tileverse.io.ByteRange;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.OptionalLong;
import javax.imageio.stream.ImageInputStream;

/**
 * Adapts a {@link RangeReader} obtained from a {@link Storage} so that closing the reader also closes the owning
 * storage. Used by the leaf-URL convenience paths ({@link StorageProvider#openRangeReader(StorageConfig)} and
 * {@link io.tileverse.storage.StorageFactory#openRangeReader(java.net.URI)}) to give callers a single resource to
 * close.
 *
 * <p>Every {@link RangeReader} method is forwarded explicitly to the delegate so that any optimized overrides on the
 * underlying reader (caching, block-aligned, etc.) are honored.
 */
public final class OwnedRangeReader implements RangeReader {

    private final RangeReader delegate;
    private final Storage owner;

    /** Wrap {@code delegate} so its {@link #close()} also closes {@code owner}. */
    public OwnedRangeReader(RangeReader delegate, Storage owner) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public ByteBuffer readRange(long offset, int length) {
        return delegate.readRange(offset, length);
    }

    @Override
    public ByteBuffer readRange(ByteRange range) {
        return delegate.readRange(range);
    }

    @Override
    public int readRange(long offset, int length, ByteBuffer target) {
        return delegate.readRange(offset, length, target);
    }

    @Override
    public int readRange(ByteRange range, ByteBuffer target) {
        return delegate.readRange(range, target);
    }

    @Override
    public OptionalLong size() {
        return delegate.size();
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
    }

    @Override
    public SeekableByteChannel asByteChannel() {
        return delegate.asByteChannel();
    }

    @Override
    public ImageInputStream asImageInputStream() {
        return delegate.asImageInputStream();
    }

    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            owner.close();
        }
    }
}
