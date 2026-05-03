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
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Decorator that wraps every {@link RangeReader} returned by {@link #openRangeReader(String)} with a caller-supplied
 * transform (typically caching and/or block alignment), while delegating every other {@link Storage} method straight
 * through to the inner backend.
 *
 * <p><b>Cache scope:</b> only {@link #openRangeReader(String)} returns a decorated (potentially cached) reader.
 * {@link #read(String, ReadOptions)}, {@link #stat(String)}, {@link #list(String, ListOptions)}, and all write methods
 * delegate uncached. Caches operate on byte ranges; sequential streams skip the cache by design.
 *
 * <p>Used by {@link StorageFactory} to apply caching auto-decoration based on the {@code storage.caching.*} parameters
 * in the {@link io.tileverse.storage.StorageConfig}, preserving the legacy auto-caching behavior without leaking the
 * decorator concern into individual backend Storage implementations.
 */
final class CachingStorage implements Storage {

    private final Storage delegate;
    private final UnaryOperator<RangeReader> readerDecorator;

    CachingStorage(Storage delegate, UnaryOperator<RangeReader> readerDecorator) {
        this.delegate = delegate;
        this.readerDecorator = readerDecorator;
    }

    @Override
    public URI baseUri() {
        return delegate.baseUri();
    }

    @Override
    public StorageCapabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public Optional<StorageEntry.File> stat(String key) {
        return delegate.stat(key);
    }

    @Override
    public Stream<StorageEntry> list(String pattern, ListOptions options) {
        return delegate.list(pattern, options);
    }

    @Override
    public RangeReader openRangeReader(String key) {
        return readerDecorator.apply(delegate.openRangeReader(key));
    }

    /**
     * Delegate URI-to-key derivation to the wrapped Storage so backend-specific overrides (e.g. GCS stripping
     * {@code ?alt=media} from the URI) are honored when callers go through the URI overload.
     */
    @Override
    public String relativizeToKey(URI uri) {
        return delegate.relativizeToKey(uri);
    }

    @Override
    public ReadHandle read(String key, ReadOptions options) {
        return delegate.read(key, options);
    }

    @Override
    public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
        return delegate.put(key, data, options);
    }

    @Override
    public StorageEntry.File put(String key, Path source, WriteOptions options) {
        return delegate.put(key, source, options);
    }

    @Override
    public OutputStream openOutputStream(String key, WriteOptions options) {
        return delegate.openOutputStream(key, options);
    }

    @Override
    public void delete(String key) {
        delegate.delete(key);
    }

    @Override
    public DeleteResult deleteAll(Collection<String> keys) {
        return delegate.deleteAll(keys);
    }

    @Override
    public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
        return delegate.copy(srcKey, dstKey, options);
    }

    @Override
    public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
        return delegate.copy(srcKey, dst, dstKey, options);
    }

    @Override
    public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
        return delegate.move(srcKey, dstKey, options);
    }

    @Override
    public URI presignGet(String key, Duration ttl) {
        return delegate.presignGet(key, ttl);
    }

    @Override
    public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
        return delegate.presignPut(key, ttl, options);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
