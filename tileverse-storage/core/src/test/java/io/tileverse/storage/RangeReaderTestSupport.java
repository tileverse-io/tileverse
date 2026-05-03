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

import io.tileverse.storage.http.HttpAuthentication;
import io.tileverse.storage.http.HttpStorageProvider;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * Test-only helpers that produce a {@link RangeReader} bound to a single object URI. The returned reader bundles a
 * private {@link Storage} (opened against the parent URI) so tests can write {@code try (RangeReader r = ...) { ... }}
 * without juggling two resources.
 *
 * <p>This bundling exists for test ergonomics only: the public API is still "open a Storage, ask for a reader."
 * Production code should always hold a Storage explicitly.
 */
public final class RangeReaderTestSupport {

    private RangeReaderTestSupport() {}

    /** Opens a {@link RangeReader} for a local file via the {@link StorageFactory#open(URI) Storage entry point}. */
    public static RangeReader fileReader(Path path) {
        URI leaf = path.toUri();
        URI parent = leaf.resolve(".");
        Storage storage = StorageFactory.open(parent);
        return wrapOrCloseStorage(storage, leaf);
    }

    /** Opens a {@link RangeReader} for an HTTP/HTTPS URL using a fresh {@link HttpClient}. */
    public static RangeReader httpReader(URI uri) {
        return httpReader(uri, HttpClient.newHttpClient(), HttpAuthentication.NONE);
    }

    /**
     * Opens a {@link RangeReader} for an HTTP/HTTPS URL with the supplied authentication, using a fresh
     * {@link HttpClient}.
     */
    public static RangeReader httpReader(URI uri, HttpAuthentication authentication) {
        return httpReader(uri, HttpClient.newHttpClient(), authentication);
    }

    /**
     * Opens a {@link RangeReader} for an HTTP/HTTPS URL using the supplied client and authentication. The URL is
     * treated as a leaf object: the reader points at this exact URL.
     */
    public static RangeReader httpReader(URI uri, HttpClient client, HttpAuthentication authentication) {
        URI parent = uri.resolve(".");
        Storage storage = HttpStorageProvider.open(parent, client, authentication);
        return wrapOrCloseStorage(storage, uri);
    }

    /**
     * Opens a reader for {@code leaf} on the supplied storage and bundles them into a single closeable. If the
     * delegate-reader open throws, the storage is closed and the exception is rethrown.
     */
    private static RangeReader wrapOrCloseStorage(Storage storage, URI leaf) {
        try {
            RangeReader delegate = storage.openRangeReader(leaf);
            return bundle(delegate, storage);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Bundles a {@link RangeReader} and the owning {@link Storage} into a single closeable. Closing the returned reader
     * closes the delegate first and the Storage in a {@code finally} block so neither leaks if the other throws. Used
     * by per-backend integration tests that drive {@link io.tileverse.storage.it.AbstractRangeReaderIT
     * AbstractRangeReaderIT}'s {@code createBaseReader()} hook, which expects a single closeable.
     */
    public static RangeReader bundle(RangeReader delegate, Storage owner) {
        return new OwningRangeReader(delegate, owner);
    }

    /**
     * Test-private analog of the production {@code OwnedRangeReader}: closes the delegate first and the owning Storage
     * in a {@code finally} block so neither leaks if the other throws.
     */
    private static final class OwningRangeReader implements RangeReader {

        private final RangeReader delegate;
        private final Storage owner;

        OwningRangeReader(RangeReader delegate, Storage owner) {
            this.delegate = delegate;
            this.owner = owner;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            return delegate.readRange(offset, length, target);
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
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                owner.close();
            }
        }
    }
}
