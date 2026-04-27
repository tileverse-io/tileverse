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
import io.tileverse.storage.spi.OwnedRangeReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;

/**
 * Test-only helpers that produce a {@link RangeReader} via the public API. Use these in tests that previously
 * constructed {@code FileRangeReader} / {@code HttpRangeReader} directly: now those concrete classes are
 * package-private, and tests should go through {@link StorageFactory} or a
 * {@link io.tileverse.storage.spi.StorageProvider} factory.
 *
 * <p>The returned readers wrap an internal {@link Storage} via the SPI's
 * {@link io.tileverse.storage.spi.OwnedRangeReader}; closing the reader closes the Storage too.
 */
public final class RangeReaderTestSupport {

    private RangeReaderTestSupport() {}

    /**
     * Opens a {@link RangeReader} for a local file via the SPI path. Equivalent to
     * {@code StorageFactory.openRangeReader(path.toUri())}.
     */
    public static RangeReader fileReader(Path path) throws IOException {
        return StorageFactory.openRangeReader(path.toUri());
    }

    /** Opens a {@link RangeReader} for an HTTP/HTTPS URL using a fresh {@link HttpClient}. */
    public static RangeReader httpReader(URI uri) throws IOException {
        return httpReader(uri, HttpClient.newHttpClient(), HttpAuthentication.NONE);
    }

    /**
     * Opens a {@link RangeReader} for an HTTP/HTTPS URL with the supplied authentication, using a fresh
     * {@link HttpClient}.
     */
    public static RangeReader httpReader(URI uri, HttpAuthentication authentication) throws IOException {
        return httpReader(uri, HttpClient.newHttpClient(), authentication);
    }

    /**
     * Opens a {@link RangeReader} for an HTTP/HTTPS URL using the supplied client and authentication. The URL is
     * treated as a leaf object: the reader points at this exact URL.
     */
    public static RangeReader httpReader(URI uri, HttpClient client, HttpAuthentication authentication) {
        URI parent = uri.resolve(".");
        String key = parent.relativize(uri).toString();
        Storage storage = HttpStorageProvider.open(parent, client, authentication);
        try {
            return new OwnedRangeReader(storage.openRangeReader(key), storage);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }
}
