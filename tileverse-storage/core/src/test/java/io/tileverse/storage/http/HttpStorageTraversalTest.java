/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;

/**
 * TCK-driven traversal-rejection tests for {@link HttpStorage}. The HTTP backend is the second most exposed (after
 * File): a malicious key concatenates into the resolved URL, so path-traversal segments could be honored by upstream
 * servers that normalize mid-path. The validation rejects them before the URL is built.
 *
 * <p>Tests run as a unit test (not IT) because no live server is required; the validation rejects the call before any
 * HTTP request is constructed. The base URL points at an unreachable port to make accidental I/O obvious.
 */
class HttpStorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        // Port 1 is reserved (tcpmux); any accidental network call would fail loudly. Validation must reject before
        // we ever try to connect.
        return StorageFactory.open(URI.create("http://127.0.0.1:1/test/"));
    }
}
