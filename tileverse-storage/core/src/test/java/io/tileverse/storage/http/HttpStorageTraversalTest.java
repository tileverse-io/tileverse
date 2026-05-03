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
