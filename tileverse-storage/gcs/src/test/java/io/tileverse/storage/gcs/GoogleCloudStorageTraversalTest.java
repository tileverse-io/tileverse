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
package io.tileverse.storage.gcs;

import static org.mockito.Mockito.mock;

import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;

/**
 * TCK-driven traversal-rejection tests for {@link GoogleCloudStorage}. Validation rejects malicious keys before any SDK
 * call, so a mock GCS Storage client suffices.
 */
class GoogleCloudStorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        com.google.cloud.storage.Storage mockClient = mock(com.google.cloud.storage.Storage.class);
        return GoogleCloudStorageProvider.open(URI.create("gs://test-bucket/prefix/"), mockClient);
    }
}
