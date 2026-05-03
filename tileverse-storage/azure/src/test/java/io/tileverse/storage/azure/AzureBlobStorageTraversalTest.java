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
package io.tileverse.storage.azure;

import static org.mockito.Mockito.mock;

import com.azure.storage.blob.BlobServiceClient;
import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;

/**
 * TCK-driven traversal-rejection tests for {@link AzureBlobStorage}. Validation rejects malicious keys before any SDK
 * call, so a mock BlobServiceClient suffices.
 */
class AzureBlobStorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        BlobServiceClient mockClient = mock(BlobServiceClient.class);
        return AzureBlobStorageProvider.open(
                URI.create("https://devstoreaccount1.blob.core.windows.net/test-container/prefix/"), mockClient);
    }
}
