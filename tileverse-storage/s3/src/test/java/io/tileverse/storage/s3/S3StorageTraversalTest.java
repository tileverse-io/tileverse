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
package io.tileverse.storage.s3;

import static org.mockito.Mockito.mock;

import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * TCK-driven traversal-rejection tests for {@link S3Storage}. Validation rejects malicious keys before any SDK call, so
 * a mock S3Client suffices - if validation worked, the mock is never touched.
 */
class S3StorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        S3Client mockClient = mock(S3Client.class);
        return S3StorageProvider.open(URI.create("s3://test-bucket/prefix/"), mockClient);
    }
}
