/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
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
        when(mockClient.getOptions())
                .thenReturn(StorageOptions.newBuilder()
                        .setProjectId("test-project")
                        .setCredentials(NoCredentials.getInstance())
                        .build());
        return GoogleCloudStorageProvider.open(URI.create("gs://test-bucket/prefix/"), mockClient);
    }
}
