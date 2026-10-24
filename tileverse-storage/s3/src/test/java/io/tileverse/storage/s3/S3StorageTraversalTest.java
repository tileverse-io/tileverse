/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tileverse.storage.Storage;
import io.tileverse.storage.tck.AbstractStorageTraversalTck;
import java.net.URI;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;

/**
 * TCK-driven traversal-rejection tests for {@link S3Storage}. Validation rejects malicious keys before any SDK call:
 * mocks of the handle and its client suffice. The handle presigns because the TCK skips its presign cases on a Storage
 * without presigned URLs.
 */
class S3StorageTraversalTest extends AbstractStorageTraversalTck {

    @Override
    protected Storage storage() {
        S3AsyncClient mockClient = mock(S3AsyncClient.class);
        when(mockClient.serviceClientConfiguration())
                .thenReturn(S3ServiceClientConfiguration.builder()
                        .region(Region.US_EAST_1)
                        .build());
        S3ClientHandle presigningHandle = mock(S3ClientHandle.class);
        when(presigningHandle.client()).thenReturn(mockClient);
        when(presigningHandle.presigns()).thenReturn(true);
        URI baseUri = URI.create("s3://test-bucket/prefix/");
        return new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), presigningHandle, false);
    }
}
