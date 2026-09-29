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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/** Streaming reads run on the sync client, regardless of the other clients in the client set. */
@ExtendWith(MockitoExtension.class)
class S3StorageReadTest {

    private static final int OBJECT_SIZE = 64 * 1024;
    private static final int CHUNK_SIZE = 4096;
    private static final byte[] OBJECT = deterministicBytes(OBJECT_SIZE);

    @Mock
    private S3Client syncClient;

    @Mock
    private S3AsyncClient asyncClient;

    private S3Storage storage;

    private static byte[] deterministicBytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i * 31);
        }
        return data;
    }

    @BeforeEach
    void openStorage() {
        // A mock has no client configuration to report; the Storage then identifies its readers by s3:// URIs.
        when(syncClient.serviceClientConfiguration()).thenThrow(new UnsupportedOperationException());
        new S3ObjectStub(OBJECT, CHUNK_SIZE).installSync(syncClient);
        S3ClientBundle bundle =
                new S3ClientBundle(syncClient, Optional.of(asyncClient), Optional.empty(), Optional.empty());
        URI baseUri = URI.create("s3://bucket/");
        storage = new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), new BorrowedS3Handle(bundle), false);
    }

    @Test
    void aStreamingReadNeverTouchesTheAsyncClient() throws IOException {
        try (ReadHandle handle = storage.read("object.bin", ReadOptions.range(1024, 8192))) {
            byte[] content = handle.content().readAllBytes();

            assertThat(content).containsExactly(Arrays.copyOfRange(OBJECT, 1024, 1024 + 8192));
        }
        verifyNoInteractions(asyncClient);
    }
}
