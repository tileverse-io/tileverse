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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/** Streaming reads run on the async client's blocking stream, one GET per read. */
@ExtendWith(MockitoExtension.class)
class S3StorageReadTest {

    private static final int OBJECT_SIZE = 64 * 1024;
    private static final int CHUNK_SIZE = 4096;
    private static final byte[] OBJECT = deterministicBytes(OBJECT_SIZE);

    @Mock
    private S3AsyncClient client;

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
        when(client.serviceClientConfiguration()).thenThrow(new UnsupportedOperationException());
        new S3ObjectStub(OBJECT, CHUNK_SIZE).installAsync(client);
        URI baseUri = URI.create("s3://bucket/");
        storage = new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), new BorrowedS3Handle(client), false);
    }

    @Test
    void aRangedReadStreamsTheRange() throws IOException {
        try (ReadHandle handle = storage.read("object.bin", ReadOptions.range(1024, 8192))) {
            byte[] content = handle.content().readAllBytes();

            assertThat(content).containsExactly(Arrays.copyOfRange(OBJECT, 1024, 1024 + 8192));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void aWholeObjectReadIsOneGetWithoutARange() throws IOException {
        try (ReadHandle handle = storage.read("object.bin")) {
            assertThat(handle.content().readAllBytes()).containsExactly(OBJECT);
        }

        verify(client)
                .getObject(
                        argThat((GetObjectRequest request) -> request.range() == null && request.partNumber() == null),
                        any(AsyncResponseTransformer.class));
    }
}
