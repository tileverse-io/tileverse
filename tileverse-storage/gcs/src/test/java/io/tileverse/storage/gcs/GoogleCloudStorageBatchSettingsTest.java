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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageOptions;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.batch.BatchProviderHelper;
import io.tileverse.storage.batch.BatchSettings;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** The batch settings of a GCS Storage reach every reader it opens. */
@ExtendWith(MockitoExtension.class)
class GoogleCloudStorageBatchSettingsTest {

    private static final URI BASE_URI = URI.create("gs://bucket/prefix/");
    private static final BatchSettings SETTINGS = new BatchSettings(100, 2048, 2);

    @Mock
    com.google.cloud.storage.Storage client;

    @Mock
    Bucket bucket;

    @BeforeEach
    void stubClient() {
        lenient()
                .when(client.getOptions())
                .thenReturn(StorageOptions.newBuilder()
                        .setProjectId("test-project")
                        .setCredentials(NoCredentials.getInstance())
                        .build());
        lenient()
                .when(client.get(any(String.class), any(BucketGetOption[].class)))
                .thenReturn(bucket);
    }

    @Test
    void storageHandsItsSettingsToTheReader() throws IOException {
        SdkStorageLocation location = SdkStorageLocation.parse(BASE_URI);

        try (GoogleCloudStorage storage = new GoogleCloudStorage(
                        BASE_URI, location, new BorrowedGcsHandle(client), Optional.empty(), SETTINGS);
                RangeReader reader = storage.openRangeReader("file.bin")) {
            assertThat(((GoogleCloudStorageRangeReader) reader).batchSettings()).isEqualTo(SETTINGS);
        }
    }

    @Test
    void borrowedClientStorageUsesTheObjectStoreDefaults() throws IOException {
        SdkStorageLocation location = SdkStorageLocation.parse(BASE_URI);

        try (GoogleCloudStorage storage =
                        new GoogleCloudStorage(BASE_URI, location, new BorrowedGcsHandle(client), Optional.empty());
                RangeReader reader = storage.openRangeReader("file.bin")) {
            assertThat(((GoogleCloudStorageRangeReader) reader).batchSettings())
                    .isEqualTo(BatchSettings.objectStoreDefaults());
        }
    }

    @Test
    void providerDeclaresTheBatchParameters() {
        assertThat(new GoogleCloudStorageProvider().getParameters())
                .containsAll(BatchProviderHelper.configParameters());
    }
}
