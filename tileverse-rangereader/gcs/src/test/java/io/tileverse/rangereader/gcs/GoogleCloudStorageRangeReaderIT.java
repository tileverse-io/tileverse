/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.rangereader.gcs;

import com.google.auth.Credentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.it.AbstractRangeReaderIT;
import io.tileverse.rangereader.it.TestUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for GoogleCloudStorageRangeReader using Google Cloud Storage emulator.
 * <p>
 * These tests verify that the {@link GoogleCloudStorageRangeReader} can correctly read ranges of bytes
 * from a GCS bucket using the Google Cloud Storage API against a local emulator container.
 */
@Testcontainers(disabledWithoutDocker = true)
public class GoogleCloudStorageRangeReaderIT extends AbstractRangeReaderIT {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_NAME = "test.bin";
    private static final String PROJECT_ID = "test-project";
    private static final Credentials NO_CREDENTIALS = new Credentials() {
        @Override
        public String getAuthenticationType() {
            return "None";
        }

        @Override
        public Map<String, List<String>> getRequestMetadata(java.net.URI uri) {
            return Collections.emptyMap();
        }

        @Override
        public boolean hasRequestMetadata() {
            return false;
        }

        @Override
        public boolean hasRequestMetadataOnly() {
            return false;
        }

        @Override
        public void refresh() {
            // no-op for emulator access
        }
    };

    private static Path testFile;
    private static Storage storage;

    static FakeGcsServerContainer gcsEmulator = new FakeGcsServerContainer();

    @BeforeAll
    static void setupGCS() throws IOException {
        // Create a test file
        testFile = TestUtil.createTempTestFile(TEST_FILE_SIZE);

        // Wait for the emulator to be ready
        gcsEmulator.start();

        // Configure the Storage client to use the emulator
        String emulatorHost = gcsEmulator.getHost();
        Integer emulatorPort = gcsEmulator.getFirstMappedPort();
        String emulatorEndpoint = "http://" + emulatorHost + ":" + emulatorPort;

        // Create Storage client
        storage = StorageOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setHost(emulatorEndpoint)
                .setCredentials(NO_CREDENTIALS)
                .build()
                .getService();

        // Create a bucket
        BucketInfo bucketInfo = BucketInfo.newBuilder(BUCKET_NAME).build();
        storage.create(bucketInfo);

        // Upload the test file
        byte[] fileContent = Files.readAllBytes(testFile);
        BlobId blobId = BlobId.of(BUCKET_NAME, OBJECT_NAME);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, fileContent);
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        // Use explicit no-op credentials so local ADC state does not interfere with emulator tests.
        String emulatorHost = gcsEmulator.getHost();
        Integer emulatorPort = gcsEmulator.getFirstMappedPort();
        String emulatorEndpoint = "http://" + emulatorHost + ":" + emulatorPort;

        Storage storage = StorageOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setHost(emulatorEndpoint)
                .setCredentials(NO_CREDENTIALS)
                .build()
                .getService();

        return GoogleCloudStorageRangeReader.builder()
                .storage(storage)
                .bucket(BUCKET_NAME)
                .objectName(OBJECT_NAME)
                .build();
    }
}
