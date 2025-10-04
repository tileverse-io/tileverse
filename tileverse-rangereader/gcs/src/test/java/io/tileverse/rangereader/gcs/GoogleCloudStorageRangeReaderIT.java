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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
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
    protected void setUp() throws IOException {
        // Nothing needed here since all setup is done in @BeforeAll
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        // Create a new Storage client for each test to avoid connection issues
        String emulatorHost = gcsEmulator.getHost();
        Integer emulatorPort = gcsEmulator.getFirstMappedPort();

        // e.g.: http://localhost:59542/storage/v1/b/testbucket/o/testfile.bin?alt=media"
        /*
         * The alt=media parameter is important for GCS API calls when you want to download the actual file content rather than metadata.
         * Otherwise we'll get metadata like
         * {
         * "kind": "storage#object",
         *   "name": "testfile.bin",
         *   "id": "testbucket/testfile.bin",
         *   "bucket": "testbucket",
         *   "size": "1048576",
         *   "contentType": "application/octet-stream",
         *   "crc32c": "l4iVSw==",
         *   "acl": [
         *     {
         *       "bucket": "testbucket",
         *       "entity": "projectOwner-test-project",
         *       "object": "testfile.bin",
         *       "projectTeam": {},
         *       "role": "OWNER"
         *     }
         *   ],
         *   "md5Hash": "Fq8tADubC1Fqxqio4nuD4w==",
         *   "etag": "\"Fq8tADubC1Fqxqio4nuD4w==\"",
         *   "timeCreated": "2025-09-07T01:41:48.638109Z",
         *   "updated": "2025-09-07T01:41:48.638112Z",
         *   "generation": "1757209308638113"
         * }
         *
         * Alternatively we could set the STORAGE_EMULATOR_HOST environment variable to "localhost:"+gcsEmulator.getFirstMappedPort()
         * and use an URI like gs://testbucket/testfile.bin
         */
        String gcsURL = "http://%s:%d/storage/v1/b/%s/o/%s?alt=media"
                .formatted(emulatorHost, emulatorPort, BUCKET_NAME, OBJECT_NAME);
        URI uri = URI.create(gcsURL);
        return GoogleCloudStorageRangeReader.builder().uri(uri).build();
    }
}
