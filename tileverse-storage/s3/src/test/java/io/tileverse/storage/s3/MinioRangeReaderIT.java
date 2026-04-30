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
package io.tileverse.storage.s3;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.it.AbstractRangeReaderIT;
import io.tileverse.storage.it.TestUtil;
import io.tileverse.storage.spi.OwnedRangeReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration tests for S3RangeReader using MinIO.
 *
 * <p>These tests verify that the S3RangeReader can correctly read ranges of bytes from an S3-compatible storage using
 * MinIO. This demonstrates compatibility with S3-compatible storage systems beyond AWS S3.
 */
@Testcontainers(disabledWithoutDocker = true)
class MinIORangeReaderIT extends AbstractRangeReaderIT {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY_NAME = "test.bin";

    private static Path testFile;
    private static S3Client s3Client;
    private static StaticCredentialsProvider credentialsProvider;

    @Container
    static MinIOContainer minio = new MinIOContainer("minio/minio:latest");

    @BeforeAll
    static void setupMinio() throws IOException {
        testFile = TestUtil.createTempTestFile(TEST_FILE_SIZE);

        credentialsProvider =
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.getUserName(), minio.getPassword()));

        s3Client = S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1) // MinIO doesn't care about region, but it's required by the SDK
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(true) // Important for S3 compatibility with MinIO
                .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder().bucket(BUCKET_NAME).key(KEY_NAME).build();
        RequestBody body = RequestBody.fromFile(testFile);
        s3Client.putObject(putObjectRequest, body);
    }

    @AfterAll
    static void cleanupMinio() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        URI bucketUri = URI.create("s3://" + BUCKET_NAME + "/");
        Storage storage = S3StorageProvider.open(bucketUri, s3Client);
        try {
            return new OwnedRangeReader(storage.openRangeReader(KEY_NAME), storage);
        } catch (RuntimeException e) {
            try {
                storage.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }
}
